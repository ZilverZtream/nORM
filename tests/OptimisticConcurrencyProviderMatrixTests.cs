using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// S1 — MySQL affected-row OCC contract (Gate 4.0→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Provider-matrix tests for the optimistic-concurrency rowcount check, covering:
///
/// <list type="bullet">
///   <item>SQLite (matched-row semantics) — rowcount check fires, genuine conflicts detected.</item>
///   <item>AffectedRows-semantics providers (MySQL default) — genuine conflicts detected via
///         SELECT-then-verify fallback; same-value updates (no columns changed) do not throw.</item>
///   <item>Matched-row opt-in override — subclass that sets <c>UseAffectedRowsSemantics=false</c>
///         uses direct rowcount check without SELECT-then-verify overhead.</item>
///   <item>DELETE path — always checked regardless of affected-row semantics (no same-value
///         ambiguity: 0 deleted rows is always a genuine conflict).</item>
/// </list>
///
/// S1 root cause: MySQL returns "affected" (changed) rows rather than "matched" rows.
/// A same-value update (row found but no column changed) returns 0 affected rows, which
/// the naive rowcount check would misinterpret as a stale-row conflict. The SELECT-then-verify
/// fallback (<see cref="DbContext.VerifyUpdateOccAsync"/>) disambiguates by querying whether
/// the original token still exists in the database.
///
/// Residual gap: if a concurrent writer sets the token to the <em>same</em> new value as the
/// existing token (same-value token conflict), the UPDATE WHERE clause still matches and returns
/// 1 row, so neither path detects the conflict. This is a documented trade-off for all OCC
/// rowcount approaches and requires application-level versioning to close.
/// </summary>
public class OptimisticConcurrencyProviderMatrixTests
{
    // ── Test entity ────────────────────────────────────────────────────────────

    [Table("OccMatrixEntity")]
    private class OccMatrixEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        [Timestamp]
        public byte[] RowVersion { get; set; } = System.Array.Empty<byte>();
    }

    // ── Fake affected-row provider: SQLite engine + affected-row semantics flag ─

    /// <summary>
    /// A SQLite-backed provider that advertises affected-row semantics (like MySQL default).
    /// Enables testing the affected-row code path without a live MySQL server.
    /// </summary>
    private sealed class AffectedRowsProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    /// <summary>
    /// A SQLite-backed provider that explicitly opts in to matched-row semantics
    /// (overrides the affected-row flag back to false). Represents a provider configured
    /// with useAffectedRows=false.
    /// </summary>
    private sealed class MatchedRowsProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => false;
    }

    // ── Schema helper ────────────────────────────────────────────────────────

    private static (SqliteConnection cn, DbContext ctx) BuildContext(DatabaseProvider provider)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE OccMatrixEntity " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, RowVersion BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        // When using affected-row semantics, opt out of the strict RequireMatchedRowOccSemantics
        // check so that the SELECT-then-verify path is exercised instead of throwing
        // NormConfigurationException.
        var opts = provider.UseAffectedRowsSemantics
            ? new DbContextOptions { RequireMatchedRowOccSemantics = false }
            : new DbContextOptions();
        return (cn, new DbContext(cn, provider, opts));
    }

    private static void SimulateExternalRowVersionChange(SqliteConnection cn, int id, byte[] newToken)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE OccMatrixEntity SET RowVersion = @rv WHERE Id = @id";
        cmd.Parameters.AddWithValue("@rv", newToken);
        cmd.Parameters.AddWithValue("@id", id);
        cmd.ExecuteNonQuery();
    }

    private static void MarkDirty(DbContext ctx, object entity)
    {
        var entry = ctx.ChangeTracker.Entries.Single();
        typeof(ChangeTracker)
            .GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance |
                                    System.Reflection.BindingFlags.NonPublic)!
            .Invoke(ctx.ChangeTracker, new object[] { entry });
    }

    // ── Provider property assertions ─────────────────────────────────────────

    [Fact]
    public void MySqlProvider_UseAffectedRowsSemantics_IsTrue()
        => Assert.True(new MySqlProvider(new SqliteParameterFactory()).UseAffectedRowsSemantics);

    [Fact]
    public void SqliteProvider_UseAffectedRowsSemantics_IsFalse()
        => Assert.False(new SqliteProvider().UseAffectedRowsSemantics);

    [Fact]
    public void SqlServerProvider_UseAffectedRowsSemantics_IsFalse()
        => Assert.False(new SqlServerProvider().UseAffectedRowsSemantics);

    [Fact]
    public void AffectedRowsProvider_UseAffectedRowsSemantics_IsTrue()
        => Assert.True(new AffectedRowsProvider().UseAffectedRowsSemantics);

    [Fact]
    public void MatchedRowsProvider_UseAffectedRowsSemantics_IsFalse()
        => Assert.False(new MatchedRowsProvider().UseAffectedRowsSemantics);

    // ── SQLite (matched-row): genuine conflict is detected ───────────────────

    [Fact]
    public async Task Sqlite_GenuineConflict_ThrowsDbConcurrencyException()
    {
        var (cn, ctx) = BuildContext(new SqliteProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Alice", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 99 }); // stale token
        e.Name = "Alice v2";
        MarkDirty(ctx, e);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Sqlite_NoConflict_SaveSucceeds()
    {
        var (cn, ctx) = BuildContext(new SqliteProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Bob", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        e.Name = "Bob v2";
        MarkDirty(ctx, e);

        // No external change — should succeed.
        await ctx.SaveChangesAsync();
    }

    // ── Affected-row provider: SELECT-then-verify detects genuine conflicts ────

    /// <summary>
    /// S1 fix: with affected-row semantics, a genuine stale-token conflict (token changed
    /// by a competing writer) is now detected via SELECT-then-verify. The UPDATE returns 0
    /// rows; VerifyUpdateOccAsync queries whether the original token still exists in the DB.
    /// Since the token was overwritten, the SELECT returns 0 → DbConcurrencyException thrown.
    /// </summary>
    [Fact]
    public async Task AffectedRowsProvider_StaleToken_ThrowsViaSelectVerify()
    {
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Charlie", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        // Competing writer changes the token — our snapshot is now stale.
        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 99 });
        e.Name = "Charlie v2";
        MarkDirty(ctx, e);

        // SELECT-then-verify: original token not in DB → genuine conflict → must throw.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    /// <summary>
    /// S1 no-false-positive: with AffectedRowsProvider and a fresh (non-stale) update,
    /// the UPDATE matches 1 row (SQLite uses matched-row semantics), so updated==batch.Count
    /// and VerifyUpdateOccAsync is never invoked. Confirms no false positive for valid saves.
    /// </summary>
    [Fact]
    public async Task AffectedRowsProvider_FreshUpdate_NoFalsePositive()
    {
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Charlie", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        // No external change — token still matches.
        e.Name = "Charlie v2";
        MarkDirty(ctx, e);

        // updated == 1 == batch.Count → no verify triggered → no throw.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    /// <summary>
    /// S1 batch scenario: two entities updated, one with stale token. The batch UPDATE
    /// returns fewer rows than expected; VerifyUpdateOccAsync finds only one token still
    /// matching → count(1) != batch.Count(2) → throws DbConcurrencyException.
    /// </summary>
    [Fact]
    public async Task AffectedRowsProvider_BatchUpdate_PartialStaleToken_Throws()
    {
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e1 = new OccMatrixEntity { Name = "Foo", RowVersion = new byte[] { 1 } };
        var e2 = new OccMatrixEntity { Name = "Bar", RowVersion = new byte[] { 2 } };
        ctx.Add(e1); ctx.Add(e2);
        await ctx.SaveChangesAsync();

        // Make e2's token stale.
        SimulateExternalRowVersionChange(cn, e2.Id, new byte[] { 99 });

        e1.Name = "Foo v2"; e2.Name = "Bar v2";
        var entries = ctx.ChangeTracker.Entries.ToList();
        foreach (var entry in entries)
        {
            typeof(ChangeTracker)
                .GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance |
                                        System.Reflection.BindingFlags.NonPublic)!
                .Invoke(ctx.ChangeTracker, new object[] { entry });
        }

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task AffectedRowsProvider_ValidUpdate_Succeeds()
    {
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Dana", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        e.Name = "Dana v2";
        MarkDirty(ctx, e);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    // ── Matched-rows opt-in override restores conflict detection ─────────────

    [Fact]
    public async Task MatchedRowsProvider_StaleToken_ThrowsDbConcurrencyException()
    {
        // Demonstrates that a provider subclass with UseAffectedRowsSemantics=false
        // (the MySQL useAffectedRows=false workaround) restores full OCC detection.
        var (cn, ctx) = BuildContext(new MatchedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Eve", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 99 });
        e.Name = "Eve v2";
        MarkDirty(ctx, e);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ── Delete path: same affected-row/matched-row contract ──────────────────

    [Fact]
    public async Task Sqlite_DeleteWithStaleToken_ThrowsDbConcurrencyException()
    {
        var (cn, ctx) = BuildContext(new SqliteProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Frank", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 88 });
        ctx.Remove(e);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task AffectedRowsProvider_DeleteWithStaleToken_ThrowsDbConcurrencyException()
    {
        // S1 fix: DELETE rowcount is always checked, even under affected-row semantics.
        // Unlike UPDATE, DELETE has no same-value ambiguity: 0 deleted rows always means
        // either the token was stale or the row is gone — both are genuine conflicts.
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Grace", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 88 });
        ctx.Remove(e);

        // Stale token → DELETE matches 0 rows → always throw, regardless of affected-row flag.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ── Strict-mode (useAffectedRows=false) end-to-end path ─────────────────

    /// <summary>
    /// End-to-end strict-mode path: a MySqlProvider subclass with
    /// UseAffectedRowsSemantics=false (representing MySQL connection string
    /// useAffectedRows=false) restores full OCC detection on the DELETE path.
    /// </summary>
    [Fact]
    public async Task MatchedRowsProvider_DeleteWithStaleToken_ThrowsDbConcurrencyException()
    {
        var (cn, ctx) = BuildContext(new MatchedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Henry", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 77 });
        ctx.Remove(e);

        // Strict mode: must detect the stale-token conflict on delete.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    /// <summary>
    /// Strict-mode path: batch save with multiple Modified entities — each entity's
    /// rowcount check is evaluated individually. With UseAffectedRowsSemantics=false,
    /// the first entity with a stale token raises DbConcurrencyException.
    /// </summary>
    [Fact]
    public async Task MatchedRowsProvider_BatchSave_StaleTokenOnOneEntity_Throws()
    {
        var (cn, ctx) = BuildContext(new MatchedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e1 = new OccMatrixEntity { Name = "Iris", RowVersion = new byte[] { 1 } };
        var e2 = new OccMatrixEntity { Name = "Jack", RowVersion = new byte[] { 2 } };
        ctx.Add(e1); ctx.Add(e2);
        await ctx.SaveChangesAsync();

        // Externally change e2's token — e1 is still valid.
        SimulateExternalRowVersionChange(cn, e2.Id, new byte[] { 99 });

        e1.Name = "Iris v2";
        e2.Name = "Jack v2";
        // Mark both dirty by accessing entries
        var entries = ctx.ChangeTracker.Entries.ToList();
        foreach (var entry in entries)
        {
            typeof(ChangeTracker)
                .GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance |
                                        System.Reflection.BindingFlags.NonPublic)!
                .Invoke(ctx.ChangeTracker, new object[] { entry });
        }

        // e2's stale token must surface as DbConcurrencyException.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    /// <summary>
    /// Strict-mode path: a fresh (non-stale) update on the MatchedRowsProvider
    /// succeeds, proving the override doesn't break normal operations.
    /// </summary>
    [Fact]
    public async Task MatchedRowsProvider_ValidUpdate_Succeeds()
    {
        var (cn, ctx) = BuildContext(new MatchedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Kate", RowVersion = new byte[] { 3 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        e.Name = "Kate v2";
        MarkDirty(ctx, e);

        // Fresh token — should succeed.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    /// <summary>
    /// Documents the "same-value conflict detection gap" for affected-row providers:
    /// if a concurrent writer sets the concurrency token to the SAME value as before,
    /// UseAffectedRowsSemantics=true will NOT detect the conflict (the UPDATE succeeds
    /// because the WHERE clause still matches). This is the documented trade-off.
    /// </summary>
    [Fact]
    public async Task AffectedRowsProvider_SameValueTokenConflict_NotDetected()
    {
        // This test documents intentional behavior — not a bug.
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var token = new byte[] { 42 };
        var e = new OccMatrixEntity { Name = "Leo", RowVersion = token };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        // Concurrent writer modifies Name but sets RowVersion to the same value.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE OccMatrixEntity SET Name = 'Leo-concurrent', RowVersion = @rv WHERE Id = @id";
        cmd.Parameters.AddWithValue("@rv", token);
        cmd.Parameters.AddWithValue("@id", e.Id);
        cmd.ExecuteNonQuery();

        // Our local change — WHERE will still match because token is the same.
        e.Name = "Leo-local";
        MarkDirty(ctx, e);

        // Affected-row provider: no exception (gap in conflict detection).
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }
}
