using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
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
///   <item>AffectedRows-semantics providers (MySQL default) — rowcount check skipped to prevent
///         false-positive <see cref="DbConcurrencyException"/> on same-value updates.</item>
///   <item>Matched-row opt-in override — subclass that sets <c>UseAffectedRowsSemantics=false</c>
///         restores full conflict detection.</item>
/// </list>
///
/// S1 root cause: MySQL returns "affected" (changed) rows rather than "matched" rows.
/// A same-value update (row found but no column changed) returns 0 affected rows, which
/// nORM's rowcount check would incorrectly interpret as a stale-row conflict.
///
/// Contract: when <c>UseAffectedRowsSemantics=true</c>, the rowcount check is intentionally
/// skipped for both the update and delete paths. The trade-off is that a genuine conflict
/// where the competing writer sets the concurrency token to the <em>same</em> new value will
/// go undetected. Strict OCC on MySQL requires <c>useAffectedRows=false</c> in the connection
/// string paired with a provider subclass that overrides <c>UseAffectedRowsSemantics</c> to
/// <c>false</c>.
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
        return (cn, new DbContext(cn, provider));
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

    // ── Affected-row provider: same-value update does NOT throw ───────────────

    /// <summary>
    /// S1 core scenario: with affected-row semantics, a same-value UPDATE (e.g., no columns
    /// actually changed) returns 0 rows even though the WHERE clause matched. The rowcount
    /// check must be skipped to prevent a false-positive DbConcurrencyException.
    ///
    /// SQLite actually does report matched rows, so this test simulates the affected-row
    /// provider returning 0 by using a token-stale scenario with the skip-check flag set.
    /// We verify no exception is thrown when UseAffectedRowsSemantics=true.
    /// </summary>
    [Fact]
    public async Task AffectedRowsProvider_StaleToken_DoesNotThrow()
    {
        // With UseAffectedRowsSemantics=true the rowcount check is bypassed entirely,
        // so even a genuine conflict (stale token → 0 matched rows) doesn't throw.
        // This is the intentional trade-off for affected-row providers.
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Charlie", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 99 });
        e.Name = "Charlie v2";
        MarkDirty(ctx, e);

        // Must NOT throw — affected-row check is skipped.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
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
    public async Task AffectedRowsProvider_DeleteWithStaleToken_DoesNotThrow()
    {
        var (cn, ctx) = BuildContext(new AffectedRowsProvider());
        await using var _ = ctx; using var __ = cn;

        var e = new OccMatrixEntity { Name = "Grace", RowVersion = new byte[] { 1 } };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        SimulateExternalRowVersionChange(cn, e.Id, new byte[] { 88 });
        ctx.Remove(e);

        // Skip check — must not throw.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }
}
