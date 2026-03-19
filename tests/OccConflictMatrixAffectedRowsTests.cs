using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.6 → 4.0 — OCC conflict matrix: affected-row semantics + SELECT-then-verify
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Comprehensive OCC conflict matrix for the SELECT-then-verify fallback introduced
/// by the S1/X1 audit fix. Tests use <see cref="AffectedRowsSqliteProvider"/> as
/// a MySQL proxy: SQLite engine with <c>UseAffectedRowsSemantics=true</c>.
///
/// Key invariants proven:
/// <list type="bullet">
///   <item>Genuine stale-token conflicts ARE detected under affected-row semantics
///         (VerifyUpdateOccAsync SELECT-then-verify path).</item>
///   <item>Fresh-token updates do NOT throw false-positive DbConcurrencyException.</item>
///   <item>Null concurrency tokens are handled correctly (IS NULL predicate).</item>
///   <item>DELETE rowcount is always checked, regardless of affected-row semantics.</item>
///   <item>Batch updates with partial conflicts surface DbConcurrencyException.</item>
///   <item>Composite-key entities generate correct multi-column SELECT predicates.</item>
/// </list>
/// </summary>
public class OccConflictMatrixAffectedRowsTests
{
    // ── Provider shims ────────────────────────────────────────────────────────

    /// <summary>
    /// SQLite engine + affected-row semantics flag set. Simulates MySQL default
    /// connector behaviour without requiring a live MySQL server.
    /// Real MySQL parity tests run when NORM_TEST_MYSQL env var is set.
    /// </summary>
    private sealed class AffectedRowsSqliteProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    // ── Entities ─────────────────────────────────────────────────────────────

    [Table("OccMatrixRow")]
    private class OccMatrixRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    [Table("OccNullTokenRow")]
    private class OccNullTokenRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[]? Token { get; set; }   // nullable
    }

    [Table("OccCompositeRow")]
    private class OccCompositeRow
    {
        [Key]
        public int TenantId { get; set; }
        [Key]
        public int RowId { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateAffectedRowsDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE OccMatrixRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new AffectedRowsSqliteProvider(),
            new DbContextOptions { RequireMatchedRowOccSemantics = false }));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateNullTokenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE OccNullTokenRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new AffectedRowsSqliteProvider(),
            new DbContextOptions { RequireMatchedRowOccSemantics = false }));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateCompositeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE OccCompositeRow " +
            "(TenantId INTEGER NOT NULL, RowId INTEGER NOT NULL, Payload TEXT NOT NULL, Token BLOB NOT NULL, " +
            "PRIMARY KEY (TenantId, RowId))";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new AffectedRowsSqliteProvider(),
            new DbContextOptions { RequireMatchedRowOccSemantics = false }));
    }

    private static void ForceTokenChange(SqliteConnection cn, string table, int id, byte[] newToken)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"UPDATE {table} SET Token = @t WHERE Id = @id";
        cmd.Parameters.AddWithValue("@t", newToken);
        cmd.Parameters.AddWithValue("@id", id);
        cmd.ExecuteNonQuery();
    }

    private static void MarkDirty(DbContext ctx, object entity)
    {
        var entry = ctx.ChangeTracker.Entries.Single();
        typeof(ChangeTracker)
            .GetMethod("MarkDirty", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(ctx.ChangeTracker, new object[] { entry });
    }

    private static void MarkAllDirty(DbContext ctx)
    {
        foreach (var entry in ctx.ChangeTracker.Entries.ToList())
        {
            typeof(ChangeTracker)
                .GetMethod("MarkDirty", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(ctx.ChangeTracker, new object[] { entry });
        }
    }

    // ── 1. Stale token detected via SELECT-then-verify ────────────────────────

    [Fact]
    public async Task Update_StaleToken_AffectedRows_SelectVerify_ThrowsDbConcurrencyException()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var row = new OccMatrixRow { Payload = "original", Token = new byte[] { 1 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // External writer changes the token.
        ForceTokenChange(cn, "OccMatrixRow", row.Id, new byte[] { 99 });

        row.Payload = "updated";
        MarkDirty(ctx, row);

        // UPDATE returns 0 (token mismatch). SELECT-then-verify: token gone → throw.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ── 2. Fresh update never triggers SELECT-then-verify ────────────────────

    [Fact]
    public async Task Update_FreshToken_AffectedRows_NoVerify_NoException()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var row = new OccMatrixRow { Payload = "original", Token = new byte[] { 1 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // No external change — UPDATE matches 1 row (SQLite matched-row), updated==batch.Count.
        row.Payload = "updated";
        MarkDirty(ctx, row);

        // No verify triggered because updated == 1 == batch.Count.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    // ── 3. Null token — SELECT-then-verify uses IS NULL predicate ────────────

    [Fact]
    public async Task Update_NullToken_AffectedRows_StaleToken_Detected()
    {
        var (cn, ctx) = CreateNullTokenDb();
        await using var _ = ctx; using var __ = cn;

        // Insert with null token.
        using var ins = cn.CreateCommand();
        ins.CommandText = "INSERT INTO OccNullTokenRow (Payload, Token) VALUES ('original', NULL)";
        ins.ExecuteNonQuery();
        using var sel = cn.CreateCommand();
        sel.CommandText = "SELECT last_insert_rowid()";
        var id = Convert.ToInt32(sel.ExecuteScalar());

        var row = new OccNullTokenRow { Id = id, Payload = "original", Token = null };
        ctx.Attach(row);
        MarkDirty(ctx, row);

        // External writer sets a non-null token.
        using var upd = cn.CreateCommand();
        upd.CommandText = "UPDATE OccNullTokenRow SET Token = X'FF' WHERE Id = @id";
        upd.Parameters.AddWithValue("@id", id);
        upd.ExecuteNonQuery();

        // Our snapshot has Token=null; DB now has Token=0xFF → SELECT WHERE (Token IS NULL) returns 0 → conflict.
        row.Payload = "new value";
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ── 4. DELETE always detects conflict regardless of affected-row semantics ─

    [Fact]
    public async Task Delete_StaleToken_AffectedRows_AlwaysThrowsDbConcurrencyException()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var row = new OccMatrixRow { Payload = "to-delete", Token = new byte[] { 5 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // External writer changes token.
        ForceTokenChange(cn, "OccMatrixRow", row.Id, new byte[] { 55 });

        ctx.Remove(row);

        // DELETE: no same-value ambiguity — always throw when row count mismatches.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Delete_FreshToken_AffectedRows_Succeeds()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var row = new OccMatrixRow { Payload = "to-delete", Token = new byte[] { 5 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // No external change — delete should succeed.
        ctx.Remove(row);
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task Delete_NullToken_StaleToken_AffectedRows_Throws()
    {
        var (cn, ctx) = CreateNullTokenDb();
        await using var _ = ctx; using var __ = cn;

        using var ins = cn.CreateCommand();
        ins.CommandText = "INSERT INTO OccNullTokenRow (Payload, Token) VALUES ('x', NULL)";
        ins.ExecuteNonQuery();
        using var sel = cn.CreateCommand();
        sel.CommandText = "SELECT last_insert_rowid()";
        var id = Convert.ToInt32(sel.ExecuteScalar());

        var row = new OccNullTokenRow { Id = id, Payload = "x", Token = null };
        ctx.Attach(row);
        ctx.Remove(row);

        // External writer sets non-null token.
        using var upd = cn.CreateCommand();
        upd.CommandText = "UPDATE OccNullTokenRow SET Token = X'AA' WHERE Id = @id";
        upd.Parameters.AddWithValue("@id", id);
        upd.ExecuteNonQuery();

        // DELETE WHERE (Token IS NULL) matches 0 rows → throw.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ── 5. Batch: partial stale token surfaces conflict ───────────────────────

    [Fact]
    public async Task Update_BatchPartialStale_AffectedRows_Throws()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var r1 = new OccMatrixRow { Payload = "row1", Token = new byte[] { 1 } };
        var r2 = new OccMatrixRow { Payload = "row2", Token = new byte[] { 2 } };
        var r3 = new OccMatrixRow { Payload = "row3", Token = new byte[] { 3 } };
        ctx.Add(r1); ctx.Add(r2); ctx.Add(r3);
        await ctx.SaveChangesAsync();

        // Make r2 stale.
        ForceTokenChange(cn, "OccMatrixRow", r2.Id, new byte[] { 200 });

        r1.Payload = "row1-v2"; r2.Payload = "row2-v2"; r3.Payload = "row3-v2";
        MarkAllDirty(ctx);

        // Batch UPDATE returns 2 (r1, r3 matched); SELECT-then-verify: r2 token missing → count 2 ≠ 3 → throw.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Update_BatchAllFresh_AffectedRows_NoException()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var r1 = new OccMatrixRow { Payload = "r1", Token = new byte[] { 1 } };
        var r2 = new OccMatrixRow { Payload = "r2", Token = new byte[] { 2 } };
        ctx.Add(r1); ctx.Add(r2);
        await ctx.SaveChangesAsync();

        // No external changes.
        r1.Payload = "r1-v2"; r2.Payload = "r2-v2";
        MarkAllDirty(ctx);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    // ── 6. Composite key — SELECT-then-verify generates multi-column predicate ──

    [Fact]
    public async Task Update_CompositeKey_StaleToken_AffectedRows_Throws()
    {
        var (cn, ctx) = CreateCompositeDb();
        await using var _ = ctx; using var __ = cn;

        // Insert directly.
        using var ins = cn.CreateCommand();
        ins.CommandText = "INSERT INTO OccCompositeRow VALUES (1, 42, 'original', X'01')";
        ins.ExecuteNonQuery();

        var row = new OccCompositeRow { TenantId = 1, RowId = 42, Payload = "original", Token = new byte[] { 1 } };
        ctx.Attach(row);
        MarkDirty(ctx, row);

        // External writer changes token.
        using var upd = cn.CreateCommand();
        upd.CommandText = "UPDATE OccCompositeRow SET Token = X'FF' WHERE TenantId=1 AND RowId=42";
        upd.ExecuteNonQuery();

        row.Payload = "modified";
        // VerifyUpdateOccAsync must generate: WHERE (TenantId=@v0 AND RowId=@v1 AND (Token=@v2 OR ...))
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Update_CompositeKey_FreshToken_AffectedRows_Succeeds()
    {
        var (cn, ctx) = CreateCompositeDb();
        await using var _ = ctx; using var __ = cn;

        using var ins = cn.CreateCommand();
        ins.CommandText = "INSERT INTO OccCompositeRow VALUES (2, 99, 'data', X'07')";
        ins.ExecuteNonQuery();

        var row = new OccCompositeRow { TenantId = 2, RowId = 99, Payload = "data", Token = new byte[] { 7 } };
        ctx.Attach(row);
        MarkDirty(ctx, row);

        row.Payload = "data-v2";
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    // ── 7. Residual gap — same-value token conflict is undetectable ───────────

    /// <summary>
    /// Documents the residual gap: if a concurrent writer sets the token to the SAME
    /// new value AND our UPDATE matches (token WHERE clause still satisfied), the UPDATE
    /// returns 1 row and neither the rowcount check nor SELECT-then-verify triggers.
    /// This is an accepted trade-off at all OCC rowcount granularities.
    /// </summary>
    [Fact]
    public async Task Update_SameValueToken_ResidualGap_NotDetected()
    {
        var (cn, ctx) = CreateAffectedRowsDb();
        await using var _ = ctx; using var __ = cn;

        var token = new byte[] { 77 };
        var row = new OccMatrixRow { Payload = "original", Token = token };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // Concurrent writer changes Payload but sets Token to the SAME byte value.
        using var upd = cn.CreateCommand();
        upd.CommandText = "UPDATE OccMatrixRow SET Payload='concurrent', Token=@t WHERE Id=@id";
        upd.Parameters.AddWithValue("@t", token);
        upd.Parameters.AddWithValue("@id", row.Id);
        upd.ExecuteNonQuery();

        // Our UPDATE: WHERE pk=@pk AND token=@{77} → still matches (token unchanged) → 1 row returned.
        // No verify triggered because updated == batch.Count.
        row.Payload = "local-change";
        MarkDirty(ctx, row);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex); // Residual gap: concurrent overwrite is undetected.
    }

    // ── 8. MatchedRows provider contrast ─────────────────────────────────────

    /// <summary>
    /// With matched-row semantics (UseAffectedRowsSemantics=false), the rowcount check
    /// fires directly without SELECT-then-verify. Stale token always detected.
    /// </summary>
    [Fact]
    public async Task Update_StaleToken_MatchedRows_DirectRowcountCheck_Throws()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE OccMatrixRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        await using var ctx = new DbContext(cn, new SqliteProvider()); // matched-row

        var row = new OccMatrixRow { Payload = "x", Token = new byte[] { 1 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        ForceTokenChange(cn, "OccMatrixRow", row.Id, new byte[] { 99 });
        row.Payload = "x-v2";
        MarkDirty(ctx, row);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ── 9. AffectedRows + no concurrency token → no OCC check runs at all ────

    [Table("OccNoToken")]
    private class OccNoToken
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        // No [Timestamp] attribute — OCC disabled for this entity.
    }

    [Fact]
    public async Task Update_NoToken_AffectedRows_NoOccCheck_AlwaysSucceeds()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var init = cn.CreateCommand();
        init.CommandText = "CREATE TABLE OccNoToken (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL)";
        init.ExecuteNonQuery();
        await using var ctx = new DbContext(cn, new AffectedRowsSqliteProvider(),
            new DbContextOptions { RequireMatchedRowOccSemantics = false });

        var row = new OccNoToken { Payload = "data" };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        row.Payload = "data-v2";
        MarkDirty(ctx, row);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex); // No TimestampColumn → OCC check not executed.
    }

    // ── 10. Verify the affected-row / matched-row flag for known providers ────

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
    public void PostgresProvider_UseAffectedRowsSemantics_IsFalse()
        => Assert.False(new PostgresProvider(new SqliteParameterFactory()).UseAffectedRowsSemantics);

    // ── 11. Provider-matrix: AffectedRowsSqliteProvider simulates MySQL path ─

    [Theory]
    [InlineData(true)]   // AffectedRows → SELECT-then-verify
    [InlineData(false)]  // MatchedRows  → direct rowcount
    public async Task Update_StaleToken_BothSemantics_Throw(bool affectedRowMode)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var init = cn.CreateCommand();
        init.CommandText = "CREATE TABLE OccMatrixRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
        init.ExecuteNonQuery();

        DatabaseProvider provider = affectedRowMode
            ? (DatabaseProvider)new AffectedRowsSqliteProvider()
            : new SqliteProvider();

        var opts = affectedRowMode
            ? new DbContextOptions { RequireMatchedRowOccSemantics = false }
            : new DbContextOptions();
        await using var ctx = new DbContext(cn, provider, opts);

        var row = new OccMatrixRow { Payload = "x", Token = new byte[] { 1 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        ForceTokenChange(cn, "OccMatrixRow", row.Id, new byte[] { 50 });
        row.Payload = "x-v2";
        MarkDirty(ctx, row);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Theory]
    [InlineData(true)]   // AffectedRows
    [InlineData(false)]  // MatchedRows
    public async Task Delete_StaleToken_BothSemantics_Throw(bool affectedRowMode)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var init = cn.CreateCommand();
        init.CommandText = "CREATE TABLE OccMatrixRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
        init.ExecuteNonQuery();

        DatabaseProvider provider = affectedRowMode
            ? (DatabaseProvider)new AffectedRowsSqliteProvider()
            : new SqliteProvider();

        var opts = affectedRowMode
            ? new DbContextOptions { RequireMatchedRowOccSemantics = false }
            : new DbContextOptions();
        await using var ctx = new DbContext(cn, provider, opts);

        var row = new OccMatrixRow { Payload = "x", Token = new byte[] { 1 } };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        ForceTokenChange(cn, "OccMatrixRow", row.Id, new byte[] { 50 });
        ctx.Remove(row);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }
}
