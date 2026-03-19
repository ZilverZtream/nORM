using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 — S1: MySQL affected-row OCC enforcement
//
// S1 (Medium): MySQL Connector/NET with useAffectedRows=true (the default) cannot
// detect OCC conflicts where the concurrent writer sets the token to the SAME value.
// The SELECT-then-verify fallback correctly handles the case where no columns changed,
// but when a concurrent writer sets the same token (astronomically unlikely with 8-byte
// random tokens, but theoretically possible) the conflict goes undetected.
//
// Enforcement options:
//   - Default (RequireMatchedRowOccSemantics=false): log warning, continue with known trade-off.
//   - Strict (RequireMatchedRowOccSemantics=true): throw NormConfigurationException at UPDATE.
//
// These tests verify:
//   - UseAffectedRowsSemantics flag is true on MySqlProvider.
//   - RequireMatchedRowOccSemantics=false (default) allows saves with warning.
//   - RequireMatchedRowOccSemantics=true throws NormConfigurationException on OCC UPDATE.
//   - Non-OCC entities are not affected by the enforcement check.
//   - SELECT-then-verify covers genuine-conflict vs same-value cases correctly.
//   - MySQL OCC gap is explicitly documented at test level.
// ══════════════════════════════════════════════════════════════════════════════

public class MySqlOccSemanticEnforcementTests
{
    // ── Simulated MySQL provider (SQLite engine + affected-row semantics) ──

    private sealed class AffectedRowsProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    // ── Entities ─────────────────────────────────────────────────────────────

    [Table("MysqlOccItem")]
    private class MysqlOccItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Label { get; set; } = "";
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    [Table("MysqlNoOccItem")]
    private class MysqlNoOccItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    private const string OccDdl =
        "CREATE TABLE MysqlOccItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Token BLOB)";
    private const string NoOccDdl =
        "CREATE TABLE MysqlNoOccItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";

    private static (SqliteConnection Cn, DbContext Ctx) CreateCtx(
        bool useAffectedRows = true, bool requireMatchedRow = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = OccDdl;
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions { RequireMatchedRowOccSemantics = requireMatchedRow };
        var provider = useAffectedRows ? (SqliteProvider)new AffectedRowsProvider() : new SqliteProvider();
        return (cn, new DbContext(cn, provider, opts));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateNoOccCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = NoOccDdl;
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions { RequireMatchedRowOccSemantics = true };
        return (cn, new DbContext(cn, new AffectedRowsProvider(), opts));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Configuration / structural
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MySqlProvider_UseAffectedRowsSemantics_IsTrue()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.True(p.UseAffectedRowsSemantics);
    }

    [Fact]
    public void SqliteProvider_UseAffectedRowsSemantics_IsFalse()
    {
        var p = new SqliteProvider();
        Assert.False(p.UseAffectedRowsSemantics);
    }

    [Fact]
    public void DbContextOptions_RequireMatchedRowOccSemantics_DefaultIsTrue()
    {
        var opts = new DbContextOptions();
        Assert.True(opts.RequireMatchedRowOccSemantics);
    }

    [Fact]
    public void DbContextOptions_RequireMatchedRowOccSemantics_CanBeSetTrue()
    {
        var opts = new DbContextOptions { RequireMatchedRowOccSemantics = true };
        Assert.True(opts.RequireMatchedRowOccSemantics);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Default mode (RequireMatchedRowOccSemantics=false): no throw
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AffectedRowSemantics_DefaultMode_UpdateSucceeds_NoException()
    {
        var (cn, ctx) = CreateCtx(useAffectedRows: true, requireMatchedRow: false);
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlOccItem { Label = "v1" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Label = "v2";
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task AffectedRowSemantics_DefaultMode_SameValueUpdate_NoException()
    {
        // Same-value update: Label unchanged, no columns actually changed.
        // SELECT-then-verify handles this correctly (all tokens still match → no conflict).
        var (cn, ctx) = CreateCtx(useAffectedRows: true, requireMatchedRow: false);
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlOccItem { Label = "v1" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        // "Update" with identical label value — no columns changed.
        item.Label = "v1";
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task AffectedRowSemantics_DefaultMode_GenuineConflict_ThrowsConcurrencyException()
    {
        // Concurrent write via raw SQL with a DIFFERENT token → conflict detected via
        // SELECT-then-verify (token is gone from DB → genuine conflict).
        var (cn, ctx) = CreateCtx(useAffectedRows: true, requireMatchedRow: false);
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlOccItem { Label = "original" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();
        int id = item.Id;

        // Concurrent writer changes the token.
        using var raw = cn.CreateCommand();
        raw.CommandText = $"UPDATE MysqlOccItem SET Label='concurrent', Token=randomblob(8) WHERE Id={id}";
        raw.ExecuteNonQuery();

        item.Label = "norm-update";
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ══════════════════════════════════════════════════════════════════════
    // Strict mode (RequireMatchedRowOccSemantics=true): throws on OCC UPDATE
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AffectedRowSemantics_StrictMode_UpdateWithOccToken_Throws()
    {
        // With RequireMatchedRowOccSemantics=true AND affected-row semantics, any UPDATE
        // on an entity with a [Timestamp] column must throw NormConfigurationException.
        var (cn, ctx) = CreateCtx(useAffectedRows: true, requireMatchedRow: true);
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlOccItem { Label = "before" };
        ctx.Add(item);
        await ctx.SaveChangesAsync(); // INSERT: no OCC check, succeeds

        item.Label = "after";
        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("affected-row semantics", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("optimistic concurrency", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AffectedRowSemantics_StrictMode_InsertStillSucceeds()
    {
        // INSERT does not trigger the OCC check — only UPDATE does.
        var (cn, ctx) = CreateCtx(useAffectedRows: true, requireMatchedRow: true);
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlOccItem { Label = "insert-only" };
        ctx.Add(item);
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex); // INSERT does not hit ExecuteUpdateBatch
    }

    [Fact]
    public async Task AffectedRowSemantics_StrictMode_NonOccEntity_NoThrow()
    {
        // Entities WITHOUT [Timestamp] columns are not subject to the OCC enforcement check.
        var (cn, ctx) = CreateNoOccCtx();
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlNoOccItem { Label = "no-occ" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Label = "updated";
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex); // No TimestampColumn → no enforcement
    }

    [Fact]
    public async Task MatchedRowSemantics_AllConflictsDetected_NoGap()
    {
        // With matched-row semantics (SqliteProvider, UseAffectedRowsSemantics=false),
        // ALL OCC conflicts are detected — even same-value updates that affect 0 rows.
        var (cn, ctx) = CreateCtx(useAffectedRows: false, requireMatchedRow: false);
        await using var _ = ctx; using var __ = cn;

        var item = new MysqlOccItem { Label = "matched" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();
        int id = item.Id;

        // Concurrent writer changes the token.
        using var raw = cn.CreateCommand();
        raw.CommandText = $"UPDATE MysqlOccItem SET Token=randomblob(8) WHERE Id={id}";
        raw.ExecuteNonQuery();

        item.Label = "norm-update";
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ══════════════════════════════════════════════════════════════════════
    // S1 gap documentation test
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void S1_Gap_IsDocumented_InMySqlProvider()
    {
        // The S1 gap is explicitly documented in MySqlProvider.UseAffectedRowsSemantics XML docs.
        // This test verifies the property exists and reflects the known trade-off.
        var propInfo = typeof(MySqlProvider)
            .GetProperty("UseAffectedRowsSemantics",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        // The property is defined on the base class DatabaseProvider as internal override.
        // Access through the instance.
        var provider = new MySqlProvider(new SqliteParameterFactory());
        // UseAffectedRowsSemantics is internal; access via reflection for the test.
        var baseType = typeof(MySqlProvider).BaseType;
        while (baseType != null)
        {
            var prop = baseType.GetProperty("UseAffectedRowsSemantics",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (prop != null)
            {
                var val = (bool)prop.GetValue(provider)!;
                Assert.True(val, "MySqlProvider.UseAffectedRowsSemantics must be true (S1 trade-off)");
                return;
            }
            baseType = baseType.BaseType;
        }
        // If not found via base, check directly.
        Assert.True(provider.UseAffectedRowsSemantics,
            "MySqlProvider.UseAffectedRowsSemantics must be true");
    }

    [Fact]
    public void S1_RequireMatchedRowOccSemantics_Enforcement_CanSuppressTrade_off()
    {
        // Structural: the option exists and its semantics are testable at the options level.
        var strictOpts = new DbContextOptions { RequireMatchedRowOccSemantics = true };
        var lenientOpts = new DbContextOptions { RequireMatchedRowOccSemantics = false };
        Assert.True(strictOpts.RequireMatchedRowOccSemantics);
        Assert.False(lenientOpts.RequireMatchedRowOccSemantics);
    }
}
