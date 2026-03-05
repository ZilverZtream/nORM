using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// QP-1: Verifies that the tenant filter expression builder coerces the runtime type of
/// TenantProvider.GetCurrentTenantId() to the mapped property type before building the
/// Expression.Constant. Without coercion, a type mismatch (e.g. long vs int) throws
/// ArgumentException before any SQL is generated, crashing all tenant-scoped queries.
/// </summary>
public class TenantTypeMismatchTests
{
    // ── Entities with different tenant column CLR types ───────────────────────

    [Table("TtmIntItem")]
    private class IntTenantItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        // Tenant column is int
        public int TenantId { get; set; }
    }

    [Table("TtmGuidItem")]
    private class GuidTenantItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        // Tenant column is Guid
        public Guid TenantId { get; set; }
    }

    // ── Tenant providers returning mismatched runtime types ───────────────────

    private sealed class LongTenantProvider : ITenantProvider
    {
        private readonly long _id;
        public LongTenantProvider(long id) => _id = id;
        // Returns long while entity property is int → mismatch without coercion
        public object GetCurrentTenantId() => _id;
    }

    private sealed class IntTenantProvider : ITenantProvider
    {
        private readonly int _id;
        public IntTenantProvider(int id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private sealed class StringTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public StringTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Schema helpers ────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateIntTenantContext(ITenantProvider provider)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TtmIntItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions { TenantProvider = provider };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx);
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateGuidTenantContext(ITenantProvider provider)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TtmGuidItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions { TenantProvider = provider };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// QP-1: TenantProvider returns boxed long; entity tenant property is int.
    /// Without type coercion this throws ArgumentException in expression construction.
    /// With the fix, the query must execute successfully (result may be empty).
    /// </summary>
    [Fact]
    public async Task TenantFilter_LongProviderAgainstIntColumn_DoesNotThrow()
    {
        // long(42) vs int tenant column — previously threw ArgumentException
        var (cn, ctx) = CreateIntTenantContext(new LongTenantProvider(42L));
        await using var _ = cn;

        // Insert a row directly so there's data to find.
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO TtmIntItem (Id, Name, TenantId) VALUES (1, 'Alice', 42)";
        await cmd.ExecuteNonQueryAsync();

        // QP-1: Must not throw ArgumentException during expression construction.
        var results = ctx.Query<IntTenantItem>().ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    /// <summary>
    /// QP-1: TenantProvider returns string "7"; entity tenant property is int.
    /// String-to-int coercion via Convert.ChangeType should work for parseable values.
    /// </summary>
    [Fact]
    public async Task TenantFilter_StringProviderAgainstIntColumn_CoercesSuccessfully()
    {
        var (cn, ctx) = CreateIntTenantContext(new StringTenantProvider("7"));
        await using var _ = cn;

        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO TtmIntItem (Id, Name, TenantId) VALUES (1, 'Bob', 7)";
        await cmd.ExecuteNonQueryAsync();

        var results = ctx.Query<IntTenantItem>().ToList();
        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    /// <summary>
    /// QP-1: Matching types (int provider, int column) must still work normally — regression guard.
    /// </summary>
    [Fact]
    public async Task TenantFilter_MatchingTypes_WorksAsNormal()
    {
        var (cn, ctx) = CreateIntTenantContext(new IntTenantProvider(5));
        await using var _ = cn;

        await using var setupCmd = cn.CreateCommand();
        setupCmd.CommandText =
            "INSERT INTO TtmIntItem (Id, Name, TenantId) VALUES (1, 'Same', 5);" +
            "INSERT INTO TtmIntItem (Id, Name, TenantId) VALUES (2, 'Other', 99);";
        await setupCmd.ExecuteNonQueryAsync();

        var results = ctx.Query<IntTenantItem>().ToList();
        Assert.Single(results);
        Assert.Equal("Same", results[0].Name);
    }

    /// <summary>
    /// QP-1: String "abc" cannot be converted to int; must throw NormConfigurationException
    /// with an actionable message, NOT a raw ArgumentException from expression construction.
    /// </summary>
    [Fact]
    public void TenantFilter_IncompatibleStringProvider_ThrowsNormConfigurationException()
    {
        var (cn, ctx) = CreateIntTenantContext(new StringTenantProvider("not-an-int"));
        cn.Dispose();

        // The exception is thrown when the expression tree is built (on first query enumeration).
        var ex = Assert.Throws<nORM.Core.NormConfigurationException>(() =>
            ctx.Query<IntTenantItem>().ToList());

        Assert.Contains("TenantProvider", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("int", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
