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
/// QP-1: Verifies null-tenant-ID handling in the global filter expression builder.
/// Previously, returning null from GetCurrentTenantId() when the tenant column is a
/// non-nullable value type (e.g. int) would crash with an unhandled ArgumentException
/// inside Expression.Constant(null, typeof(int)).
///
/// With the fix:
/// - Null + non-nullable column → deterministic NormConfigurationException
/// - Null + nullable column (int?, string) → query emits WHERE col IS NULL
/// </summary>
public class TenantNullHandlingTests
{
    // ── Entities ──────────────────────────────────────────────────────────────

    [Table("TnhNonNullableItem")]
    private class NonNullableIntTenantItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int TenantId { get; set; }   // non-nullable value type
    }

    [Table("TnhNullableIntItem")]
    private class NullableIntTenantItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int? TenantId { get; set; }  // nullable int
    }

    [Table("TnhStringItem")]
    private class StringTenantItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? TenantId { get; set; }  // nullable reference type
    }

    // ── Tenant providers ──────────────────────────────────────────────────────

    private sealed class NullTenantProvider : ITenantProvider
    {
        public object GetCurrentTenantId() => null!;
    }

    private sealed class FixedIntTenantProvider : ITenantProvider
    {
        private readonly int _id;
        public FixedIntTenantProvider(int id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn, ITenantProvider provider)
        => new DbContext(cn, new SqliteProvider(), new DbContextOptions { TenantProvider = provider });

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Null tenant ID with a non-nullable int column must throw NormConfigurationException,
    /// not an unhandled ArgumentException from Expression.Constant.
    /// </summary>
    [Fact]
    public void NullTenantId_NonNullableIntColumn_ThrowsNormConfigurationException()
    {
        using var cn = CreateConnection();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TnhNonNullableItem (Id INTEGER PRIMARY KEY, Name TEXT, TenantId INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();

        using var ctx = CreateCtx(cn, new NullTenantProvider());

        var ex = Assert.Throws<NormConfigurationException>(() =>
            ctx.Query<NonNullableIntTenantItem>().ToList());

        Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TenantId", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Null tenant ID with a nullable int? column should succeed and emit WHERE TenantId IS NULL,
    /// returning only rows where TenantId is NULL.
    /// </summary>
    [Fact]
    public async Task NullTenantId_NullableIntColumn_EmitsIsNullFilter()
    {
        using var cn = CreateConnection();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TnhNullableIntItem (Id INTEGER PRIMARY KEY, Name TEXT, TenantId INTEGER);";
            cmd.ExecuteNonQuery();
        }

        // Insert: one null-tenant row, one with tenant 42
        await InsertAsync(cn, "INSERT INTO TnhNullableIntItem (Id, Name, TenantId) VALUES (1, 'NullRow', NULL)");
        await InsertAsync(cn, "INSERT INTO TnhNullableIntItem (Id, Name, TenantId) VALUES (2, 'TenantRow', 42)");

        using var ctx = CreateCtx(cn, new NullTenantProvider());

        var results = ctx.Query<NullableIntTenantItem>().ToList();

        // Only the null-tenant row should be returned
        Assert.Single(results);
        Assert.Equal("NullRow", results[0].Name);
        Assert.Null(results[0].TenantId);
    }

    /// <summary>
    /// Null tenant ID with a nullable string column should succeed and emit WHERE TenantId IS NULL.
    /// </summary>
    [Fact]
    public async Task NullTenantId_StringColumn_EmitsIsNullFilter()
    {
        using var cn = CreateConnection();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TnhStringItem (Id INTEGER PRIMARY KEY, Name TEXT, TenantId TEXT);";
            cmd.ExecuteNonQuery();
        }

        await InsertAsync(cn, "INSERT INTO TnhStringItem (Id, Name, TenantId) VALUES (1, 'NullRow', NULL)");
        await InsertAsync(cn, "INSERT INTO TnhStringItem (Id, Name, TenantId) VALUES (2, 'TenantRow', 'abc')");

        using var ctx = CreateCtx(cn, new NullTenantProvider());

        var results = ctx.Query<StringTenantItem>().ToList();

        Assert.Single(results);
        Assert.Equal("NullRow", results[0].Name);
        Assert.Null(results[0].TenantId);
    }

    /// <summary>
    /// Regression guard: valid non-null tenant ID with non-nullable column should filter correctly.
    /// </summary>
    [Fact]
    public async Task ValidTenantId_NonNullableColumn_FiltersCorrectly()
    {
        using var cn = CreateConnection();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TnhNonNullableItem (Id INTEGER PRIMARY KEY, Name TEXT, TenantId INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        await InsertAsync(cn, "INSERT INTO TnhNonNullableItem (Id, Name, TenantId) VALUES (1, 'T1Row', 1)");
        await InsertAsync(cn, "INSERT INTO TnhNonNullableItem (Id, Name, TenantId) VALUES (2, 'T2Row', 2)");

        using var ctx = CreateCtx(cn, new FixedIntTenantProvider(1));

        var results = ctx.Query<NonNullableIntTenantItem>().ToList();

        Assert.Single(results);
        Assert.Equal("T1Row", results[0].Name);
        Assert.Equal(1, results[0].TenantId);
    }

    private static async Task InsertAsync(SqliteConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
