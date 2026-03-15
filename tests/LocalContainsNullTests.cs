using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that local-collection Contains handles null elements correctly.
/// SQL `col IN (NULL, @p1)` never matches null rows; only `col IS NULL` does.
/// Fix: emit (col IN (...) OR col IS NULL) when the local collection contains nulls.
/// </summary>
public class LocalContainsNullTests : TestBase
{
    [Table("LcnItem")]
    private class LcnItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public int? NullableInt { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE LcnItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, NullableInt INTEGER)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(SqliteConnection cn, int? val)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO LcnItem (NullableInt) VALUES (@v)";
        cmd.Parameters.AddWithValue("@v", val.HasValue ? (object)val.Value : DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ─── Execution tests (SQLite) ──────────────────────────────────────────

    [Fact]
    public async Task Where_localContains_withNull_matchesNullRows()
    {
        // Collection {null, 1} should return both the null row and the row with value 1.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null); // id=1 — null row
        Insert(cn, 1);    // id=2 — value 1
        Insert(cn, 2);    // id=3 — value 2 (not in collection)

        int?[] collection = [null, 1];
        var results = await ctx.Query<LcnItem>()
            .Where(e => collection.Contains(e.NullableInt))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NullableInt == null);
        Assert.Contains(results, r => r.NullableInt == 1);
    }

    [Fact]
    public async Task Where_localContains_onlyNull_matchesOnlyNullRows()
    {
        // Collection {null} should return only the null row.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null); // id=1 — null row
        Insert(cn, 1);    // id=2 — non-null row

        int?[] collection = [null];
        var results = await ctx.Query<LcnItem>()
            .Where(e => collection.Contains(e.NullableInt))
            .ToListAsync();

        Assert.Single(results);
        Assert.Null(results[0].NullableInt);
    }

    [Fact]
    public async Task Where_localContains_noNull_behaviorUnchanged()
    {
        // Non-regression: collection without nulls still returns correct rows.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null); // id=1 — null row (not in collection)
        Insert(cn, 1);    // id=2
        Insert(cn, 2);    // id=3

        int[] collection = [1, 2];
        var results = await ctx.Query<LcnItem>()
            .Where(e => collection.Contains(e.Id))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Contains(r.Id, collection));
    }

    [Fact]
    public async Task Where_localContains_emptyCollection_returnsNothing()
    {
        // Empty collection should return no rows (1=0 predicate).
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, 1);
        Insert(cn, 2);

        int?[] collection = [];
        var results = await ctx.Query<LcnItem>()
            .Where(e => collection.Contains(e.NullableInt))
            .ToListAsync();

        Assert.Empty(results);
    }

    // ─── Provider-matrix SQL generation tests ─────────────────────────────

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void LocalContains_WithNullAndNonNull_EmitsInAndIsNull(ProviderKind providerKind)
    {
        // Collection {null, 1, 2} → SQL must contain both IN ( and IS NULL.
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        int?[] collection = [null, 1, 2];
        var (sql, parameters) = Translate<LcnItem>(
            e => collection.Contains(e.NullableInt),
            connection, provider);

        Assert.Contains("IN (", sql);
        Assert.Contains("IS NULL", sql);
        // Only non-null items become parameters
        Assert.Equal(2, parameters.Count);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void LocalContains_WithoutNull_EmitsOnlyIn(ProviderKind providerKind)
    {
        // Collection {1, 2} → SQL is just col IN (...), no IS NULL.
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        int?[] collection = [1, 2];
        var (sql, parameters) = Translate<LcnItem>(
            e => collection.Contains(e.NullableInt),
            connection, provider);

        Assert.Contains("IN (", sql);
        Assert.DoesNotContain("IS NULL", sql);
        Assert.Equal(2, parameters.Count);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void LocalContains_OnlyNull_EmitsIsNullOnly(ProviderKind providerKind)
    {
        // Collection {null} → SQL is just col IS NULL, no parameters.
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        int?[] collection = [null];
        var (sql, parameters) = Translate<LcnItem>(
            e => collection.Contains(e.NullableInt),
            connection, provider);

        Assert.Contains("IS NULL", sql);
        Assert.DoesNotContain("IN (", sql);
        Assert.Empty(parameters);
    }
}
