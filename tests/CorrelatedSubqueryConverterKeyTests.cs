using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Correlated subqueries whose correlation predicate or aggregate touches a
/// value-converter column must bind the PROVIDER representation, not the CLR
/// value. A converter that stores an enum as a string, or a status as an int,
/// is compared inside the subquery against the outer column — binding the raw
/// CLR value silently matches nothing (or the wrong rows).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedSubqueryConverterKeyTests
{
    public enum Region { North = 1, South = 2, East = 3 }

    [Table("CsckDept_Test")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public Region Region { get; set; }
    }

    [Table("CsckSale_Test")]
    public class Sale
    {
        [Key] public int Id { get; set; }
        public Region Region { get; set; }
        public int Amount { get; set; }
    }

    private sealed class RegionToStringConverter : IValueConverter
    {
        public Type ModelType => typeof(Region);
        public Type ProviderType => typeof(string);
        public object ConvertToProvider(object? value) => ((Region)value!).ToString();
        public object ConvertFromProvider(object? value) => Enum.Parse<Region>((string)value!);
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:csck_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            // Region stored as its STRING name via the converter.
            cmd.CommandText = """
                CREATE TABLE CsckDept_Test (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL);
                CREATE TABLE CsckSale_Test (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO CsckDept_Test VALUES (1, 'North'), (2, 'South'), (3, 'East');
                INSERT INTO CsckSale_Test VALUES
                    (1, 'North', 10), (2, 'North', 20), (3, 'South', 5), (4, 'East', 100);
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().Property(d => d.Region).HasConversion(new RegionToStringConverter());
                mb.Entity<Sale>().Property(s => s.Region).HasConversion(new RegionToStringConverter());
            }
        };
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Correlated_count_on_converter_key_binds_provider_value()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Count sales per dept region — the correlation `s.Region == d.Region`
        // compares converter columns, which must match on the stored string.
        var rows = (await ctx.Query<Dept>()
                .Select(d => new { d.Id, N = ctx.Query<Sale>().Count(s => s.Region == d.Region) })
                .ToListAsync())
            .OrderBy(x => x.Id).ToList();

        Assert.Equal(new[] { (1, 2), (2, 1), (3, 1) }, rows.Select(x => (x.Id, x.N)).ToArray());
    }

    [Fact]
    public async Task Correlated_sum_on_converter_key_binds_provider_value()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var rows = (await ctx.Query<Dept>()
                .Select(d => new { d.Id, S = (int?)ctx.Query<Sale>().Where(s => s.Region == d.Region).Sum(s => s.Amount) })
                .ToListAsync())
            .OrderBy(x => x.Id).ToList();

        Assert.Equal(new[] { (1, (int?)30), (2, 5), (3, 100) }, rows.Select(x => (x.Id, x.S)).ToArray());
    }

    [Fact]
    public async Task Correlated_predicate_on_converter_key_filters_correctly()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Depts that have at least two sales in their region.
        var ids = (await ctx.Query<Dept>()
                .Where(d => ctx.Query<Sale>().Count(s => s.Region == d.Region) >= 2)
                .ToListAsync())
            .Select(d => d.Id).OrderBy(i => i).ToList();

        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Correlated_closure_on_converter_column_binds_provider_value()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // A captured enum compared against the converter column inside the
        // subquery must bind the stored string form.
        foreach (var (region, expected) in new[] { (Region.North, 2), (Region.East, 1) })
        {
            var n = (await ctx.Query<Dept>()
                    .Where(d => d.Id == 1)
                    .Select(d => new { C = ctx.Query<Sale>().Count(s => s.Region == region) })
                    .ToListAsync())
                .Single().C;
            Assert.True(n == expected, $"region={region}: expected {expected} sales got {n}");
        }
    }

    [Fact]
    public async Task Predicate_correlated_closure_on_converter_column_binds_provider_value()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Same closure-converter comparison, but the subquery aggregate is in a
        // WHERE predicate (the ETSV route, not the SCV projection route).
        foreach (var (region, expectedCount) in new[] { (Region.North, 3), (Region.South, 3), (Region.East, 3) })
        {
            // All 3 depts survive: the subquery counts sales in the CAPTURED
            // region (a constant per iteration) and compares >= 0, which always
            // holds when the closure binds correctly; a mis-bound closure would
            // still count 0 but the >= 0 stays true — so assert on the COUNT via
            // projection instead to actually observe the value.
            var counts = (await ctx.Query<Dept>()
                    .Where(d => ctx.Query<Sale>().Count(s => s.Region == region) >= 1)
                    .ToListAsync())
                .Count;
            // When the closure binds correctly there IS at least one sale in each
            // seeded region, so every dept row survives the >= 1 filter.
            Assert.True(counts == expectedCount,
                $"region={region}: expected {expectedCount} depts got {counts} — closure converter mis-bound in predicate subquery");
        }
    }

    [Fact]
    public async Task Exists_correlated_closure_on_converter_column_binds_provider_value()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Any() → EXISTS route with a closure-converter comparison.
        foreach (var (region, expectAny) in new[] { (Region.North, true), (Region.North, true) })
        {
            var survived = (await ctx.Query<Dept>()
                    .Where(d => ctx.Query<Sale>().Any(s => s.Region == region))
                    .ToListAsync())
                .Count;
            Assert.True((survived > 0) == expectAny,
                $"region={region}: EXISTS closure converter mis-bound (survived={survived})");
        }
    }
}
