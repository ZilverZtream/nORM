using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Correlated scalar Queryable aggregates in predicates — Count/LongCount/Sum/
/// Min/Max/Average over an explicit `ctx.Query&lt;T&gt;()` subquery — lower to
/// correlated scalar subqueries, correlating outer references through the
/// shared parameter mappings and slicing the projection into the aggregate.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedScalarAggregateSubqueryTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CsaParent_Test")]
    public class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Threshold { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CsaChild_Test")]
    public class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Parent[] Parents, Child[] Children) CreateDb()
    {
        var cs = $"Data Source=file:csagg_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CsaParent_Test (Id INTEGER PRIMARY KEY, Threshold INTEGER NOT NULL);" +
                "CREATE TABLE CsaChild_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var parents = new[]
        {
            new Parent { Id = 1, Threshold = 50 },
            new Parent { Id = 2, Threshold = 30 },
            new Parent { Id = 3, Threshold = 100 },
            new Parent { Id = 4, Threshold = 10 },
        };
        var children = new[]
        {
            new Child { Id = 1, ParentId = 1, Amount = 20 },
            new Child { Id = 2, ParentId = 1, Amount = 45 },
            new Child { Id = 3, ParentId = 2, Amount = 15 },
            new Child { Id = 4, ParentId = 2, Amount = 15 },
            new Child { Id = 5, ParentId = 2, Amount = 60 },
            new Child { Id = 6, ParentId = 3, Amount = 5 },
            // Parent 4 has no children: aggregates over its subquery are NULL.
        };
        return (keeper, ctx, parents, children);
    }

    private static async Task SeedAsync(DbContext ctx, Parent[] parents, Child[] children)
    {
        foreach (var p in parents) ctx.Add(p);
        foreach (var c in children) ctx.Add(c);
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Correlated_Count_with_inner_filter_matches_linq()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        var expected = parents
            .Where(p => children.Count(c => c.ParentId == p.Id && c.Amount >= 15) >= 2)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Amount >= 15) >= 2)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_Sum_against_outer_column_matches_linq()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Empty subquery sums to SQL NULL, which no comparison matches — the
        // childless parent 4 stays excluded exactly as LINQ's Sum()=0 < 10 does not...
        // LINQ Sum over empty = 0; 0 > Threshold(10) is false, so both exclude it.
        var expected = parents
            .Where(p => children.Where(c => c.ParentId == p.Id).Sum(c => c.Amount) > p.Threshold)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Sum(c => c.Amount) > p.Threshold)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_Max_over_projected_subquery_matches_linq()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        var expected = parents
            .Where(p => children.Where(c => c.ParentId == p.Id).Select(c => c.Amount).Any()
                        && children.Where(c => c.ParentId == p.Id).Select(c => c.Amount).Max() < p.Threshold)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Select(c => c.Amount).Max() < p.Threshold)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_Min_with_selector_matches_linq()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        var expected = parents
            .Where(p => children.Any(c => c.ParentId == p.Id)
                        && children.Where(c => c.ParentId == p.Id).Min(c => c.Amount) >= 15)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Min(c => c.Amount) >= 15)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_Average_compares_fractionally()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Parent 2's amounts are 15,15,60 — C# Average = 30.0 exactly; use a
        // fractional cutoff so integer-truncated AVG would flip the result.
        var expected = parents
            .Where(p => children.Any(c => c.ParentId == p.Id)
                        && children.Where(c => c.ParentId == p.Id).Average(c => c.Amount) > 29.5)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Average(c => c.Amount) > 29.5)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_Count_over_distinct_projection_counts_distinct_values()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Parent 2 has amounts 15,15,60: three rows but two DISTINCT values.
        var expected = parents
            .Where(p => children.Where(c => c.ParentId == p.Id).Select(c => c.Amount).Distinct().Count() == 2)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Select(c => c.Amount).Distinct().Count() == 2)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_Any_with_closure_and_outer_closure_binds_both()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // The ctx capture inside the subquery is a closure member the extractor
        // walks like any other; the inner and outer closures around it must still
        // bind their own slots.
        var cut = 15;
        var limit = 60;
        var expected = parents
            .Where(p => children.Any(c => c.ParentId == p.Id && c.Amount >= cut) && p.Threshold <= limit)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Any(c => c.ParentId == p.Id && c.Amount >= cut) && p.Threshold <= limit)
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Windowed_subquery_aggregate_fails_closed()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Take windows the subquery's rows; a sliced aggregate would apply the
        // LIMIT to the one-row aggregate result instead of the inputs.
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Take(2).Sum(c => c.Amount) > 10)
                .ToListAsync());
    }
}
