using System;
using System.Collections.Generic;
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
    public async Task Correlated_count_in_projection_translates_server_side()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Explicit ctx.Query aggregates in PROJECTIONS lower to correlated scalar
        // subqueries — including the ctx-capture alignment slot and closures.
        foreach (var cut in new[] { 15, 30 })
        {
            var expected = parents
                .Select(p => (p.Id, N: children.Count(c => c.ParentId == p.Id && c.Amount >= cut)))
                .OrderBy(t => t.Id).ToList();
            var actual = (await ctx.Query<Parent>()
                .Select(p => new { p.Id, N = ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Amount >= cut) })
                .ToListAsync()).Select(x => (x.Id, x.N)).OrderBy(t => t.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"cut={cut}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Correlated_sum_in_projection_translates_server_side()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Empty subqueries yield SQL NULL; project into int? and mirror with an
        // Any()-guarded oracle (parent 4 has no children).
        var expected = parents
            .Select(p => (p.Id, S: children.Any(c => c.ParentId == p.Id)
                ? (int?)children.Where(c => c.ParentId == p.Id).Sum(c => c.Amount)
                : null))
            .OrderBy(t => t.Id).ToList();
        var actual = (await ctx.Query<Parent>()
            .Select(p => new { p.Id, S = (int?)ctx.Query<Child>().Where(c => c.ParentId == p.Id).Sum(c => c.Amount) })
            .ToListAsync()).Select(x => (x.Id, x.S)).OrderBy(t => t.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_count_as_order_key_binds_closures()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        var cut = 15;
        var expected = parents
            .OrderByDescending(p => children.Count(c => c.ParentId == p.Id && c.Amount >= cut))
            .ThenBy(p => p.Id).Select(p => p.Id).ToList();
        var actual = (await ctx.Query<Parent>()
            .OrderByDescending(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Amount >= cut))
            .ThenBy(p => p.Id).ToListAsync()).Select(p => p.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Nested_correlated_any_binds_both_ctx_captures()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        var t = 30;
        var amt = 10;
        var expected = parents
            .Where(p => children.Any(c => c.ParentId == p.Id && c.Amount >= amt
                && parents.Any(q => q.Id == c.ParentId && q.Threshold >= t)))
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Any(c => c.ParentId == p.Id && c.Amount >= amt
                && ctx.Query<Parent>().Any(q => q.Id == c.ParentId && q.Threshold >= t)))
            .ToListAsync()).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Correlated_count_in_bulk_delete_predicate_binds_closures()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        var cut = 15;
        var minCount = 2;
        var doomed = parents
            .Where(p => children.Count(c => c.ParentId == p.Id && c.Amount >= cut) >= minCount)
            .Select(p => p.Id).ToHashSet();
        var affected = await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Amount >= cut) >= minCount)
            .ExecuteDeleteAsync();
        Assert.Equal(doomed.Count, affected);

        using var check = keeper.CreateCommand();
        check.CommandText = "SELECT Id FROM CsaParent_Test ORDER BY Id";
        var survivors = new List<int>();
        using (var reader = check.ExecuteReader())
            while (reader.Read()) survivors.Add(reader.GetInt32(0));
        var expected = parents.Where(p => !doomed.Contains(p.Id)).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(survivors),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", survivors)}]");
    }

    private static readonly Func<DbContext, int, Task<List<Parent>>> _parentsByChildCount =
        Norm.CompileQuery((DbContext c, int minCount) =>
            c.Query<Parent>().Where(p => c.Query<Child>().Count(ch => ch.ParentId == p.Id) >= minCount));

    [Fact]
    public async Task Compiled_query_with_correlated_count_binds_the_free_parameter()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // The compiled lambda's DbContext is a FREE PARAMETER, not a closure —
        // the subquery root consumes it through a different route than ctx-local
        // captures, and the threshold must re-bind per invocation.
        foreach (var min in new[] { 1, 2, 3 })
        {
            var expected = parents.Where(p => children.Count(ch => ch.ParentId == p.Id) >= min)
                .Select(p => p.Id).OrderBy(i => i).ToList();
            var actual = (await _parentsByChildCount(ctx, min)).Select(p => p.Id).OrderBy(i => i).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"min={min}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
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

    [Fact]
    public async Task Sibling_projection_aggregates_bind_independent_slots()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Two ctx captures and two distinct closures in ONE projection — each
        // subquery must reserve its own alignment slot and bind its own cut.
        foreach (var (cutA, cutB) in new[] { (15, 40), (5, 20) })
        {
            var expected = parents
                .Select(p => (p.Id,
                    N: children.Count(c => c.ParentId == p.Id && c.Amount >= cutA),
                    S: children.Any(c => c.ParentId == p.Id && c.Amount >= cutB)
                        ? (int?)children.Where(c => c.ParentId == p.Id && c.Amount >= cutB).Sum(c => c.Amount)
                        : null))
                .OrderBy(t => t.Id).ToList();
            var actual = (await ctx.Query<Parent>()
                .Select(p => new
                {
                    p.Id,
                    N = ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Amount >= cutA),
                    S = (int?)ctx.Query<Child>().Where(c => c.ParentId == p.Id && c.Amount >= cutB).Sum(c => c.Amount)
                })
                .ToListAsync()).Select(x => (x.Id, x.N, x.S)).OrderBy(t => t.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"cuts=({cutA},{cutB}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Projection_aggregate_between_closure_members_keeps_slot_alignment()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Closures BEFORE, INSIDE, and AFTER the aggregate in document order —
        // the ctx capture sits between value-bearing slots and must not shift them.
        foreach (var (before, mid, after) in new[] { (100, 15, 3), (7, 40, 11) })
        {
            var expected = parents
                .Select(p => (A: p.Threshold + before,
                    N: children.Count(c => c.ParentId == p.Id && c.Amount >= mid),
                    B: p.Id * after))
                .OrderBy(t => t.B).ToList();
            var actual = (await ctx.Query<Parent>()
                .Select(p => new
                {
                    A = p.Threshold + before,
                    N = ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Amount >= mid),
                    B = p.Id * after
                })
                .ToListAsync()).Select(x => (x.A, x.N, x.B)).OrderBy(t => t.B).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"({before},{mid},{after}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Projected_chain_aggregates_translate_in_projection()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // Where + Select chains under Min/Max/Average and a Distinct count.
        var expected = parents
            .Select(p => (p.Id,
                Mn: children.Where(c => c.ParentId == p.Id).Select(c => (int?)c.Amount).Min(),
                Mx: children.Where(c => c.ParentId == p.Id).Select(c => (int?)c.Amount).Max(),
                Av: children.Any(c => c.ParentId == p.Id)
                    ? (double?)children.Where(c => c.ParentId == p.Id).Average(c => c.Amount)
                    : null,
                D: children.Where(c => c.ParentId == p.Id).Select(c => c.Amount).Distinct().Count()))
            .OrderBy(t => t.Id).ToList();
        var actual = (await ctx.Query<Parent>()
            .Select(p => new
            {
                p.Id,
                Mn = (int?)ctx.Query<Child>().Where(c => c.ParentId == p.Id).Select(c => c.Amount).Min(),
                Mx = (int?)ctx.Query<Child>().Where(c => c.ParentId == p.Id).Select(c => c.Amount).Max(),
                Av = (double?)ctx.Query<Child>().Where(c => c.ParentId == p.Id).Average(c => c.Amount),
                D = ctx.Query<Child>().Where(c => c.ParentId == p.Id).Select(c => c.Amount).Distinct().Count()
            })
            .ToListAsync()).Select(x => (x.Id, x.Mn, x.Mx, x.Av, x.D)).OrderBy(t => t.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Self_type_nested_aggregate_in_projection_uses_distinct_alias()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // The subquery targets the SAME entity type as the outer query — the
        // inner FROM must bind its own alias, not the outer one.
        var expected = parents
            .Select(p => (p.Id, N: parents.Count(p2 => p2.Id > p.Id)))
            .OrderBy(t => t.Id).ToList();
        var actual = (await ctx.Query<Parent>()
            .Select(p => new { p.Id, N = ctx.Query<Parent>().Count(p2 => p2.Id > p.Id) })
            .ToListAsync()).Select(x => (x.Id, x.N)).OrderBy(t => t.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Projection_aggregate_composed_in_arithmetic_translates()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // The aggregate is a binary-expression OPERAND, not the whole member.
        foreach (var (mul, add) in new[] { (2, 5), (3, 1) })
        {
            var expected = parents
                .Select(p => (p.Id, V: children.Count(c => c.ParentId == p.Id) * mul + add))
                .OrderBy(t => t.Id).ToList();
            var actual = (await ctx.Query<Parent>()
                .Select(p => new { p.Id, V = ctx.Query<Child>().Count(c => c.ParentId == p.Id) * mul + add })
                .ToListAsync()).Select(x => (x.Id, x.V)).OrderBy(t => t.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"({mul},{add}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    public sealed class ParentChildCountDto
    {
        public int Id { get; set; }
        public int N { get; set; }
    }

    private static readonly Func<DbContext, int, Task<List<ParentChildCountDto>>> _parentCountsByCut =
        Norm.CompileQuery((DbContext c, int cut) =>
            c.Query<Parent>().Select(p => new ParentChildCountDto
            {
                Id = p.Id,
                N = c.Query<Child>().Count(ch => ch.ParentId == p.Id && ch.Amount >= cut)
            }));

    [Fact]
    public async Task Compiled_query_with_projection_aggregate_binds_the_free_parameter()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, parents, children);

        // The compiled lambda's DbContext is a FREE PARAMETER — the projection
        // subquery consumes it without a closure slot, and the cut re-binds
        // per invocation.
        foreach (var cut in new[] { 15, 45 })
        {
            var expected = parents
                .Select(p => (p.Id, N: children.Count(ch => ch.ParentId == p.Id && ch.Amount >= cut)))
                .OrderBy(t => t.Id).ToList();
            var actual = (await _parentCountsByCut(ctx, cut))
                .Select(x => (x.Id, x.N)).OrderBy(t => t.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"cut={cut}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }
}
