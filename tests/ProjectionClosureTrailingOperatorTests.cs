using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Closure captures inside a projection register their compiled slots when the
/// projection is rendered at plan-Build time — AFTER every clause-translated
/// slot — while the per-execution value extractor walks closures in expression
/// document order. Any trailing operator that consumes its own closure therefore
/// stresses the pairing between slot registration order and extraction order.
/// A mismatch silently binds one closure's value into the other's slot.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionClosureTrailingOperatorTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("PctRow_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Row[] Rows) CreateDb()
    {
        var cs = $"Data Source=file:pcttrail_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE PctRow_Test (Id INTEGER PRIMARY KEY, IntVal INTEGER NOT NULL, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var rows = new[]
        {
            new Row { Id = 1, IntVal = 10, Name = "alpha" },
            new Row { Id = 2, IntVal = 25, Name = "beta" },
            new Row { Id = 3, IntVal = 40, Name = "gamma" },
            new Row { Id = 4, IntVal = 55, Name = "delta" },
            new Row { Id = 5, IntVal = 70, Name = "epsilon" },
        };
        return (keeper, ctx, rows);
    }

    private static async Task SeedAsync(DbContext ctx, Row[] rows)
    {
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Projected_scalar_contains_binds_projection_and_needle_closures()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // The projection closure (add) renders at Build time; the Contains needle
        // registers at terminal-translation time — swapped pairing makes the
        // needle search for the wrong sum.
        foreach (var (add, needle) in new[] { (5, 30), (100, 140), (5, 140) })
        {
            var expected = rows.Select(r => r.IntVal + add).Contains(needle);
            var actual = await ctx.Query<Row>().Select(r => r.IntVal + add).ContainsAsync(needle);
            Assert.True(expected == actual, $"add={add},needle={needle}: expected {expected} got {actual}");
        }
    }

    [Fact]
    public async Task Projected_rows_concat_second_arm_closure_binds_independently()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // First arm's projection closure renders at Build; the second arm's WHERE
        // closure registers when the set-op arm translates.
        foreach (var (add, cut) in new[] { (1000, 30), (2000, 55) })
        {
            var expected = rows.Select(r => r.IntVal + add)
                .Concat(rows.Where(r => r.IntVal >= cut).Select(r => r.IntVal + add))
                .OrderBy(x => x).ToList();
            var actual = ctx.Query<Row>().Select(r => r.IntVal + add)
                .Concat(ctx.Query<Row>().Where(r => r.IntVal >= cut).Select(r => r.IntVal + add))
                .AsEnumerable()
                .OrderBy(x => x).ToList();
            await Task.CompletedTask;
            Assert.True(expected.SequenceEqual(actual),
                $"add={add},cut={cut}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Projection_closure_after_where_closure_stays_aligned()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Document order [cut, add] matches registration order here (WHERE first,
        // projection at Build) — the control shape for the trailing-operator probes.
        foreach (var (cut, add) in new[] { (25, 7), (40, 3) })
        {
            var expected = rows.Where(r => r.IntVal >= cut).Select(r => r.IntVal + add)
                .OrderBy(x => x).ToList();
            var actual = ctx.Query<Row>().Where(r => r.IntVal >= cut).Select(r => r.IntVal + add)
                .AsEnumerable()
                .OrderBy(x => x).ToList();
            await Task.CompletedTask;
            Assert.True(expected.SequenceEqual(actual),
                $"cut={cut},add={add}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Projected_distinct_count_predicate_closure_binds_after_projection_closure()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Projection closure (mod) + trailing quantifier closure (min) — Any's
        // predicate translates after the projection clause in document order.
        foreach (var (mod, min) in new[] { (3, 1), (7, 5) })
        {
            var expected = rows.Select(r => r.IntVal % mod).Distinct().Any(x => x >= min);
            var actual = ctx.Query<Row>().Select(r => r.IntVal % mod).Distinct().Any(x => x >= min);
            await Task.CompletedTask;
            Assert.True(expected == actual, $"mod={mod},min={min}: expected {expected} got {actual}");
        }
    }

    [Fact]
    public async Task Split_projection_client_closure_does_not_shift_server_slots()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Warn policy splits the projection: serverAdd stays in SQL, clientTag runs
        // in the client tail. The extractor walks BOTH closures in document order —
        // the client-only closure must not consume (or shift) a server slot.
        var options = new DbContextOptions { ClientEvaluationPolicy = ClientEvaluationPolicy.Warn };
        var cn2 = new SqliteConnection(keeper.ConnectionString);
        cn2.Open();
        await using var ctx2 = new DbContext(cn2, new SqliteProvider(), options);

        foreach (var (serverAdd, clientTag, cut) in new[] { (10, "x", 25), (90, "yy", 40) })
        {
            var expected = rows.Where(r => r.IntVal >= cut)
                .Select(r => Tag(r.IntVal + serverAdd, clientTag))
                .OrderBy(x => x, StringComparer.Ordinal).ToList();
            var actual = (await ctx2.Query<Row>().Where(r => r.IntVal >= cut)
                    .Select(r => Tag(r.IntVal + serverAdd, clientTag))
                    .ToListAsync())
                .OrderBy(x => x, StringComparer.Ordinal).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"serverAdd={serverAdd},clientTag={clientTag},cut={cut}: " +
                $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    private static string Tag(int value, string tag) => $"{value}:{tag}";

    [Fact]
    public async Task Closure_order_key_after_projection_closure_stays_aligned()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // The ORDER BY key re-expands the projection and mints its closure (mod)
        // at clause time; the projection closure (add) mints at Build time —
        // document order is the reverse. Ordered comparison so a mis-bound mod
        // changes the sequence.
        foreach (var (add, mod) in new[] { (100, 7), (3, 13) })
        {
            var expected = rows.Select(r => new { r.Id, V = r.IntVal + add })
                .OrderBy(x => x.V % mod).ThenBy(x => x.Id)
                .Select(x => x.V).ToList();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, V = r.IntVal + add })
                    .OrderBy(x => x.V % mod).ThenBy(x => x.Id)
                    .ToListAsync())
                .Select(x => x.V).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"add={add},mod={mod}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Closure_take_count_after_projection_closure_stays_aligned()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // A captured Take count after a closure-bearing projection: a cross-bound
        // pairing would use the projection delta as the row limit.
        foreach (var (add, take) in new[] { (1000, 2), (5000, 4) })
        {
            var expected = rows.OrderBy(r => r.Id).Select(r => r.IntVal + add).Take(take).ToList();
            var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.IntVal + add).Take(take)
                .AsEnumerable().ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"add={add},take={take}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
        await Task.CompletedTask;
    }
}
