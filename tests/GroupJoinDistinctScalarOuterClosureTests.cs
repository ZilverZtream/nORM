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
/// GroupJoin over a Select(column).Distinct() scalar outer translates through a
/// derived-table branch whose runtime outer element IS the scalar. A closure
/// captured in the result selector must rebind per execution from the cached
/// plan: the rebind walk composes the projection into the selector the same way
/// translation does, and for a plain-column projection that composition must
/// leave the closure slot sequence unchanged or cache hits would silently
/// replay the first execution's values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinDistinctScalarOuterClosureTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GjDsoEmp_Test")]
    public class Emp
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int DeptId { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("GjDsoTask_Test")]
    public class TaskRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public int Hours { get; set; }
    }

    [Fact]
    public async Task Projection_and_selector_closures_rebind_across_cached_plans()
    {
        var dbName = $"gjdso_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GjDsoEmp_Test (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL);
                CREATE TABLE GjDsoTask_Test (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Hours INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        for (var id = 1; id <= 6; id++)
            ctx.Add(new Emp { Id = id, DeptId = id % 4 });
        ctx.Add(new TaskRow { Id = 1, DeptId = 0, Hours = 10 });
        ctx.Add(new TaskRow { Id = 2, DeptId = 1, Hours = 20 });
        ctx.Add(new TaskRow { Id = 3, DeptId = 1, Hours = 30 });
        ctx.Add(new TaskRow { Id = 4, DeptId = 2, Hours = 40 });
        await ctx.SaveChangesAsync();

        var emps = await ctx.Query<Emp>().ToListAsync();
        var tasks = await ctx.Query<TaskRow>().ToListAsync();

        // Computed projection with a constant: Distinct semantics come from emitting one
        // result per distinct computed key (LINQ: distinct BEFORE the join), with the
        // selector closure rebinding per execution from the cached plan.
        for (var round = 1; round <= 3; round++)
        {
            var bonus = round * 1000;
            var expected = emps.Select(e => e.DeptId % 3).Distinct()
                .GroupJoin(tasks, v => v, t => t.DeptId % 3, (v, ts) => new { V = v, S = (ts.Sum(t => (int?)t.Hours) ?? 0) + bonus })
                .OrderBy(x => x.V).ToList();

            var actual = ctx.Query<Emp>().Select(e => e.DeptId % 3).Distinct()
                .GroupJoin(ctx.Query<TaskRow>(), v => v, t => t.DeptId % 3, (v, ts) => new { V = v, S = (ts.Sum(t => (int?)t.Hours) ?? 0) + bonus })
                .AsEnumerable().OrderBy(x => x.V).ToList();

            Assert.True(expected.SequenceEqual(actual),
                $"computed distinct bonus={bonus}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }

        // A projection that CAPTURES a local variable: the distinct-key delegate is
        // closure-lifted, so the de-dup comparison must use the CURRENT captured value
        // on every cached-plan execution, never the first execution's.
        for (var m = 2; m <= 4; m++)
        {
            var mod = m;
            var expected = emps.Select(e => e.DeptId % mod).Distinct()
                .GroupJoin(tasks, v => v, t => t.DeptId % mod, (v, ts) => new { V = v, N = ts.Count() })
                .OrderBy(x => x.V).ToList();

            var actual = ctx.Query<Emp>().Select(e => e.DeptId % mod).Distinct()
                .GroupJoin(ctx.Query<TaskRow>(), v => v, t => t.DeptId % mod, (v, ts) => new { V = v, N = ts.Count() })
                .AsEnumerable().OrderBy(x => x.V).ToList();

            Assert.True(expected.SequenceEqual(actual),
                $"captured m={mod}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }

        // Plain-column projection (the supported Distinct shape); the closure lives in
        // the result selector and must rebind per execution from the cached plan.
        for (var round = 1; round <= 3; round++)
        {
            var bonus = round * 100;
            var expected = emps.Select(e => e.DeptId).Distinct()
                .GroupJoin(tasks, v => v, t => t.DeptId, (v, ts) => new { V = v, S = (ts.Sum(t => (int?)t.Hours) ?? 0) + bonus })
                .OrderBy(x => x.V).ToList();

            var actual = ctx.Query<Emp>().Select(e => e.DeptId).Distinct()
                .GroupJoin(ctx.Query<TaskRow>(), v => v, t => t.DeptId, (v, ts) => new { V = v, S = (ts.Sum(t => (int?)t.Hours) ?? 0) + bonus })
                .AsEnumerable().OrderBy(x => x.V).ToList();

            Assert.True(expected.SequenceEqual(actual),
                $"bonus={bonus}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }
}
