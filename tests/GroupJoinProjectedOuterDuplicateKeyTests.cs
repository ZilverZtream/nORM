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
/// GroupJoin yields one result per OUTER ELEMENT. When the outer sequence is a
/// projection (scalar or DTO) rather than the entity, duplicate outer elements
/// must each produce their own result — segmenting the flattened LEFT JOIN
/// stream by the join key alone would fuse outers that share a key value and
/// silently drop rows relative to LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinProjectedOuterDuplicateKeyTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GjPoDept_Test")]
    public class Emp
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("GjPoTask_Test")]
    public class TaskRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public int Hours { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var dbName = $"gjpo_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GjPoDept_Test (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Name TEXT NOT NULL);
                CREATE TABLE GjPoTask_Test (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Hours INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()));
    }

    private static async Task SeedAsync(DbContext ctx)
    {
        // Two employees share DeptId=1, two share DeptId=2 — duplicate outer keys
        // after projecting the entity away.
        ctx.Add(new Emp { Id = 1, DeptId = 1, Name = "a" });
        ctx.Add(new Emp { Id = 2, DeptId = 1, Name = "b" });
        ctx.Add(new Emp { Id = 3, DeptId = 2, Name = "c" });
        ctx.Add(new Emp { Id = 4, DeptId = 2, Name = "d" });
        ctx.Add(new Emp { Id = 5, DeptId = 3, Name = "e" });
        ctx.Add(new TaskRow { Id = 1, DeptId = 1, Hours = 10 });
        ctx.Add(new TaskRow { Id = 2, DeptId = 1, Hours = 20 });
        ctx.Add(new TaskRow { Id = 3, DeptId = 2, Hours = 30 });
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Dto_projected_outer_with_duplicate_keys_yields_one_result_per_outer_row()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        var emps = await ctx.Query<Emp>().ToListAsync();
        var tasks = await ctx.Query<TaskRow>().ToListAsync();
        var expected = emps.Select(e => new { e.DeptId, e.Name })
            .GroupJoin(tasks, o => o.DeptId, t => t.DeptId, (o, ts) => new { o.Name, Total = ts.Sum(t => (int?)t.Hours) ?? 0 })
            .OrderBy(x => x.Name).ToList();

        var actual = ctx.Query<Emp>().Select(e => new { e.DeptId, e.Name })
            .GroupJoin(ctx.Query<TaskRow>(), o => o.DeptId, t => t.DeptId, (o, ts) => new { o.Name, Total = ts.Sum(t => (int?)t.Hours) ?? 0 })
            .AsEnumerable().OrderBy(x => x.Name).ToList();

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.True(expected[i].Name == actual[i].Name && expected[i].Total == actual[i].Total,
                $"row {i}: expected ({expected[i].Name},{expected[i].Total}) got ({actual[i].Name},{actual[i].Total})");
        }
    }

    [Fact]
    public async Task Scalar_projected_outer_with_duplicate_keys_yields_one_result_per_outer_row()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        var emps = await ctx.Query<Emp>().ToListAsync();
        var tasks = await ctx.Query<TaskRow>().ToListAsync();
        var expected = emps.Select(e => e.DeptId)
            .GroupJoin(tasks, d => d, t => t.DeptId, (d, ts) => new { Dept = d, Cnt = ts.Count() })
            .OrderBy(x => x.Dept).ThenBy(x => x.Cnt).ToList();

        var actual = ctx.Query<Emp>().Select(e => e.DeptId)
            .GroupJoin(ctx.Query<TaskRow>(), d => d, t => t.DeptId, (d, ts) => new { Dept = d, Cnt = ts.Count() })
            .AsEnumerable().OrderBy(x => x.Dept).ThenBy(x => x.Cnt).ToList();

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.True(expected[i].Dept == actual[i].Dept && expected[i].Cnt == actual[i].Cnt,
                $"row {i}: expected ({expected[i].Dept},{expected[i].Cnt}) got ({actual[i].Dept},{actual[i].Cnt})");
        }
    }

    [Fact]
    public async Task Projection_closure_rebinds_across_cached_plan_executions()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        var emps = await ctx.Query<Emp>().ToListAsync();
        var tasks = await ctx.Query<TaskRow>().ToListAsync();

        // The modulus lives in the OUTER PROJECTION, which translation composes into
        // the result selector; the cached plan must rebind the CURRENT value, not
        // replay the first execution's.
        for (var m = 2; m <= 4; m++)
        {
            var mod = m;
            var expected = emps.Select(e => e.DeptId % mod)
                .GroupJoin(tasks, v => v, t => t.DeptId % mod, (v, ts) => new { V = v, N = ts.Count() })
                .OrderBy(x => x.V).ThenBy(x => x.N).ToList();

            var actual = ctx.Query<Emp>().Select(e => e.DeptId % mod)
                .GroupJoin(ctx.Query<TaskRow>(), v => v, t => t.DeptId % mod, (v, ts) => new { V = v, N = ts.Count() })
                .AsEnumerable().OrderBy(x => x.V).ThenBy(x => x.N).ToList();

            Assert.True(expected.SequenceEqual(actual),
                $"m={mod}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Dto_projected_outer_duplicate_keys_with_closure_in_result_selector()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        var emps = await ctx.Query<Emp>().ToListAsync();
        var tasks = await ctx.Query<TaskRow>().ToListAsync();

        for (var bonus = 100; bonus <= 300; bonus += 100)
        {
            var b = bonus; // closure captured per iteration: cached plans must rebind
            var expected = emps.Select(e => new { e.DeptId, e.Name })
                .GroupJoin(tasks, o => o.DeptId, t => t.DeptId, (o, ts) => new { o.Name, Score = (ts.Sum(t => (int?)t.Hours) ?? 0) + b })
                .OrderBy(x => x.Name).ToList();

            var actual = ctx.Query<Emp>().Select(e => new { e.DeptId, e.Name })
                .GroupJoin(ctx.Query<TaskRow>(), o => o.DeptId, t => t.DeptId, (o, ts) => new { o.Name, Score = (ts.Sum(t => (int?)t.Hours) ?? 0) + b })
                .AsEnumerable().OrderBy(x => x.Name).ToList();

            Assert.Equal(expected.Count, actual.Count);
            for (var i = 0; i < expected.Count; i++)
            {
                Assert.True(expected[i].Name == actual[i].Name && expected[i].Score == actual[i].Score,
                    $"bonus={b} row {i}: expected ({expected[i].Name},{expected[i].Score}) got ({actual[i].Name},{actual[i].Score})");
            }
        }
    }
}
