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
/// TakeWhile/SkipWhile directly after a Take/Skip window must evaluate the
/// predicate over the WINDOW's rows in window order, matching LINQ-to-Objects:
/// the windowed source keeps its ORDER BY + paging clause inside the derived
/// table and the cumulative break flag runs over the wrapped rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TakeSkipWhileAfterWindowTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("WhileWin_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Row[] Rows) CreateDb()
    {
        var cs = $"Data Source=file:whilewin_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE WhileWin_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var rows = Enumerable.Range(1, 12).Select(i => new Row { Id = i, Val = 100 - i * 3 }).ToArray();
        return (keeper, ctx, rows);
    }

    [Fact]
    public async Task TakeWhile_after_Take_stops_within_the_window()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var (n, cut) in new[] { (7, 8), (5, 12), (9, 3), (12, 6) })
        {
            var expected = rows.OrderBy(r => r.Val).Take(n).TakeWhile(r => r.Id > cut)
                .Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(n).TakeWhile(r => r.Id > cut).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"Take({n}).TakeWhile(Id>{cut}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task SkipWhile_after_Take_resumes_within_the_window()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var (n, cut) in new[] { (8, 9), (6, 1), (10, 20) })
        {
            var expected = rows.OrderBy(r => r.Val).Take(n).SkipWhile(r => r.Id > cut)
                .Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(n).SkipWhile(r => r.Id > cut).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"Take({n}).SkipWhile(Id>{cut}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task TakeWhile_after_Skip_evaluates_the_remaining_tail()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var (s, cut) in new[] { (4, 3), (9, 1), (2, 30) })
        {
            var expected = rows.OrderByDescending(r => r.Val).Skip(s).TakeWhile(r => r.Id > cut)
                .Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderByDescending(r => r.Val).Skip(s).TakeWhile(r => r.Id > cut).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"Skip({s}).TakeWhile(Id>{cut}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Indexed_TakeWhile_after_Take_restarts_the_index_at_the_window()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        // The index parameter counts within the WINDOW (LINQ restarts enumeration
        // after Take), not within the full table.
        var expected = rows.OrderBy(r => r.Val).Skip(3).TakeWhile((r, i) => i < 4 || r.Id > 6)
            .Select(r => r.Id).ToList();
        var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Skip(3).TakeWhile((r, i) => i < 4 || r.Id > 6).ToListAsync())
            .Select(r => r.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task TakeWhile_with_closure_cutoff_after_Take_binds_per_execution()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var cut in new[] { 5, 10, 0 })
        {
            var expected = rows.OrderBy(r => r.Val).Take(8).TakeWhile(r => r.Id > cut)
                .Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(8).TakeWhile(r => r.Id > cut).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"cut={cut}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task TakeWhile_after_filtered_window_evaluates_filtered_rows()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var expected = rows.OrderBy(r => r.Val).Take(8).Where(r => r.Id != 9).TakeWhile(r => r.Id > 6)
            .Select(r => r.Id).ToList();
        var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(8).Where(r => r.Id != 9).TakeWhile(r => r.Id > 6).ToListAsync())
            .Select(r => r.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task TakeWhile_after_TakeLast_evaluates_only_the_tail_rows()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        // TakeLast keeps its reversed ORDER BY + LIMIT inside the derived table;
        // stripping that ORDER BY would drop the LIMIT and silently evaluate the
        // predicate over the whole table. The predicate holds for most of the
        // table but breaks inside the tail, so a lost LIMIT changes the result.
        var expected = rows.OrderBy(r => r.Val).TakeLast(5).TakeWhile(r => r.Id >= 2)
            .Select(r => r.Id).ToList();
        var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).TakeLast(5).TakeWhile(r => r.Id >= 2).ToListAsync())
            .Select(r => r.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }
}
