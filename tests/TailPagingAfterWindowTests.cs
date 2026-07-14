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
/// TakeLast/SkipLast directly after a Take/Skip window must count from the end
/// of the WINDOW, matching LINQ-to-Objects: the windowed source wraps as a
/// derived table and the tail pages the flipped outer, never the full table.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TailPagingAfterWindowTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("TailWin_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Row[] Rows) CreateDb()
    {
        var cs = $"Data Source=file:tailwin_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TailWin_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var rows = Enumerable.Range(1, 12).Select(i => new Row { Id = i, Val = 100 - i * 3 }).ToArray();
        return (keeper, ctx, rows);
    }

    [Fact]
    public async Task TakeLast_after_Take_returns_the_window_tail()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var (n, m) in new[] { (7, 3), (5, 5), (4, 9), (3, 1) })
        {
            var expected = rows.OrderBy(r => r.Val).Take(n).TakeLast(m).Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(n).TakeLast(m).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"Take({n}).TakeLast({m}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task SkipLast_after_Take_drops_the_window_tail()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var (n, m) in new[] { (8, 3), (6, 6), (5, 8) })
        {
            var expected = rows.OrderBy(r => r.Val).Take(n).SkipLast(m).Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(n).SkipLast(m).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"Take({n}).SkipLast({m}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task TakeLast_after_Skip_counts_from_the_remaining_tail()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        foreach (var (s, m) in new[] { (4, 3), (9, 5), (12, 2) })
        {
            var expected = rows.OrderByDescending(r => r.Val).Skip(s).TakeLast(m).Select(r => r.Id).ToList();
            var actual = (await ctx.Query<Row>().OrderByDescending(r => r.Val).Skip(s).TakeLast(m).ToListAsync())
                .Select(r => r.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"Skip({s}).TakeLast({m}): expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task TakeLast_after_unordered_Take_uses_key_order()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        // No explicit OrderBy: nORM's Take window and the tail both fall back to
        // primary-key order, matching the in-memory oracle ordered by Id.
        var expected = rows.OrderBy(r => r.Id).Take(6).TakeLast(2).Select(r => r.Id).ToList();
        var actual = (await ctx.Query<Row>().Take(6).TakeLast(2).ToListAsync())
            .Select(r => r.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task TakeLast_after_filtered_window_matches_linq()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();

        // Where-after-Take already wraps the window as a subquery and re-derives
        // the ordering, so the tail operator pages the filtered window correctly.
        var expected = rows.OrderBy(r => r.Val).Take(8).Where(r => r.Id > 2).TakeLast(3)
            .Select(r => r.Id).ToList();
        var actual = (await ctx.Query<Row>().OrderBy(r => r.Val).Take(8).Where(r => r.Id > 2).TakeLast(3).ToListAsync())
            .Select(r => r.Id).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }
}
