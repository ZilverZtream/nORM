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
/// ExecuteUpdate/ExecuteDelete over ordered, paged, and windowed queries must
/// affect exactly the rows the equivalent SELECT would return: the row set is
/// resolved through a keyed subquery over the full query SQL (ORDER BY + paging
/// kept inside a derived table), never the bare WHERE clause.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderedPagedBulkCudTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BulkWin_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Row[] Rows) CreateDb()
    {
        var cs = $"Data Source=file:bulkwin_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BulkWin_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var rows = Enumerable.Range(1, 12).Select(i => new Row { Id = i, Val = i * 5 % 12 * 10 }).ToArray();
        return (keeper, ctx, rows);
    }

    private static async Task SeedAsync(DbContext ctx, Row[] rows)
    {
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
    }

    /// <summary>
    /// Reads (Id, Val) straight through the keeper connection — the test context
    /// tracks the seeded entities, and a tracked re-query returns the identity-map
    /// instances with their in-memory values, hiding what the bulk UPDATE wrote.
    /// </summary>
    private static List<(int Id, int Val)> ReadRaw(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Id, Val FROM BulkWin_Test ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var result = new List<(int, int)>();
        while (reader.Read())
            result.Add((reader.GetInt32(0), reader.GetInt32(1)));
        return result;
    }

    [Fact]
    public async Task ExecuteDelete_after_ordered_Take_deletes_only_the_window()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var doomed = rows.OrderBy(r => r.Val).Take(3).Select(r => r.Id).ToHashSet();
        var affected = await ctx.Query<Row>().OrderBy(r => r.Val).Take(3).ExecuteDeleteAsync();
        Assert.Equal(3, affected);

        var survivors = (await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync()).Select(r => r.Id).ToList();
        var expected = rows.Where(r => !doomed.Contains(r.Id)).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(survivors),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", survivors)}]");
    }

    [Fact]
    public async Task ExecuteDelete_after_ordered_Skip_deletes_only_the_remainder()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var doomed = rows.OrderByDescending(r => r.Val).Skip(9).Select(r => r.Id).ToHashSet();
        var affected = await ctx.Query<Row>().OrderByDescending(r => r.Val).Skip(9).ExecuteDeleteAsync();
        Assert.Equal(3, affected);

        var survivors = (await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync()).Select(r => r.Id).ToList();
        var expected = rows.Where(r => !doomed.Contains(r.Id)).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(survivors),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", survivors)}]");
    }

    [Fact]
    public async Task ExecuteUpdate_after_filtered_ordered_Take_updates_only_the_window()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var targets = rows.Where(r => r.Id > 2).OrderByDescending(r => r.Val).Take(4).Select(r => r.Id).ToHashSet();
        var affected = await ctx.Query<Row>().Where(r => r.Id > 2).OrderByDescending(r => r.Val).Take(4)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Val, -1));
        Assert.Equal(4, affected);

        foreach (var (id, val) in ReadRaw(keeper))
        {
            var expectedVal = targets.Contains(id) ? -1 : rows.Single(o => o.Id == id).Val;
            Assert.True(expectedVal == val, $"Id {id}: expected Val {expectedVal} got {val}");
        }
    }

    [Fact]
    public async Task ExecuteDelete_after_Where_composed_on_a_window_deletes_the_filtered_window()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Where AFTER Take wraps the window as a derived table and the paging flag
        // is consumed into the subquery — the delete must still honor the window.
        var doomed = rows.OrderBy(r => r.Val).Take(6).Where(r => r.Id % 2 == 0).Select(r => r.Id).ToHashSet();
        var affected = await ctx.Query<Row>().OrderBy(r => r.Val).Take(6).Where(r => r.Id % 2 == 0).ExecuteDeleteAsync();
        Assert.Equal(doomed.Count, affected);

        var survivors = (await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync()).Select(r => r.Id).ToList();
        var expected = rows.Where(r => !doomed.Contains(r.Id)).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(survivors),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", survivors)}]");
    }

    [Fact]
    public async Task ExecuteDelete_after_TakeLast_deletes_only_the_tail()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var doomed = rows.OrderBy(r => r.Val).TakeLast(3).Select(r => r.Id).ToHashSet();
        var affected = await ctx.Query<Row>().OrderBy(r => r.Val).TakeLast(3).ExecuteDeleteAsync();
        Assert.Equal(3, affected);

        var survivors = (await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync()).Select(r => r.Id).ToList();
        var expected = rows.Where(r => !doomed.Contains(r.Id)).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(survivors),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", survivors)}]");
    }

    [Fact]
    public async Task ExecuteDelete_with_ordering_but_no_paging_matches_the_plain_filter()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // ORDER BY without paging does not change the row set; the delete must
        // accept the shape and behave exactly like the unordered filter.
        var affected = await ctx.Query<Row>().Where(r => r.Val >= 60).OrderBy(r => r.Val).ExecuteDeleteAsync();
        Assert.Equal(rows.Count(r => r.Val >= 60), affected);

        var survivors = (await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync()).Select(r => r.Id).ToList();
        var expected = rows.Where(r => r.Val < 60).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.True(expected.SequenceEqual(survivors),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", survivors)}]");
    }

    [Fact]
    public async Task ExecuteUpdate_after_Skip_with_closure_count_updates_the_remainder()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var skip = 8;
        var targets = rows.OrderBy(r => r.Val).Skip(skip).Select(r => r.Id).ToHashSet();
        var affected = await ctx.Query<Row>().OrderBy(r => r.Val).Skip(skip)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Val, r => r.Val + 1000));
        Assert.Equal(12 - skip, affected);

        foreach (var (id, val) in ReadRaw(keeper))
        {
            var orig = rows.Single(o => o.Id == id).Val;
            var expectedVal = targets.Contains(id) ? orig + 1000 : orig;
            Assert.True(expectedVal == val, $"Id {id}: expected Val {expectedVal} got {val}");
        }
    }

    [Fact]
    public async Task ExecuteDelete_with_Distinct_matches_the_plain_filter()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Distinct over entity rows is inert (rows are key-unique); the delete
        // must accept the shape and match the plain filter.
        var affected = await ctx.Query<Row>().Where(r => r.Id <= 4).Distinct().ExecuteDeleteAsync();
        Assert.Equal(4, affected);

        var survivors = (await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync()).Select(r => r.Id).ToList();
        Assert.True(Enumerable.Range(5, 8).SequenceEqual(survivors),
            $"got [{string.Join(",", survivors)}]");
    }
}
