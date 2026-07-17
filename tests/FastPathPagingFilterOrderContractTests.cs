using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// Pins the LINQ semantics of paging composed with a filter in BOTH orders, against a
/// LINQ-to-Objects oracle. The failing shapes are Take(n).Where(p) and Skip(n).Where(p):
/// LINQ pages FIRST then filters, which needs a derived-table wrap — the fast path was
/// emitting flat "WHERE p LIMIT n" (wrong rows) or bare "OFFSET n" with no LIMIT (invalid
/// SQL on SQLite/MySQL). The filter-first shapes (Where(p).Take(n)) stay flat and correct.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FastPathPagingFilterOrderContractTests
{
    [Table("FppRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
    }

    private static async Task<(DbContext ctx, List<Row> oracle)> BootstrapAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FppRow (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var oracle = new List<Row>();
        for (var id = 1; id <= 10; id++)
        {
            var active = id % 2 == 0; // even ids active: 2,4,6,8,10
            using var cmd = cn.CreateCommand();
            cmd.CommandText = $"INSERT INTO FppRow VALUES ({id}, {(active ? 1 : 0)})";
            cmd.ExecuteNonQuery();
            oracle.Add(new Row { Id = id, Active = active });
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        await Task.CompletedTask;
        return (ctx, oracle);
    }

    [Fact]
    public async Task Take_then_where_pages_before_filtering()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, oracle) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        // LINQ: take first 4 (ids 1-4), then keep the even ones -> [2,4].
        var expected = oracle.Take(4).Where(r => r.Active).Select(r => r.Id).OrderBy(i => i).ToArray();
        var actual = (await ctx.Query<Row>().Take(4).Where(r => r.Active).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Skip_then_where_pages_before_filtering()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, oracle) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        // LINQ: skip first 3 (ids 1-3), then keep the even ones -> [4,6,8,10].
        var expected = oracle.Skip(3).Where(r => r.Active).Select(r => r.Id).OrderBy(i => i).ToArray();
        var actual = (await ctx.Query<Row>().Skip(3).Where(r => r.Active).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Filter_first_paging_shapes_stay_correct()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, oracle) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        // Where(p).Take(n): filter to even, take 2 -> [2,4].
        var takeExpected = oracle.Where(r => r.Active).Take(2).Select(r => r.Id).OrderBy(i => i).ToArray();
        var takeActual = (await ctx.Query<Row>().Where(r => r.Active).Take(2).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(takeExpected, takeActual);

        // Where(p).Skip(n): filter to even, skip 2 -> [6,8,10].
        var skipExpected = oracle.Where(r => r.Active).Skip(2).Select(r => r.Id).OrderBy(i => i).ToArray();
        var skipActual = (await ctx.Query<Row>().Where(r => r.Active).Skip(2).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(skipExpected, skipActual);

        // Where(p).OrderBy(k).Take(n): filter, order, page -> flat and correct.
        var orderedExpected = oracle.Where(r => r.Active).OrderBy(r => r.Id).Take(3).Select(r => r.Id).ToArray();
        var orderedActual = (await ctx.Query<Row>().Where(r => r.Active).OrderBy(r => r.Id).Take(3).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(orderedExpected, orderedActual);
    }
}
