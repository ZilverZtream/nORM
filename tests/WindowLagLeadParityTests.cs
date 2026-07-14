using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// WithLag / WithLead parity against a LINQ-to-Objects oracle over a fully
/// ordered sequence: LAG(value, k) at row i is the value k rows earlier (or the
/// default/NULL when out of range), LEAD(value, k) is k rows later. Covers
/// offsets, the NULL default, and an explicit default value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class WindowLagLeadParityTests
{
    [Table("WllRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Ord { get; set; }
        public int Val { get; set; }
    }

    private static readonly (int Id, int Ord, int Val)[] Seed =
    {
        (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400), (5, 50, 500),
    };

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:wll_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE WllRow_Test (Id INTEGER PRIMARY KEY, Ord INTEGER NOT NULL, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, ord, val) in Seed)
            {
                cmd.CommandText = $"INSERT INTO WllRow_Test VALUES ({id}, {ord}, {val})";
                cmd.ExecuteNonQuery();
            }
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly List<Row> Oracle = Seed
        .Select(t => new Row { Id = t.Id, Ord = t.Ord, Val = t.Val }).ToList();

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public async Task Lag_matches_linq(int offset)
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var expected = Oracle.OrderBy(r => r.Ord)
            .Select((r, i) => (r.Id, Prev: i - offset >= 0 ? (int?)Oracle.OrderBy(x => x.Ord).ElementAt(i - offset).Val : null))
            .OrderBy(t => t.Id).ToList();

        var actual = (await ctx.Query<Row>().OrderBy(r => r.Ord)
                .WithLag(r => (int?)r.Val, offset, (r, prev) => new { r.Id, Prev = prev })
                .ToListAsync())
            .Select(x => (x.Id, x.Prev)).OrderBy(t => t.Id).ToList();

        Assert.True(expected.SequenceEqual(actual),
            $"offset={offset}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public async Task Lead_matches_linq(int offset)
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var ordered = Oracle.OrderBy(r => r.Ord).ToList();
        var expected = ordered
            .Select((r, i) => (r.Id, Next: i + offset < ordered.Count ? (int?)ordered[i + offset].Val : null))
            .OrderBy(t => t.Id).ToList();

        var actual = (await ctx.Query<Row>().OrderBy(r => r.Ord)
                .WithLead(r => (int?)r.Val, offset, (r, next) => new { r.Id, Next = next })
                .ToListAsync())
            .Select(x => (x.Id, x.Next)).OrderBy(t => t.Id).ToList();

        Assert.True(expected.SequenceEqual(actual),
            $"offset={offset}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Lag_with_explicit_default_fills_out_of_range()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var ordered = Oracle.OrderBy(r => r.Ord).ToList();
        var expected = ordered
            .Select((r, i) => (r.Id, Prev: i - 2 >= 0 ? ordered[i - 2].Val : -1))
            .OrderBy(t => t.Id).ToList();

        var actual = (await ctx.Query<Row>().OrderBy(r => r.Ord)
                .WithLag(r => r.Val, 2, (r, prev) => new { r.Id, Prev = prev }, r => -1)
                .ToListAsync())
            .Select(x => (x.Id, x.Prev)).OrderBy(t => t.Id).ToList();

        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }
}
