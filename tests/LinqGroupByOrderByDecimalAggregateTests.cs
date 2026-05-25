using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probe pin for `GroupBy(k).Select(g =&gt; new {k=g.Key, total=g.Sum(x =&gt; x.V)})
/// .OrderBy(r =&gt; r.total)` over a decimal column. Sister of the HAVING
/// decimal-aggregate probe (7bcb621). OrderBy on a result of a GroupBy-
/// projection aggregate goes through OrderByTranslator's `g.Sum(...)`
/// detection branch (line 458+) -- verify the decimal CAST AS REAL
/// coercion (fb8f52f, EmitGroupAggregateWithOptionalFilter) flows
/// through.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByOrderByDecimalAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GbodaItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL, Cat TEXT NOT NULL);
            INSERT INTO GbodaItem VALUES
                (1, '10.5',  'a'),
                (2, '2.0',   'a'),
                (3, '100.0', 'a'),
                (4, '1.5',   'b'),
                (5, '20.0',  'b'),
                (6, '5.0',   'c');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GbodaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_then_OrderBy_decimal_Sum_aggregate_orders_numerically()
    {
        // Sums: a=112.5, b=21.5, c=5.0. Ascending order: c, b, a.
        var groups = await _ctx.Query<GbodaItem>()
            .GroupBy(p => p.Cat)
            .Select(g => new { Cat = g.Key, Total = g.Sum(x => x.V) })
            .OrderBy(r => r.Total)
            .ToListAsync();
        Assert.Equal(3, groups.Count);
        Assert.Equal("c", groups[0].Cat);  // 5.0
        Assert.Equal("b", groups[1].Cat);  // 21.5
        Assert.Equal("a", groups[2].Cat);  // 112.5
    }

    [Table("GbodaItem")]
    public sealed class GbodaItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
        public string Cat { get; set; } = string.Empty;
    }
}
