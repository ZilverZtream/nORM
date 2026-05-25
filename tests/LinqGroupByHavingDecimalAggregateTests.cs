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
/// Probe pin for GroupBy + HAVING with a decimal-aggregate threshold:
///   `.GroupBy(p => p.Cat).Where(g => g.Sum(x => x.V) &gt; 50m)`
/// The aggregate side now coerces (sister to the GroupBy result-selector
/// fix), but the HAVING predicate compares the aggregate to a decimal
/// constant -- verify the constant side and the comparison operator
/// preserve numeric semantics.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByHavingDecimalAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GhdaItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL, Cat TEXT NOT NULL);
            INSERT INTO GhdaItem VALUES
                (1, '10.5', 'a'),
                (2, '2.0',  'a'),
                (3, '100.0','a'),
                (4, '1.5',  'b'),
                (5, '20.0', 'b'),
                (6, '5.0',  'c');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GhdaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_Cat_Having_Sum_decimal_greater_than_threshold_filters_numerically()
    {
        // Sums: a=112.5, b=21.5, c=5.0. Threshold 50 -> only 'a'.
        var groups = await _ctx.Query<GhdaItem>()
            .GroupBy(p => p.Cat)
            .Where(g => g.Sum(x => x.V) > 50m)
            .Select(g => new { Cat = g.Key, Total = g.Sum(x => x.V) })
            .OrderBy(g => g.Cat)
            .ToListAsync();
        Assert.Single(groups);
        Assert.Equal("a", groups[0].Cat);
        Assert.Equal(112.5m, groups[0].Total, precision: 9);
    }

    [Table("GhdaItem")]
    public sealed class GhdaItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
        public string Cat { get; set; } = string.Empty;
    }
}
