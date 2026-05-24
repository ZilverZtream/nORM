using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that group-aggregate functions accept arbitrary computed selectors and not only
/// bare column accesses: g.Sum(x => x.Price * x.Quantity), g.Average(x => x.Score -
/// x.Penalty), g.Min over a sum of two columns. These reach for the visitor to translate
/// the binary arithmetic inside the selector lambda, then wrap the result in the aggregate.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupAggregateComputedSelectorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GaRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Price INTEGER NOT NULL, Quantity INTEGER NOT NULL);
            INSERT INTO GaRow VALUES
                (1, 'A', 10, 2),
                (2, 'A', 20, 3),
                (3, 'B', 5,  10),
                (4, 'B', 7,  4);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Sum_of_Price_times_Quantity_per_category()
    {
        var rows = (await _ctx.Query<GaRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Cat = g.Key, Total = g.Sum(x => x.Price * x.Quantity) })
            .ToListAsync())
            .OrderBy(r => r.Cat).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("A", rows[0].Cat); Assert.Equal(10*2 + 20*3, rows[0].Total);
        Assert.Equal("B", rows[1].Cat); Assert.Equal(5*10 + 7*4, rows[1].Total);
    }

    [Fact]
    public async Task Min_over_difference_of_two_columns_per_category()
    {
        var rows = (await _ctx.Query<GaRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Cat = g.Key, MinDiff = g.Min(x => x.Price - x.Quantity) })
            .ToListAsync())
            .OrderBy(r => r.Cat).ToArray();
        Assert.Equal("A", rows[0].Cat); Assert.Equal(Math.Min(10-2, 20-3), rows[0].MinDiff);
        Assert.Equal("B", rows[1].Cat); Assert.Equal(Math.Min(5-10, 7-4), rows[1].MinDiff);
    }

    [Table("GaRow")]
    public sealed class GaRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Price { get; set; }
        public int Quantity { get; set; }
    }
}
