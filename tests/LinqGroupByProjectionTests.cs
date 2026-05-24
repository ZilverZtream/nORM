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
/// Exercises projections that follow a GroupBy: returning the key, returning the count, and
/// projecting into an anonymous record that mixes key and aggregate. These are the shapes EF
/// applications reach for most often and they previously leaked ArgumentNullException because
/// the IGrouping parameter had no associated table mapping.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GpRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO GpRow VALUES
                (1, 'A', 10),
                (2, 'A', 20),
                (3, 'B', 30),
                (4, 'B', 40),
                (5, 'C', 50);
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
    public async Task GroupBy_then_Select_key_returns_distinct_categories()
    {
        var keys = (await _ctx.Query<GpRow>()
            .GroupBy(r => r.Category)
            .Select(g => g.Key)
            .ToListAsync())
            .OrderBy(k => k).ToArray();
        Assert.Equal(new[] { "A", "B", "C" }, keys);
    }

    [Fact]
    public async Task GroupBy_then_Select_count_returns_row_counts_per_key()
    {
        var rows = (await _ctx.Query<GpRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Key = g.Key, Cnt = g.Count() })
            .ToListAsync())
            .OrderBy(r => r.Key).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("A", rows[0].Key); Assert.Equal(2, rows[0].Cnt);
        Assert.Equal("B", rows[1].Key); Assert.Equal(2, rows[1].Cnt);
        Assert.Equal("C", rows[2].Key); Assert.Equal(1, rows[2].Cnt);
    }

    [Fact]
    public async Task GroupBy_then_Select_sum_returns_total_per_key()
    {
        var rows = (await _ctx.Query<GpRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Key = g.Key, Total = g.Sum(x => x.Amount) })
            .ToListAsync())
            .OrderBy(r => r.Key).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(30, rows[0].Total); // A: 10+20
        Assert.Equal(70, rows[1].Total); // B: 30+40
        Assert.Equal(50, rows[2].Total); // C: 50
    }

    [Table("GpRow")]
    public sealed class GpRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
