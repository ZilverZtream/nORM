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
/// Exercises the common reporting shape: a single anonymous-type projection that contains the
/// group key plus several aggregates (Min, Max, Avg, Sum, Count) — and aggregates whose
/// selector is itself a conditional expression so the SQL has to wrap a CASE inside the
/// aggregate function.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupMultiAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GmRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Score INTEGER NOT NULL, Active INTEGER NOT NULL);
            INSERT INTO GmRow VALUES
                (1, 'A', 10, 1),
                (2, 'A', 30, 0),
                (3, 'A', 20, 1),
                (4, 'B', 50, 1),
                (5, 'B', 40, 1);
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
    public async Task GroupBy_with_Min_Max_Sum_Count_in_single_projection()
    {
        var rows = (await _ctx.Query<GmRow>()
            .GroupBy(r => r.Category)
            .Select(g => new
            {
                Cat = g.Key,
                MinScore = g.Min(x => x.Score),
                MaxScore = g.Max(x => x.Score),
                TotalScore = g.Sum(x => x.Score),
                Count = g.Count()
            })
            .ToListAsync())
            .OrderBy(r => r.Cat).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("A", rows[0].Cat);
        Assert.Equal(10, rows[0].MinScore);
        Assert.Equal(30, rows[0].MaxScore);
        Assert.Equal(60, rows[0].TotalScore);
        Assert.Equal(3, rows[0].Count);
        Assert.Equal("B", rows[1].Cat);
        Assert.Equal(40, rows[1].MinScore);
        Assert.Equal(50, rows[1].MaxScore);
        Assert.Equal(90, rows[1].TotalScore);
        Assert.Equal(2, rows[1].Count);
    }

    [Fact]
    public async Task GroupBy_Sum_over_conditional_selector_filters_within_aggregate()
    {
        // Sum of Score for active rows only: A: 10 + 20 = 30 (id=2 has Active=0), B: 50+40=90.
        var rows = (await _ctx.Query<GmRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Cat = g.Key, ActiveSum = g.Sum(x => x.Active == 1 ? x.Score : 0) })
            .ToListAsync())
            .OrderBy(r => r.Cat).ToArray();
        Assert.Equal("A", rows[0].Cat); Assert.Equal(30, rows[0].ActiveSum);
        Assert.Equal("B", rows[1].Cat); Assert.Equal(90, rows[1].ActiveSum);
    }

    [Table("GmRow")]
    public sealed class GmRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Score { get; set; }
        public int Active { get; set; }
    }
}
