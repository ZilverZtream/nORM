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
/// Verifies that composite anonymous-type GroupBy keys (the `p => new { p.A, p.B }` shape) emit
/// a multi-column GROUP BY and that member access on the composite key (g.Key.A, g.Key.B)
/// resolves to the right column in projections.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompositeGroupByTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CkRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Region TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO CkRow VALUES
                (1, 'A', 'EU', 10),
                (2, 'A', 'EU', 20),
                (3, 'A', 'US', 30),
                (4, 'B', 'EU', 40),
                (5, 'B', 'US', 50),
                (6, 'B', 'US', 60);
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
    public async Task Composite_key_with_aggregates_in_projection_groups_by_both_columns()
    {
        var rows = (await _ctx.Query<CkRow>()
            .GroupBy(r => new { r.Category, r.Region })
            .Select(g => new { Cat = g.Key.Category, Reg = g.Key.Region, Total = g.Sum(x => x.Amount) })
            .ToListAsync())
            .OrderBy(r => r.Cat).ThenBy(r => r.Reg).ToArray();

        // 4 groups: (A,EU), (A,US), (B,EU), (B,US)
        Assert.Equal(4, rows.Length);
        Assert.Equal("A", rows[0].Cat); Assert.Equal("EU", rows[0].Reg); Assert.Equal(30, rows[0].Total);
        Assert.Equal("A", rows[1].Cat); Assert.Equal("US", rows[1].Reg); Assert.Equal(30, rows[1].Total);
        Assert.Equal("B", rows[2].Cat); Assert.Equal("EU", rows[2].Reg); Assert.Equal(40, rows[2].Total);
        Assert.Equal("B", rows[3].Cat); Assert.Equal("US", rows[3].Reg); Assert.Equal(110, rows[3].Total);
    }

    [Fact]
    public async Task Composite_key_with_aggregate_only_projection_returns_counts_per_combination()
    {
        var counts = (await _ctx.Query<CkRow>()
            .GroupBy(r => new { r.Category, r.Region })
            .Select(g => new { Count = g.Count() })
            .ToListAsync())
            .Select(r => r.Count).OrderBy(c => c).ToArray();
        // Four groups: counts are 1 (B,EU), 1 (A,US) ... wait: (A,EU)=2, (A,US)=1, (B,EU)=1, (B,US)=2.
        Assert.Equal(new[] { 1, 1, 2, 2 }, counts);
    }

    [Table("CkRow")]
    public sealed class CkRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
