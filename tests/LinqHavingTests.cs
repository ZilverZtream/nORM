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
/// Verifies that GroupBy followed by Where on an aggregate produces a HAVING clause server-side
/// and returns the right groups. The Where translator detects the IGrouping source and routes
/// the predicate into _having instead of _where.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqHavingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE HvRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO HvRow VALUES
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
    public async Task GroupBy_Where_on_Count_returns_groups_above_threshold()
    {
        var keys = (await _ctx.Query<HvRow>()
            .GroupBy(r => r.Category)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToListAsync())
            .OrderBy(k => k).ToArray();
        // A has 2 rows, B has 2 rows, C has 1 row → A and B pass.
        Assert.Equal(new[] { "A", "B" }, keys);
    }

    [Fact]
    public async Task GroupBy_Where_on_Sum_returns_groups_with_total_above_threshold()
    {
        var rows = (await _ctx.Query<HvRow>()
            .GroupBy(r => r.Category)
            .Where(g => g.Sum(x => x.Amount) >= 50)
            .Select(g => new { Cat = g.Key, Total = g.Sum(x => x.Amount) })
            .ToListAsync())
            .OrderBy(r => r.Cat).ToArray();
        // Sums: A=30, B=70, C=50. >=50: B and C.
        Assert.Equal(2, rows.Length);
        Assert.Equal("B", rows[0].Cat); Assert.Equal(70, rows[0].Total);
        Assert.Equal("C", rows[1].Cat); Assert.Equal(50, rows[1].Total);
    }

    [Table("HvRow")]
    public sealed class HvRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
