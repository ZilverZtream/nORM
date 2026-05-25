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
/// Probe pin for <c>GroupBy(p =&gt; p.DecimalCol)</c> on mixed-magnitude
/// values. SQL GROUP BY on TEXT columns groups by the raw text storage,
/// so '10.5' and '10.50' would land in separate groups. The numeric
/// grouping intent is preserved by CAST AS REAL on the key.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByDecimalKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GdkItem (Id INTEGER PRIMARY KEY, K TEXT NOT NULL);
            INSERT INTO GdkItem VALUES
                (1, '10.5'),
                (2, '10.5'),
                (3, '2.0'),
                (4, '2.0'),
                (5, '100.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GdkItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_decimal_key_returns_three_distinct_groups_with_correct_counts()
    {
        var groups = await _ctx.Query<GdkItem>()
            .GroupBy(p => p.K)
            .Select(g => new { Key = g.Key, Count = g.Count() })
            .OrderBy(g => g.Key)
            .ToListAsync();
        Assert.Equal(3, groups.Count);
        // Numeric order: 2.0, 10.5, 100.0
        Assert.Equal(2.0m, groups[0].Key);
        Assert.Equal(2, groups[0].Count);
        Assert.Equal(10.5m, groups[1].Key);
        Assert.Equal(2, groups[1].Count);
        Assert.Equal(100.0m, groups[2].Key);
        Assert.Equal(1, groups[2].Count);
    }

    [Fact]
    public async Task GroupBy_decimal_key_treats_numerically_equal_text_as_same_group()
    {
        // .NET: decimal.Equals(10.5m, 10.50m) == true (numerically equal even
        // though scale differs). But the TEXT column stores them as different
        // strings. Numeric grouping intent should lump them together.
        await using var c = _cn.CreateCommand();
        c.CommandText = """
            INSERT INTO GdkItem VALUES
                (6, '10.50'),
                (7, '10.500'),
                (8, '2.0000');
            """;
        await c.ExecuteNonQueryAsync();
        var groups = await _ctx.Query<GdkItem>()
            .GroupBy(p => p.K)
            .Select(g => new { Key = g.Key, Count = g.Count() })
            .OrderBy(g => g.Key)
            .ToListAsync();
        // Expected: 2.0 -> 3 rows (2.0, 2.0, 2.0000), 10.5 -> 4 (10.5, 10.5,
        // 10.50, 10.500), 100.0 -> 1.
        Assert.Equal(3, groups.Count);
        Assert.Equal(2.0m, groups[0].Key);
        Assert.Equal(3, groups[0].Count);
        Assert.Equal(10.5m, groups[1].Key);
        Assert.Equal(4, groups[1].Count);
        Assert.Equal(100.0m, groups[2].Key);
        Assert.Equal(1, groups[2].Count);
    }

    [Table("GdkItem")]
    public sealed class GdkItem
    {
        [Key] public int Id { get; set; }
        public decimal K { get; set; }
    }
}
