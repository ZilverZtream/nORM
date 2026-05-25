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
/// Probe pin for <c>g.Min(x =&gt; x.V) / g.Max(...)</c> inside the
/// GroupBy result-selector aggregate path. Sister to the direct
/// aggregate fix in 2002200 and the nav-aggregate fix in 1e8725d --
/// covers the third site that emits MIN/MAX/SUM/AVG over decimal.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByDecimalAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GdaItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL, Cat TEXT NOT NULL);
            INSERT INTO GdaItem VALUES
                (1, '10.5', 'a'),
                (2, '2.0', 'a'),
                (3, '100.0', 'a'),
                (4, '1.5', 'b'),
                (5, '20.0', 'b');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GdaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_Cat_then_Max_decimal_returns_numerically_max_per_group()
    {
        var groups = await _ctx.Query<GdaItem>()
            .GroupBy(p => p.Cat)
            .Select(g => new { Cat = g.Key, MaxV = g.Max(x => x.V) })
            .OrderBy(g => g.Cat)
            .ToListAsync();
        Assert.Equal(2, groups.Count);
        Assert.Equal("a", groups[0].Cat);
        Assert.Equal(100.0m, groups[0].MaxV);  // a: 10.5, 2.0, 100.0
        Assert.Equal("b", groups[1].Cat);
        Assert.Equal(20.0m, groups[1].MaxV);   // b: 1.5, 20.0
    }

    [Table("GdaItem")]
    public sealed class GdaItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
        public string Cat { get; set; } = string.Empty;
    }
}
