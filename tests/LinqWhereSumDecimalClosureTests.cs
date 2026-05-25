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
/// Probe pin for the Where(closure)+Sum(decimal) end-to-end shape:
///   `q.Where(p => p.Cat == catVar).SumAsync(p => p.V)`
/// Verifies (a) the closure-captured filter binds correctly, (b) the
/// SUM aggregate over decimal still numeric-coerces (post 2002200),
/// and (c) the InvariantCulture scalar parse returns the expected
/// decimal under any locale.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereSumDecimalClosureTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsdcItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL, Cat TEXT NOT NULL);
            INSERT INTO WsdcItem VALUES
                (1, '10.5', 'a'),
                (2, '2.0', 'a'),
                (3, '100.0', 'b'),
                (4, '1.5', 'a'),
                (5, '50.25', 'b');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WsdcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_closure_category_then_SumAsync_decimal_returns_filtered_total()
    {
        var category = "a";
        var sum = await _ctx.Query<WsdcItem>()
            .Where(p => p.Cat == category)
            .SumAsync(p => p.V);
        // 'a' group: 10.5 + 2.0 + 1.5 = 14.0
        Assert.Equal(14.0m, sum, precision: 9);
    }

    [Fact]
    public async Task Where_closure_with_decimal_threshold_then_SumAsync_filters_numerically()
    {
        var threshold = 5m;
        var sum = await _ctx.Query<WsdcItem>()
            .Where(p => p.V > threshold)
            .SumAsync(p => p.V);
        // V > 5: 10.5 + 100.0 + 50.25 = 160.75
        Assert.Equal(160.75m, sum, precision: 9);
    }

    [Table("WsdcItem")]
    public sealed class WsdcItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
        public string Cat { get; set; } = string.Empty;
    }
}
