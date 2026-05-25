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
/// Pins <c>q.OrderBy(x).Take(N).Last(pred)</c> / <c>.LastOrDefault(pred)</c>:
/// the predicate must run only against the windowed first N rows. Sister of
/// the First/Single fix (commit e0f1397); Last has its own translator that
/// reverses the ORDER BY and applies the same single-row paging.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqLastAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LatRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO LatRow VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 999);
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
    public async Task LastOrDefault_after_take_returns_default_when_predicate_match_outside_window()
    {
        // First 3 rows V=10,20,30. None > 100. Row 5 has V=999 outside the window.
        var hit = await _ctx.Query<LatRow>().OrderBy(r => r.Id).Take(3).LastOrDefaultAsync(r => r.V > 100);
        Assert.Null(hit);
        // Full-table would find V=999.
        var fullHit = await _ctx.Query<LatRow>().OrderBy(r => r.Id).LastOrDefaultAsync(r => r.V > 100);
        Assert.NotNull(fullHit);
        Assert.Equal(5, fullHit!.Id);
    }

    [Fact]
    public async Task LastAsync_after_take_returns_last_window_row_matching_predicate()
    {
        // First 3 rows V=10,20,30. > 15 → V=20 (Id=2) and V=30 (Id=3). Last → Id=3.
        var hit = await _ctx.Query<LatRow>().OrderBy(r => r.Id).Take(3).LastAsync(r => r.V > 15);
        Assert.Equal(3, hit.Id);
    }

    [Table("LatRow")]
    public sealed class LatRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
