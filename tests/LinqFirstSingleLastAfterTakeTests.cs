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
/// Pins <c>q.OrderBy(x).Take(N).First(pred)</c> / <c>.Single(pred)</c> /
/// <c>.Last(pred)</c>: the predicate must run only over the windowed first
/// N rows. Sister of <see cref="LinqAnyAllContainsAfterTakeTests"/>; same
/// invariant family.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqFirstSingleLastAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FslRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO FslRow VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 999);
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
    public async Task FirstOrDefault_after_take_returns_default_when_predicate_match_outside_window()
    {
        // First 3 rows V=10,20,30. None > 100. Row 5 has V=999 outside the window.
        var hit = await _ctx.Query<FslRow>().OrderBy(r => r.Id).Take(3).FirstOrDefaultAsync(r => r.V > 100);
        Assert.Null(hit);
        // Full-table would find V=999.
        var fullHit = await _ctx.Query<FslRow>().OrderBy(r => r.Id).FirstOrDefaultAsync(r => r.V > 100);
        Assert.NotNull(fullHit);
        Assert.Equal(5, fullHit!.Id);
    }

    [Fact]
    public async Task FirstAsync_after_take_returns_first_window_row_matching_predicate()
    {
        // First 3 rows V=10,20,30. > 15 → V=20 (Id=2).
        var hit = await _ctx.Query<FslRow>().OrderBy(r => r.Id).Take(3).FirstAsync(r => r.V > 15);
        Assert.Equal(2, hit.Id);
    }

    [Fact]
    public async Task SingleOrDefault_after_take_returns_default_when_predicate_match_outside_window()
    {
        var hit = await _ctx.Query<FslRow>().OrderBy(r => r.Id).Take(3).SingleOrDefaultAsync(r => r.V > 100);
        Assert.Null(hit);
    }

    [Table("FslRow")]
    public sealed class FslRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
