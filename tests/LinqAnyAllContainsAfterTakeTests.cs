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
/// Pins <c>q.OrderBy(x).Take(N).Any(pred)</c>, <c>.All(pred)</c>, and
/// <c>.Contains(value)</c>: the predicate must run only against the windowed
/// first N rows, not the full table. nORM previously threw with a misleading
/// workaround pin claiming the Take was equivalent to no Take; in fact when
/// matching rows appear AFTER the window the windowed Any returns false where
/// the bare Any returns true.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAnyAllContainsAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AatRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO AatRow VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 999);
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
    public async Task Any_after_take_returns_false_when_predicate_match_is_outside_window()
    {
        // First 3 rows by Id are V=10,20,30. None > 100.
        Assert.False(await _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(3).AnyAsync(r => r.V > 100));
        // Full table contains V=999 → bare Any would return true. The Take must isolate.
        Assert.True(await _ctx.Query<AatRow>().OrderBy(r => r.Id).AnyAsync(r => r.V > 100));
    }

    [Fact]
    public async Task Any_after_take_returns_true_when_predicate_match_is_inside_window()
    {
        // First 3 rows V=10,20,30. 20 > 15 matches.
        Assert.True(await _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(3).AnyAsync(r => r.V > 15));
    }

    [Fact]
    public async Task All_after_take_evaluates_only_windowed_rows()
    {
        // First 3 rows V=10,20,30. All < 100 → true.
        Assert.True(await _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(3).AllAsync(r => r.V < 100));
        // Full table contains V=999 → bare All would return false.
        Assert.False(await _ctx.Query<AatRow>().OrderBy(r => r.Id).AllAsync(r => r.V < 100));
    }

    [Fact]
    public async Task Contains_after_take_finds_value_only_in_window()
    {
        // V=999 is in row 5, outside the first-3 window.
        var top3 = _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(3).Select(r => r.V);
        Assert.False(await top3.ContainsAsync(999));
        Assert.True(await top3.ContainsAsync(20));
    }

    [Table("AatRow")]
    public sealed class AatRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
