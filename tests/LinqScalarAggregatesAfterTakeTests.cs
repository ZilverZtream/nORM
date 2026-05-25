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
/// Pins <c>q.OrderBy(Id).Take(N).Count(pred)</c>, <c>.Sum(...)</c>,
/// <c>.Average(...)</c>, <c>.Min(...)</c>, <c>.Max(...)</c>: scalar
/// aggregates over a windowed source must reduce only the windowed rows.
/// Sister of the post-Take/Skip silent-wrongness family.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqScalarAggregatesAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SatRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO SatRow VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 999);
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
    public async Task Count_after_take_counts_only_the_windowed_rows()
    {
        // OrderBy(Id).Take(3) → rows 1,2,3. Count → 3. Full table = 5.
        var n = await _ctx.Query<SatRow>().OrderBy(r => r.Id).Take(3).CountAsync();
        Assert.Equal(3, n);
    }

    [Fact]
    public async Task Count_with_predicate_after_take_counts_only_within_window()
    {
        // First 3 rows V=10,20,30 — all <= 100 → count=3. None > 100. Full table includes V=999.
        Assert.Equal(3, await _ctx.Query<SatRow>().OrderBy(r => r.Id).Take(3).CountAsync(r => r.V <= 100));
        Assert.Equal(0, await _ctx.Query<SatRow>().OrderBy(r => r.Id).Take(3).CountAsync(r => r.V > 100));
    }

    [Fact]
    public async Task Sum_after_take_sums_only_the_windowed_rows()
    {
        // First 3 rows V=10,20,30 → Sum=60. Full table Sum = 10+20+30+40+999 = 1099.
        var s = await _ctx.Query<SatRow>().OrderBy(r => r.Id).Take(3).SumAsync(r => r.V);
        Assert.Equal(60, s);
    }

    [Fact]
    public async Task Average_after_take_averages_only_the_windowed_rows()
    {
        // First 3 rows V=10,20,30 → Avg=20. Full table Avg = 1099/5 = 219.8.
        var a = await _ctx.Query<SatRow>().OrderBy(r => r.Id).Take(3).AverageAsync(r => (double)r.V);
        Assert.Equal(20.0, a);
    }

    [Fact]
    public async Task Min_after_take_returns_min_of_windowed_rows()
    {
        // First 3 rows V=10,20,30 → Min=10. (Same as full-table min in this case but the contract holds.)
        // Skip(2).Take(3) → rows 3,4,5 V=30,40,999 → Min=30. Full Min=10.
        var m = await _ctx.Query<SatRow>().OrderBy(r => r.Id).Skip(2).Take(3).MinAsync(r => r.V);
        Assert.Equal(30, m);
    }

    [Fact]
    public async Task Max_after_take_returns_max_of_windowed_rows()
    {
        // First 3 rows V=10,20,30 → Max=30. Full Max=999.
        var m = await _ctx.Query<SatRow>().OrderBy(r => r.Id).Take(3).MaxAsync(r => r.V);
        Assert.Equal(30, m);
    }

    [Table("SatRow")]
    public sealed class SatRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
