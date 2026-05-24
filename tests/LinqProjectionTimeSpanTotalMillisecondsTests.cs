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
/// Strict pin for column-side <c>TimeSpan.TotalMilliseconds</c>. Sister to
/// 413248d's TotalSeconds/TotalMinutes/TotalHours work, but the ms total
/// requires reading the fractional-seconds component of the canonical
/// 'HH:mm:ss[.fffffff]' Microsoft.Data.Sqlite binding -- TimeSpan
/// .ToString('c') always emits 7-digit fractional ticks when nonzero
/// (it does NOT trim trailing zeros the way DateTime FFFFFFF does).
///
/// Silent-wrongness shapes:
///   * Fractional ignored -> 1500ms span returns 1000 (TotalSeconds * 1000).
///   * Scale wrong -> 7-digit ticks / 1e3 instead of / 1e4 returns 5,000,000
///     ms for what is really 1500ms.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanTotalMillisecondsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtmsItem (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Mix of fractional and whole-second TimeSpans to exercise both
        // length-9 (HH:mm:ss) and length-17 (HH:mm:ss.fffffff) storage shapes.
        var spans = new (int id, TimeSpan dur)[]
        {
            (1, new TimeSpan(0, 0, 1)),                      // 1000 ms
            (2, new TimeSpan(0, 0, 0, 1, 500)),              // 1500 ms
            (3, new TimeSpan(0, 0, 0, 0, 250)),              // 250 ms
            (4, new TimeSpan(0, 1, 30)),                     // 90,000 ms
            (5, new TimeSpan(0, 0, 0, 0, 0, 750)),           // 0.75 ms (sub-ms ticks)
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PtmsItem (Id, Duration) VALUES ($id, $d)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var pd = insert.CreateParameter(); pd.ParameterName = "$d"; insert.Parameters.Add(pd);
        foreach (var (id, dur) in spans)
        {
            pid.Value = id;
            pd.Value = dur.ToString("c");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtmsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_column_TimeSpan_TotalMilliseconds_projects_total_ms_per_row()
    {
        var result = await _ctx.Query<PtmsItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, Ms = i.Duration.TotalMilliseconds })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(1000.0, result[0].Ms);
        Assert.Equal(1500.0, result[1].Ms);
        Assert.Equal(250.0, result[2].Ms);
        Assert.Equal(90000.0, result[3].Ms);
        Assert.Equal(0.75, result[4].Ms, 5);
    }

    [Fact]
    public async Task Where_with_column_TimeSpan_TotalMilliseconds_threshold_filters_rows()
    {
        // Duration.TotalMilliseconds >= 1500 -> {Id 2 (1500), Id 4 (90000)}.
        var result = await _ctx.Query<PtmsItem>()
            .Where(i => i.Duration.TotalMilliseconds >= 1500)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PtmsItem")]
    public sealed class PtmsItem
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
