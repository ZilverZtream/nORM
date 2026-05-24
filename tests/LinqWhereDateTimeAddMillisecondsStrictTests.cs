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
/// Strict pin for column-side <c>DateTime.AddMilliseconds</c> in projection
/// AND Where. SqliteProvider gained AddSeconds/AddDays/AddMonths/AddYears
/// per memory item #75 (column-side datetime modifiers), but
/// <c>AddMilliseconds</c> was missing -- silently throwing for users who
/// need sub-second precision shifts.
///
/// SQLite supports fractional-second modifiers ('+0.5 seconds'); the
/// emission must scale int millis to fractional seconds AND preserve the
/// fractional output, since the default datetime() drops sub-second. Use
/// strftime('%Y-%m-%d %H:%M:%f', ...) for 'YYYY-MM-DD HH:MM:SS.SSS'.
///
/// Format-matching for Where round-trip: Microsoft.Data.Sqlite serializes
/// DateTime params via 'yyyy-MM-dd HH:mm:ss.FFFFFFF' which trims trailing
/// zeros (and the literal '.' when no fractional remains). The column-side
/// strftime emits a fixed 3-digit fractional, so trim trailing zeros (and
/// the dot) with RTRIM to match -- otherwise '.500' lexically != '.5'.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeAddMillisecondsStrictTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdmsItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Stamps separated by ~250ms so additive shifts produce visibly
        // distinct ordering and threshold results.
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, 0, DateTimeKind.Utc);
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, anchor.AddMilliseconds(0)),
            (2, anchor.AddMilliseconds(250)),
            (3, anchor.AddMilliseconds(500)),
            (4, anchor.AddMilliseconds(750)),
            (5, anchor.AddMilliseconds(1000)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WdmsItem (Id, Stamp) VALUES ($id, $s)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        foreach (var (id, stamp) in stamps)
        {
            pid.Value = id;
            ps.Value = stamp.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdmsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_column_AddMilliseconds_positive_threshold_filters_rows()
    {
        // Stamp.AddMilliseconds(500) > anchor + 1000ms -> rows where shifted
        // stamp is > anchor + 1000ms -> Stamp > anchor + 500ms -> {Id 4, 5}.
        var cutoff = new DateTime(2026, 5, 24, 12, 0, 1, 0, DateTimeKind.Utc); // anchor + 1000ms
        var result = await _ctx.Query<WdmsItem>()
            .Where(i => i.Stamp.AddMilliseconds(500) > cutoff)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_column_AddMilliseconds_negative_threshold_filters_rows()
    {
        // Stamp.AddMilliseconds(-250) >= anchor -> Stamp >= anchor + 250ms ->
        // {Id 2, 3, 4, 5}. Silent-wrongness: sign drop -> {3,4,5} or {}.
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, 0, DateTimeKind.Utc);
        var result = await _ctx.Query<WdmsItem>()
            .Where(i => i.Stamp.AddMilliseconds(-250) >= anchor)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Select_column_AddMilliseconds_projects_shifted_DateTime_per_row()
    {
        // Project shifted DateTime; each row's Stamp + 100ms should round-trip
        // through the materializer to the expected DateTime value.
        var result = await _ctx.Query<WdmsItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, S = i.Stamp.AddMilliseconds(100) })
            .ToListAsync();
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(5, result.Count);
        Assert.Equal(anchor.AddMilliseconds(100), result[0].S);
        Assert.Equal(anchor.AddMilliseconds(350), result[1].S);
        Assert.Equal(anchor.AddMilliseconds(600), result[2].S);
        Assert.Equal(anchor.AddMilliseconds(850), result[3].S);
        Assert.Equal(anchor.AddMilliseconds(1100), result[4].S);
    }

    [Table("WdmsItem")]
    public sealed class WdmsItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
