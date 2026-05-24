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
/// Strict pin for <c>DateTime.Date</c> truncation in projection AND Where.
/// SQLite has a date() function plus strftime; the analyzer must admit the
/// member and the provider must emit a string format that round-trips with
/// the DateTime serialization used by ParameterManager (per memory item #71,
/// stamps are written as 'yyyy-MM-dd HH:mm:ss.fffffff').
///
/// Silent-wrongness shapes this catches:
///   * Projection: <c>.Date</c> not admitted -> client-eval throw on a
///     trivially translatable member access. Symmetric to Year/Month/Day,
///     which ARE admitted in the analyzer.
///   * Where: <c>.Date</c> emits 'YYYY-MM-DD' but params serialize as
///     'yyyy-MM-dd HH:mm:ss.fffffff' -> text comparison fails for every
///     row, returning 0 results where the user expects all rows on the
///     given date.
///
/// Pin shape: covers 5 rows that share two distinct calendar dates and one
/// row at an unrelated date. Each test asserts a different distinct row set
/// so a silently no-op .Date would produce visibly wrong output.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeDateTruncationStrictTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private static readonly DateTime DayA = new(2026, 1, 15, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime DayB = new(2026, 2, 20, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime DayC = new(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc);

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtdItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // 2 rows on DayA (different times of day), 2 on DayB, 1 on DayC.
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, DayA.AddHours(2).AddMinutes(30)),
            (2, DayA.AddHours(23).AddMinutes(59)),
            (3, DayB.AddHours(5)),
            (4, DayB.AddHours(15).AddMinutes(45)),
            (5, DayC.AddHours(12)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO DtdItem (Id, Stamp) VALUES ($id, $s)";
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
            OnModelCreating = mb => mb.Entity<DtdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_column_Date_equals_constant_returns_only_matching_day()
    {
        // Stamp.Date == DayB -> {Id 3, 4}. Silent-wrongness: if Date emits
        // 'YYYY-MM-DD' (e.g. '2026-02-20') and the param is serialized as
        // '2026-02-20 00:00:00.0000000', text comparison never matches -> 0 rows.
        var result = await _ctx.Query<DtdItem>()
            .Where(i => i.Stamp.Date == DayB)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_column_Date_in_range_includes_both_calendar_days()
    {
        // Stamp.Date >= DayA AND Stamp.Date <= DayB -> {Id 1, 2, 3, 4} (excludes Id 5 on DayC).
        var result = await _ctx.Query<DtdItem>()
            .Where(i => i.Stamp.Date >= DayA && i.Stamp.Date <= DayB)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Select_column_Date_projects_truncated_per_row_value()
    {
        // Project Date alongside Id; each row's Stamp.Date should equal the
        // calendar-day midnight DateTime, not the row's actual time.
        var result = await _ctx.Query<DtdItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, D = i.Stamp.Date })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(DayA, result[0].D);
        Assert.Equal(DayA, result[1].D);
        Assert.Equal(DayB, result[2].D);
        Assert.Equal(DayB, result[3].D);
        Assert.Equal(DayC, result[4].D);
    }

    [Table("DtdItem")]
    public sealed class DtdItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
