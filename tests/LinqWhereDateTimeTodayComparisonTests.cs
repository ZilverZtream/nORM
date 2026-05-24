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
/// Sister of ca13e7a (DateTime.UtcNow pin) for the midnight-snapped
/// <c>DateTime.Today</c> variant. Common "all rows on / before today"
/// idiom. The translator must evaluate <c>DateTime.Today</c> client-
/// side at translate time and bind as a SQL parameter -- not emit a
/// SQL function like <c>DATE(CURRENT_TIMESTAMP)</c> that the DB
/// re-evaluates per row.
///
/// Silent-wrongness shapes:
///   * Per-row DB clock evaluation: timezone drift between app and DB
///     can flip the verdict on edge-of-day rows.
///   * Translator drops the .Today midnight snap and uses Now (with
///     time component): "all rows today" silently excludes rows seeded
///     earlier today with a smaller time-of-day.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeTodayComparisonTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Seed relative to the test process's notion of "today" so the
        // assertions don't drift across machines/timezones. Use local kind
        // because DateTime.Today returns DateTimeKind.Local.
        var today = DateTime.Today;
        await using (var insert = _cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO WdtItem (Id, Stamp) VALUES ($id, $stamp)";
            var idParam = insert.CreateParameter(); idParam.ParameterName = "$id"; insert.Parameters.Add(idParam);
            var stampParam = insert.CreateParameter(); stampParam.ParameterName = "$stamp"; insert.Parameters.Add(stampParam);
            var stamps = new (int id, DateTime stamp)[]
            {
                (1, today.AddDays(-7)),         // last week
                (2, today.AddDays(-1)),         // yesterday
                (3, today),                     // today at midnight
                (4, today.AddHours(12)),        // today at noon
                (5, today.AddDays(1)),          // tomorrow
                (6, today.AddDays(7)),          // next week
            };
            foreach (var (id, stamp) in stamps)
            {
                idParam.Value = id;
                stampParam.Value = stamp.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
                await insert.ExecuteNonQueryAsync();
            }
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_Stamp_less_than_DateTime_Today_returns_strictly_past_rows()
    {
        // DateTime.Today is today's midnight. `Stamp < Today` excludes
        // rows with Stamp == today midnight (Id 3 is exactly today). So
        // matches Id 1 (-7d) and Id 2 (-1d) only.
        // Silent-wrongness: if translator emits DateTime.Now instead of
        // DateTime.Today, Id 3 (which is today midnight, also < now) would
        // also match, giving {1, 2, 3}.
        var rows = await _ctx.Query<WdtItem>()
            .Where(i => i.Stamp < DateTime.Today)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Stamp_greater_than_or_equal_to_DateTime_Today_returns_today_and_future_rows()
    {
        // Stamp >= Today -> Id 3 (today midnight), Id 4 (today noon),
        // Id 5 (tomorrow), Id 6 (next week).
        var rows = await _ctx.Query<WdtItem>()
            .Where(i => i.Stamp >= DateTime.Today)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4, 5, 6 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Stamp_between_Today_and_Tomorrow_returns_only_todays_rows()
    {
        // Combined range probe: rows with Stamp in [Today, Tomorrow).
        // Should match Id 3 (today midnight) and Id 4 (today noon); exclude
        // Id 5 (tomorrow midnight, equality on upper exclusive bound) and
        // anything earlier/later.
        var tomorrow = DateTime.Today.AddDays(1);
        var rows = await _ctx.Query<WdtItem>()
            .Where(i => i.Stamp >= DateTime.Today && i.Stamp < tomorrow)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("WdtItem")]
    public sealed class WdtItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
