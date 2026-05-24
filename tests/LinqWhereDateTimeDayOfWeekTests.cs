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
/// Probes <c>DateTime.DayOfWeek</c> (and related extracted-part comparisons
/// like <c>.Year</c>, <c>.Month</c>, <c>.Day</c>) used as Where-predicate
/// values against a database column.
///
/// LINQ idiom: <c>q.Where(r =&gt; r.Stamp.DayOfWeek == DayOfWeek.Saturday)</c>.
/// Translator options:
///   * Emit provider-specific date-part SQL (SQLite: <c>strftime('%w', col)
///     = '6'</c>) -- best.
///   * Throw <c>NormUnsupportedFeatureException</c> with actionable text --
///     acceptable; tells the user to fetch + filter client-side or compute
///     a stored date-part column.
///   * Silent client-eval / dropped predicate -- worst; either materializes
///     the entire table OR returns all-or-nothing.
///
/// Pins the throw-or-correct contract so a regression that silently drops
/// the predicate surfaces.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeDayOfWeekTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdwItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            -- Fixed-week dataset (the week of Mon 2026-01-05 .. Sun 2026-01-11),
            -- one row per weekday so the assertion is deterministic regardless
            -- of when the test runs.
            INSERT INTO WdwItem VALUES
                (1, '2026-01-05 12:00:00.0000000'),  -- Monday
                (2, '2026-01-06 12:00:00.0000000'),  -- Tuesday
                (3, '2026-01-07 12:00:00.0000000'),  -- Wednesday
                (4, '2026-01-08 12:00:00.0000000'),  -- Thursday
                (5, '2026-01-09 12:00:00.0000000'),  -- Friday
                (6, '2026-01-10 12:00:00.0000000'),  -- Saturday
                (7, '2026-01-11 12:00:00.0000000');  -- Sunday
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdwItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_DayOfWeek_equals_Saturday_either_translates_or_throws_actionable_error()
    {
        // Probe: keep rows where Stamp.DayOfWeek == Saturday. Expected: Id 6
        // (2026-01-10 is a Saturday).
        // Silent-wrongness shapes the probe catches:
        //   * dropped predicate -> all 7 rows
        //   * Sunday confused with Saturday -> Id 7
        //   * predicate dropped silently to no-op -> 0 rows
        System.Exception? ex = null;
        int[]? matched = null;
        try
        {
            var result = await _ctx.Query<WdwItem>()
                .Where(r => r.Stamp.DayOfWeek == DayOfWeek.Saturday)
                .OrderBy(r => r.Id)
                .ToListAsync();
            matched = result.Select(r => r.Id).ToArray();
        }
        catch (System.Exception caught)
        {
            ex = caught;
        }

        if (ex != null)
        {
            Assert.True(
                ex is NormException || ex is System.InvalidOperationException || ex is System.NotSupportedException,
                $"DateTime.DayOfWeek threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
            return;
        }

        Assert.Equal(new[] { 6 }, matched);
    }

    [Fact]
    public async Task Where_with_Year_equals_constant_translates_to_correct_row_set()
    {
        // Strict: DateTime.Year is a very common idiom; the translator must
        // emit strftime('%Y', col) or equivalent and return all 7 (every
        // seed row is in 2026).
        var result = await _ctx.Query<WdwItem>()
            .Where(r => r.Stamp.Year == 2026)
            .ToListAsync();
        Assert.Equal(7, result.Count);

        // Negative probe: a year that matches nothing must return empty,
        // not "all rows" (predicate dropped) and not throw.
        var none = await _ctx.Query<WdwItem>()
            .Where(r => r.Stamp.Year == 1999)
            .ToListAsync();
        Assert.Empty(none);
    }

    [Fact]
    public async Task Where_with_Month_equals_constant_translates_to_correct_row_set()
    {
        // Strict: all seeded rows are January (month=1) -> all 7.
        var result = await _ctx.Query<WdwItem>()
            .Where(r => r.Stamp.Month == 1)
            .ToListAsync();
        Assert.Equal(7, result.Count);

        // Negative probe: February returns empty.
        var none = await _ctx.Query<WdwItem>()
            .Where(r => r.Stamp.Month == 2)
            .ToListAsync();
        Assert.Empty(none);
    }

    [Table("WdwItem")]
    public sealed class WdwItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
