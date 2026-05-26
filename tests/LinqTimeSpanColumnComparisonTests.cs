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
/// Pins cross-column TimeSpan ordering: <c>Where(x => x.Lo &lt; x.Hi)</c>.
/// SQLite stores TimeSpan as canonical 'c' TEXT ("d.hh:mm:ss.fffffff").
/// Direct column comparison uses lexicographic ordering which is wrong for
/// multi-day durations: "10.00:00:00" &lt; "9.23:59:59" lexicographically
/// but "10 days" &gt; "9 days 23 hours" mathematically.
/// The translator must use GetTimeSpanColumnSecondsSql for both sides on
/// providers that store TimeSpan as TEXT.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeSpanColumnComparisonTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    // Rows: (Id, Lo, Hi)
    // (1,  10 mins,     20 mins)     → Lo < Hi  ✓
    // (2,  20 mins,     10 mins)     → Lo > Hi  ✗
    // (3,  1 day,       2 days)      → Lo < Hi  ✓
    // (4,  10 days,     9 days 23h)  → Lo > Hi  ✗  (lexicographic: "10...." < "9...." would be WRONG)
    // (5,  1h,          1h)          → Lo == Hi   ✗ (not strictly less)

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // SQLite stores TimeSpan as TEXT (canonical "c" format via Microsoft.Data.Sqlite)
        cmd.CommandText = """
            CREATE TABLE TsCompRow (Id INTEGER PRIMARY KEY, Lo TEXT NOT NULL, Hi TEXT NOT NULL);
            INSERT INTO TsCompRow VALUES
              (1, '00:10:00',           '00:20:00'),
              (2, '00:20:00',           '00:10:00'),
              (3, '1.00:00:00',         '2.00:00:00'),
              (4, '10.00:00:00',        '9.23:00:00'),
              (5, '01:00:00',           '01:00:00');
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
    public async Task TimeSpan_column_less_than_column_returns_correct_rows()
    {
        var rows = (await _ctx.Query<TsCompRow>()
            .Where(r => r.Lo < r.Hi)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToArray();

        // Rows 1 (10min < 20min) and 3 (1day < 2days) qualify.
        // Row 4 must NOT qualify: 10 days > 9 days 23h despite "10." < "9." lexicographically.
        Assert.Equal(new[] { 1, 3 }, rows);
    }

    [Fact]
    public async Task TimeSpan_column_greater_than_column_returns_correct_rows()
    {
        var rows = (await _ctx.Query<TsCompRow>()
            .Where(r => r.Lo > r.Hi)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToArray();

        // Row 2 (20min > 10min) and row 4 (10 days > 9 days 23h).
        Assert.Equal(new[] { 2, 4 }, rows);
    }

    [Fact]
    public async Task TimeSpan_column_equal_column_returns_correct_rows()
    {
        var rows = (await _ctx.Query<TsCompRow>()
            .Where(r => r.Lo == r.Hi)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToArray();

        Assert.Equal(new[] { 5 }, rows);
    }

    [Fact]
    public async Task TimeSpan_column_less_than_or_equal_column_includes_equal_row()
    {
        var rows = (await _ctx.Query<TsCompRow>()
            .Where(r => r.Lo <= r.Hi)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToArray();

        Assert.Equal(new[] { 1, 3, 5 }, rows);
    }

    [Table("TsCompRow")]
    public sealed class TsCompRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Lo { get; set; }
        public TimeSpan Hi { get; set; }
    }
}
