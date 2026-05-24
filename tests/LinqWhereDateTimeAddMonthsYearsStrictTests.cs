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
/// Strict pin for column-side <c>DateTime.AddMonths</c> and
/// <c>DateTime.AddYears</c> in Where -- symmetric to 0e88607's AddDays
/// pin. SqliteProvider has datetime(col, '+N months') and 'N years'
/// templates per memory item #75; this pin locks in that both work in
/// Where comparisons (not just projection/scalar paths).
///
/// Silent-wrongness shapes:
///   * AddMonths drops to AddDays silently -- "older than a year" becomes
///     "older than 12 days" -> dramatically different result set.
///   * AddYears/AddMonths sign handling regression (see memory item #75
///     for the '+-N' bug).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeAddMonthsYearsStrictTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdayItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var now = DateTime.UtcNow;
        await using (var insert = _cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO WdayItem (Id, Stamp) VALUES ($id, $stamp)";
            var idParam = insert.CreateParameter(); idParam.ParameterName = "$id"; insert.Parameters.Add(idParam);
            var stampParam = insert.CreateParameter(); stampParam.ParameterName = "$stamp"; insert.Parameters.Add(stampParam);
            // Span from ~3 years ago to a few months in the future so AddYears
            // and AddMonths predicates produce distinct row sets.
            var stamps = new (int id, DateTime stamp)[]
            {
                (1, now.AddYears(-3)),       // 3 years ago
                (2, now.AddMonths(-18)),     // ~1.5 years ago
                (3, now.AddMonths(-6)),      // 6 months ago
                (4, now.AddMonths(-2)),      // 2 months ago
                (5, now.AddMonths(2)),       // 2 months in the future
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
            OnModelCreating = mb => mb.Entity<WdayItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_column_AddYears_positive_compares_to_UtcNow_returns_older_than_year_rows()
    {
        // r.Stamp.AddYears(1) < UtcNow -> rows where Stamp + 1y is in the past
        // -> Stamp older than 1 year ago. Expected: Id 1 (-3y), Id 2 (-1.5y).
        var result = await _ctx.Query<WdayItem>()
            .Where(r => r.Stamp.AddYears(1) < DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_column_AddMonths_positive_compares_to_UtcNow_returns_older_rows()
    {
        // r.Stamp.AddMonths(3) < UtcNow -> rows where Stamp + 3mo is in the
        // past -> Stamp older than 3 months ago. Expected: Id 1, 2, 3.
        var result = await _ctx.Query<WdayItem>()
            .Where(r => r.Stamp.AddMonths(3) < DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_column_AddMonths_negative_compares_to_UtcNow_returns_future_rows()
    {
        // r.Stamp.AddMonths(-6) > UtcNow -> Stamp > UtcNow + 6mo. Id 5 is
        // only +2mo, so nothing matches.
        var result = await _ctx.Query<WdayItem>()
            .Where(r => r.Stamp.AddMonths(-6) > DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Fact]
    public async Task Where_with_column_AddYears_negative_compares_to_UtcNow_minus_one_year()
    {
        // r.Stamp.AddYears(-1) < UtcNow.AddYears(-2) -> Stamp < UtcNow.AddYears(-1)
        // by transitivity (after moving the column shift to constant side).
        // Strict: matches Id 1 (-3y, well past -1y back) but excludes Id 2
        // (-1.5y is more recent than UtcNow.AddYears(-2) since the assertion
        // shifts the column LEFT 1y -- so Stamp - 1y < UtcNow - 2y means
        // Stamp < UtcNow - 1y; both Id 1 and Id 2 qualify).
        var cutoffMinus2 = DateTime.UtcNow.AddYears(-2);
        var result = await _ctx.Query<WdayItem>()
            .Where(r => r.Stamp.AddYears(-1) < cutoffMinus2)
            .OrderBy(r => r.Id)
            .ToListAsync();
        // Id 1: -3y - 1y = -4y < -2y ✓
        // Id 2: -1.5y - 1y = -2.5y < -2y ✓
        // Id 3: -6mo - 1y = -1.5y > -2y ✗
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WdayItem")]
    public sealed class WdayItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
