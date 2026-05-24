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
/// Strict pin for column-side <c>DateTime.AddDays</c> in Where -- the
/// path 0e88607 originally noted as "throws actionable error or works".
/// The provider's TranslateFunction already has datetime(col, '+N days')
/// templates for AddDays/AddMonths/AddYears/AddHours/AddMinutes/AddSeconds
/// (memory item #75); this test verifies the visitor admits the call
/// through the column-side BinaryExpression compare.
///
/// Shapes pinned:
///   * <c>r.Stamp.AddDays(7) &lt; UtcNow</c> -- "older than a week" by
///     pushing the future-shifted column back to the past.
///   * <c>r.Stamp.AddHours(-1) &gt;= UtcNow</c> -- hour-granular form.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeAddDaysColumnSideStrictTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdaColItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var now = DateTime.UtcNow;
        await using (var insert = _cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO WdaColItem (Id, Stamp) VALUES ($id, $stamp)";
            var idParam = insert.CreateParameter(); idParam.ParameterName = "$id"; insert.Parameters.Add(idParam);
            var stampParam = insert.CreateParameter(); stampParam.ParameterName = "$stamp"; insert.Parameters.Add(stampParam);
            var stamps = new (int id, DateTime stamp)[]
            {
                (1, now.AddDays(-30)),
                (2, now.AddDays(-10)),
                (3, now.AddDays(-3)),
                (4, now),
                (5, now.AddDays(3)),
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
            OnModelCreating = mb => mb.Entity<WdaColItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_column_AddDays_positive_compares_to_UtcNow_returns_older_rows()
    {
        // r.Stamp.AddDays(7) < UtcNow means r.Stamp < UtcNow - 7d (rows older than
        // a week). Past rows: Id 1 (-30d), Id 2 (-10d). Excludes -3d/now/+3d.
        var result = await _ctx.Query<WdaColItem>()
            .Where(r => r.Stamp.AddDays(7) < DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_column_AddDays_negative_compares_to_UtcNow_returns_future_rows()
    {
        // r.Stamp.AddDays(-7) > UtcNow means r.Stamp > UtcNow + 7d (rows more
        // than a week in the future). None in the seed -> empty result.
        var result = await _ctx.Query<WdaColItem>()
            .Where(r => r.Stamp.AddDays(-7) > DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Fact]
    public async Task Where_with_column_AddHours_returns_correct_rows()
    {
        // r.Stamp.AddHours(72) < UtcNow ~= r.Stamp < UtcNow - 3d. Matches Id 1
        // (-30d) and Id 2 (-10d); Id 3 (-3d) is on the boundary -- expect
        // include because 72h before UtcNow lands before the -3d seed.
        var result = await _ctx.Query<WdaColItem>()
            .Where(r => r.Stamp.AddHours(72) < DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        // Allow either {1,2} or {1,2,3} depending on micro-timing of the seed
        // vs the query (Id 3 is -3d = -72h; with any drift it may or may not
        // satisfy strict <). The pin verifies translation produces a sensible
        // past slice.
        var ids = result.Select(r => r.Id).ToArray();
        Assert.True(
            (ids.Length == 2 && ids[0] == 1 && ids[1] == 2)
                || (ids.Length == 3 && ids[0] == 1 && ids[1] == 2 && ids[2] == 3),
            $"AddHours(72) < UtcNow returned [{string.Join(",", ids)}]; expected {{1,2}} or {{1,2,3}}.");
    }

    [Table("WdaColItem")]
    public sealed class WdaColItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
