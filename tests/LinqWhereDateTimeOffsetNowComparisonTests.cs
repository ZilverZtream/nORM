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
/// Sister of ca13e7a (DateTime.UtcNow pin) for the timezone-aware
/// <c>DateTimeOffset</c> variant. Apps with multi-region data store
/// timestamps with an offset and compare against
/// <c>DateTimeOffset.UtcNow</c>; the translator must evaluate the
/// captured clock client-side at translate time and bind as a SQL
/// parameter (same contract as plain DateTime), NOT lower to a
/// CURRENT_TIMESTAMP-style per-row function which would mix timezones.
///
/// Pinned via:
///   * captured-local cutoff comparison
///   * inline DateTimeOffset.UtcNow comparison
///   * empty-future probe (greater-than NowAddMinutes returns empty when
///     no future-stamped rows exist)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeOffsetNowComparisonTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtoItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var now = DateTimeOffset.UtcNow;
        await using (var insert = _cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO WdtoItem (Id, Stamp) VALUES ($id, $stamp)";
            var idParam = insert.CreateParameter(); idParam.ParameterName = "$id"; insert.Parameters.Add(idParam);
            var stampParam = insert.CreateParameter(); stampParam.ParameterName = "$stamp"; insert.Parameters.Add(stampParam);
            // 5 rows around -30d / -1h / now / +1h / +1d. The +/-1 hour
            // gap is wider than any clock drift between insert and query.
            var stamps = new (int id, DateTimeOffset stamp)[]
            {
                (1, now.AddDays(-30)),
                (2, now.AddHours(-1)),
                (3, now),
                (4, now.AddHours(1)),
                (5, now.AddDays(1)),
            };
            foreach (var (id, stamp) in stamps)
            {
                idParam.Value = id;
                stampParam.Value = stamp.ToString("yyyy-MM-dd HH:mm:ss.fffffffzzz");
                await insert.ExecuteNonQueryAsync();
            }
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtoItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_Stamp_less_than_captured_DateTimeOffset_returns_past_rows()
    {
        // Captured-local cutoff comfortably in the past (now -1 minute).
        // Past rows: Id 1 (-30d), Id 2 (-1h).
        var cutoff = DateTimeOffset.UtcNow.AddMinutes(-1);
        var result = await _ctx.Query<WdtoItem>()
            .Where(i => i.Stamp < cutoff)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Stamp_less_than_inline_DateTimeOffset_UtcNow_returns_past_rows()
    {
        // Inline DateTimeOffset.UtcNow. Past rows include Id 3 because the
        // InitializeAsync's "now" has already drifted into the past.
        var result = await _ctx.Query<WdtoItem>()
            .Where(i => i.Stamp < DateTimeOffset.UtcNow)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Stamp_greater_than_far_future_returns_empty_set()
    {
        var farFuture = DateTimeOffset.UtcNow.AddYears(1);
        var result = await _ctx.Query<WdtoItem>()
            .Where(i => i.Stamp > farFuture)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("WdtoItem")]
    public sealed class WdtoItem
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
