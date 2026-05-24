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
/// Pins the contract for <c>DateTime.UtcNow</c> / <c>DateTime.Now</c> used as
/// a comparison RHS in a Where predicate (very common shape: "records expiring
/// before now", "rows created in the last 24h").
///
/// The user expectation: the runtime clock value at translation time becomes
/// a SQL parameter (e.g. <c>@p0 = '2026-05-24 12:00:00'</c>), giving stable
/// row sets across the row scan. Silent-wrongness shapes:
///   * Translator emits <c>CURRENT_TIMESTAMP</c> -- evaluated per row by SQLite;
///     timing differences over a long scan can give inconsistent results, and
///     the value source moves from app clock to DB clock (different timezone
///     or skew can flip the verdict).
///   * Translator evaluates DateTime.UtcNow at COMPILE time and caches it
///     forever -- subsequent calls return rows relative to a stale "now".
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeNowComparisonTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdnItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Insert relative to the test process's current time so the assertion
        // doesn't depend on a fixed absolute timestamp.
        var now = DateTime.UtcNow;
        await using (var insert = _cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO WdnItem (Id, Stamp) VALUES ($id, $stamp)";
            var idParam = insert.CreateParameter(); idParam.ParameterName = "$id"; insert.Parameters.Add(idParam);
            var stampParam = insert.CreateParameter(); stampParam.ParameterName = "$stamp"; insert.Parameters.Add(stampParam);
            // 5 rows: 2 in the past, 1 right now, 2 in the future.
            var stamps = new (int id, DateTime stamp)[]
            {
                (1, now.AddDays(-30)),
                (2, now.AddHours(-1)),
                (3, now),
                (4, now.AddHours(1)),
                (5, now.AddDays(30)),
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
            OnModelCreating = mb => mb.Entity<WdnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_Stamp_less_than_UtcNow_returns_past_rows()
    {
        // "rows in the past" -- expect Id 1 (-30d) and Id 2 (-1h). Id 3 is "now"
        // and may or may not match depending on microsecond precision; we use
        // a comfortably-past comparison to avoid the edge case.
        var cutoff = DateTime.UtcNow.AddMinutes(-1);
        var rows = await _ctx.Query<WdnItem>()
            .Where(i => i.Stamp < cutoff)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Stamp_less_than_DateTime_UtcNow_directly_evaluates_at_translation()
    {
        // Same shape, but DateTime.UtcNow is INSIDE the lambda body. The user
        // expectation is the translator evaluates it client-side once at
        // translate time and binds as a SQL parameter -- not emit a SQL
        // CURRENT_TIMESTAMP function call that re-evaluates per row.
        var rows = await _ctx.Query<WdnItem>()
            .Where(i => i.Stamp < DateTime.UtcNow)
            .OrderBy(i => i.Id)
            .ToListAsync();
        // At translation time DateTime.UtcNow is approximately the test-process
        // current time, which is slightly after the seed time. So all past rows
        // (1, 2) plus the "now" row (3) -- which was inserted with the InitializeAsync's
        // earlier now -- should match. Id 4 and Id 5 are in the future, so excluded.
        Assert.Equal(new[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Stamp_greater_than_DateTime_UtcNow_returns_future_rows()
    {
        var rows = await _ctx.Query<WdnItem>()
            .Where(i => i.Stamp > DateTime.UtcNow)
            .OrderBy(i => i.Id)
            .ToListAsync();
        // Future rows: Id 4 (+1h), Id 5 (+30d). Id 3 ("now" at seed) is in the
        // past relative to UtcNow at query time so excluded.
        Assert.Equal(new[] { 4, 5 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("WdnItem")]
    public sealed class WdnItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
