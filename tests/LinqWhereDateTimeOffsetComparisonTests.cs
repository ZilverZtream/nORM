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
/// Strict pin + implement-first for <c>DateTimeOffset</c> column equality
/// and range comparisons in Where. SQLite has no native DTO type;
/// Microsoft.Data.Sqlite serializes via DateTimeOffset.ToString("o"). Pin
/// verifies the lex-text compare matches the .NET ordering for canonical
/// 'o'-format strings.
///
/// Silent-wrongness shapes:
///   * DTO stored as numeric (julianday) -> text compare returns 0 rows.
///   * Offset-aware vs naive: comparing a column with +01:00 offset to a
///     UTC literal should respect the absolute instant, not the local
///     wall-clock. SQLite's text compare on canonical 'o' format doesn't
///     normalize offsets -- documented limitation; this pin asserts the
///     literal text-equality behavior.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeOffsetComparisonTests : IAsyncLifetime
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
        var rows = new (int id, DateTimeOffset dto)[]
        {
            (1, new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero)),
            (2, new DateTimeOffset(2026, 2, 20, 12, 0, 0, TimeSpan.Zero)),
            (3, new DateTimeOffset(2026, 3, 25, 12, 0, 0, TimeSpan.Zero)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WdtoItem (Id, Stamp) VALUES ($id, $s)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        foreach (var (id, dto) in rows)
        {
            pid.Value = id;
            // Microsoft.Data.Sqlite serializes DateTimeOffset as
            // 'yyyy-MM-dd HH:mm:ss.FFFFFFFzzz' (space separator, trimmed
            // fractional). Match that on the seed so exact equality lex-text
            // compare works for round-trip.
            ps.Value = dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", System.Globalization.CultureInfo.InvariantCulture);
            await insert.ExecuteNonQueryAsync();
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
    public async Task Where_DateTimeOffset_equals_constant_returns_single_row()
    {
        var target = new DateTimeOffset(2026, 2, 20, 12, 0, 0, TimeSpan.Zero);
        var result = await _ctx.Query<WdtoItem>()
            .Where(p => p.Stamp == target)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_DateTimeOffset_greater_than_filters_later_rows()
    {
        var cutoff = new DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var result = await _ctx.Query<WdtoItem>()
            .Where(p => p.Stamp > cutoff)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_DateTimeOffset_between_two_dates_filters_inclusive_range()
    {
        var lo = new DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero);
        var hi = new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero);
        var result = await _ctx.Query<WdtoItem>()
            .Where(p => p.Stamp >= lo && p.Stamp <= hi)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WdtoItem")]
    public sealed class WdtoItem
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
