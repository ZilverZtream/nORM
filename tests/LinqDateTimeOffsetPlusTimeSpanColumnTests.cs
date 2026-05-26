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
/// Pins <c>dtoCol + tsCol</c> and <c>dtoCol - tsCol</c> in projection.
/// Both operands are column references — the SQL must shift the DTO
/// instant by the TimeSpan column's fractional seconds and round-trip
/// the result as a DateTimeOffset preserving the original wall-clock
/// rendering.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetPlusTimeSpanColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtsRow (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL, Span TEXT NOT NULL);
            INSERT INTO DtsRow VALUES
                (1, '2026-05-25 12:00:00+00:00', '01:30:00'),
                (2, '2026-05-25 12:00:00+02:00', '00:00:30'),
                (3, '2026-05-25 12:00:00+00:00', '-00:15:00');
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
    public async Task DateTimeOffset_column_plus_TimeSpan_column_shifts_instant_by_column_value()
    {
        var rows = (await _ctx.Query<DtsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Shifted = r.Dto + r.Span })
            .ToListAsync())
            .ToArray();
        // Row 1: 12:00:00Z + 01:30:00 → 13:30:00 (UTC instant), offset +00:00.
        // Row 2: 12:00:00+02:00 + 00:00:30 → 12:00:30 (wall) +02:00.
        // Row 3: 12:00:00Z + (-00:15:00) → 11:45:00 (UTC), offset +00:00.
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 13, 30,  0, TimeSpan.Zero).UtcDateTime,
                     rows[0].Shifted.UtcDateTime);
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 10,  0, 30, TimeSpan.Zero).UtcDateTime,
                     rows[1].Shifted.UtcDateTime);
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 11, 45,  0, TimeSpan.Zero).UtcDateTime,
                     rows[2].Shifted.UtcDateTime);
    }

    [Fact]
    public async Task DateTimeOffset_column_minus_TimeSpan_column_shifts_instant_backward()
    {
        var rows = (await _ctx.Query<DtsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Shifted = r.Dto - r.Span })
            .ToListAsync())
            .ToArray();
        // Row 1: 12:00:00Z - 01:30:00 → 10:30:00 UTC.
        // Row 3: 12:00:00Z - (-00:15:00) = 12:15:00 UTC.
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 10, 30,  0, TimeSpan.Zero).UtcDateTime,
                     rows[0].Shifted.UtcDateTime);
        Assert.Equal(new DateTimeOffset(2026, 5, 25,  9, 59, 30, TimeSpan.Zero).UtcDateTime,
                     rows[1].Shifted.UtcDateTime);
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 12, 15,  0, TimeSpan.Zero).UtcDateTime,
                     rows[2].Shifted.UtcDateTime);
    }

    [Table("DtsRow")]
    public sealed class DtsRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Span { get; set; }
    }
}
