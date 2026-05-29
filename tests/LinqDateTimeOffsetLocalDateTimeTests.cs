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
/// <summary>
/// Pins <c>dtoCol.LocalDateTime</c> in projection. nORM preserves
/// per-instant local timezone semantics by lowering TimeZoneInfo.Local offset
/// ranges to provider SQL, so rows on different DST sides use their own
/// local offset.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetLocalDateTimeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DolRow (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL);
            INSERT INTO DolRow VALUES
                (1, '2026-05-25 12:30:45+00:00'),
                (2, '2025-12-31 23:00:00+01:00');
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
    public async Task LocalDateTime_accessor_returns_local_offset_wall_clock_in_projection()
    {
        var rows = (await _ctx.Query<DolRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Local = r.Dto.LocalDateTime })
            .ToListAsync())
            .ToArray();
        var row1Expected = new DateTimeOffset(2026, 5, 25, 12, 30, 45, TimeSpan.Zero).LocalDateTime;
        var row2Expected = new DateTimeOffset(2025, 12, 31, 23, 0, 0, TimeSpan.FromHours(1)).LocalDateTime;
        Assert.Equal(row1Expected, DateTime.SpecifyKind(rows[0].Local, DateTimeKind.Unspecified));
        Assert.Equal(row2Expected, DateTime.SpecifyKind(rows[1].Local, DateTimeKind.Unspecified));
    }

    [Table("DolRow")]
    public sealed class DolRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}
