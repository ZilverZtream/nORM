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
/// Pins <c>dtoCol.LocalDateTime</c> in WHERE and ORDER BY. nORM preserves
/// per-instant local timezone semantics by lowering TimeZoneInfo.Local offset
/// ranges to provider SQL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetLocalDateTimeInWhereTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DowRow (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL);
            INSERT INTO DowRow VALUES
                (1, '2026-05-25 06:00:00+00:00'),
                (2, '2026-05-25 12:00:00+00:00'),
                (3, '2026-05-25 18:00:00+00:00');
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
    public async Task LocalDateTime_in_WHERE_filters_by_per_instant_local_wall_clock()
    {
        var noonLocal = new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero).LocalDateTime;
        var rows = await _ctx.Query<DowRow>()
            .Where(r => r.Dto.LocalDateTime >= noonLocal)
            .OrderBy(r => r.Id)
            .ToListAsync();

        var matchedIds = new[]
            {
                (Id: 1, Utc: new DateTimeOffset(2026, 5, 25, 6, 0, 0, TimeSpan.Zero)),
                (Id: 2, Utc: new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero)),
                (Id: 3, Utc: new DateTimeOffset(2026, 5, 25, 18, 0, 0, TimeSpan.Zero))
            }
            .Where(row => row.Utc.LocalDateTime >= noonLocal)
            .Select(row => row.Id)
            .ToArray();
        Assert.Equal(matchedIds, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task LocalDateTime_in_OrderBy_orders_by_local_wall_clock()
    {
        var rows = await _ctx.Query<DowRow>()
            .OrderBy(r => r.Dto.LocalDateTime)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("DowRow")]
    public sealed class DowRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}
