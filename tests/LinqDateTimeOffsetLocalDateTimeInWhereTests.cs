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
/// Pins <c>dtoCol.LocalDateTime &lt; literal</c> (and ORDER BY by
/// LocalDateTime) — sister of the projection-side support (1f06ac1).
/// Uses snapshot semantics: the local offset is captured at query-build
/// time and baked into the SQL shift, consistent with the projection
/// path. See [[dto-local-datetime]] for the DST trade-off.
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
    public async Task LocalDateTime_in_WHERE_filters_by_snapshot_local_wall_clock()
    {
        var snap = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
        // Threshold = noon local wall-clock on the same date.
        var noonLocal = new DateTime(2026, 5, 25, 12, 0, 0);
        var rows = await _ctx.Query<DowRow>()
            .Where(r => r.Dto.LocalDateTime >= noonLocal)
            .OrderBy(r => r.Id)
            .ToListAsync();
        // Row 1 UTC 06:00, row 2 UTC 12:00, row 3 UTC 18:00. After local-shift by snap,
        // figure out which rows have wall-clock >= noon.
        var matchedIds = new[] { 1, 2, 3 }
            .Where(id => {
                var utc = id == 1 ? 6 : id == 2 ? 12 : 18;
                var localWall = new DateTime(2026, 5, 25, utc, 0, 0).Add(snap);
                return localWall >= noonLocal;
            })
            .ToArray();
        Assert.Equal(matchedIds, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task LocalDateTime_in_OrderBy_orders_by_snapshot_local_wall_clock()
    {
        var snap = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
        var rows = await _ctx.Query<DowRow>()
            .OrderBy(r => r.Dto.LocalDateTime)
            .ToListAsync();
        // Local wall-clock order = UTC-instant order for same-offset rows. All rows here
        // share +00:00 storage so the snapshot shift is uniform — order should still match
        // UTC order 06:00 < 12:00 < 18:00 → IDs 1, 2, 3.
        Assert.Equal(new[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("DowRow")]
    public sealed class DowRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}
