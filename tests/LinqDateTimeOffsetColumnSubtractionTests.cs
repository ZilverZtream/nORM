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
/// Pins <c>dtoColA - dtoColB</c> in projection. Result is a TimeSpan
/// equal to the difference between the two UTC instants — independent
/// of the offsets each column was stored in. The SQL must lower to a
/// UTC epoch-microsecond difference (using the same normalization that powers
/// DateTimeOffset == DateTime equality) and the materialiser must reconstruct a
/// TimeSpan from the fractional seconds value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetColumnSubtractionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // Three pairs of DTOs. Each pair represents UTC instants that
        // differ by a known TimeSpan, with the LHS and RHS stored in
        // different offsets to verify the lowering is offset-aware.
        cmd.CommandText = """
            CREATE TABLE DsubRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO DsubRow VALUES
                (1, '2026-05-25 12:30:45+00:00', '2026-05-25 12:30:30+00:00'),
                (2, '2026-05-25 14:00:00+02:00', '2026-05-25 11:00:00-01:00'),
                (3, '2026-05-25 12:00:00+00:00', '2026-05-25 13:00:00+00:00'),
                (4, '2026-05-25 12:30:45.123456+00:00', '2026-05-25 12:30:45.123000+00:00');
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
    public async Task DateTimeOffset_minus_DateTimeOffset_returns_UTC_instant_difference_as_TimeSpan()
    {
        var rows = (await _ctx.Query<DsubRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Diff = r.A - r.B })
            .ToListAsync())
            .ToArray();
        // Row 1: 12:30:45Z - 12:30:30Z = +15s
        // Row 2: 12:00:00Z (=14:00+02:00) - 12:00:00Z (=11:00-01:00) = 0s
        // Row 3: 12:00:00Z - 13:00:00Z = -1h
        Assert.Equal(TimeSpan.FromSeconds(15), rows[0].Diff);
        Assert.Equal(TimeSpan.Zero,             rows[1].Diff);
        Assert.Equal(TimeSpan.FromHours(-1),    rows[2].Diff);
        Assert.InRange(rows[3].Diff.TotalMicroseconds, 455.5, 456.5);
    }

    [Table("DsubRow")]
    public sealed class DsubRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset A { get; set; }
        public DateTimeOffset B { get; set; }
    }
}
