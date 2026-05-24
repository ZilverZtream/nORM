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
/// Pins materializer + WHERE round-trip on a <see cref="DateTimeOffset"/> column.
/// SQLite stores DateTimeOffset as TEXT in <c>yyyy-MM-dd HH:mm:ss.fffffffzzz</c>
/// form; the binding must produce a comparable representation so equality
/// predicates against a constant DateTimeOffset match the stored value.
/// Probes whether the same conversion-shape bug as DateOnly affects DTO.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtoRow (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO DtoRow VALUES
                (1, '2024-06-15 10:30:00+02:00'),
                (2, '2024-07-01 00:00:00+00:00'),
                (3, '2024-08-20 14:45:00-05:00');
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
    public async Task DateTimeOffset_round_trips_through_materializer()
    {
        var rows = (await _ctx.Query<DtoRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(TimeSpan.FromHours(2),  rows[0].Stamp.Offset);
        Assert.Equal(TimeSpan.Zero,          rows[1].Stamp.Offset);
        Assert.Equal(TimeSpan.FromHours(-5), rows[2].Stamp.Offset);
    }

    [Fact]
    public async Task DateTimeOffset_equality_filter_matches_with_offset_preserved()
    {
        // Use the materialized value from the round-trip to drive the equality predicate —
        // if the binding format differs from storage, the predicate silently misses the row.
        var materialized = (await _ctx.Query<DtoRow>().Where(r => r.Id == 1).ToListAsync())[0].Stamp;
        var rows = await _ctx.Query<DtoRow>().Where(r => r.Stamp == materialized).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Table("DtoRow")]
    public sealed class DtoRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
