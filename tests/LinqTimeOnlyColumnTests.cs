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
/// Pins materializer + WHERE round-trip on a <see cref="TimeOnly"/> column.
/// SQLite stores TimeOnly as TEXT in <c>HH:mm:ss</c> form; the binding must
/// produce that exact representation so equality predicates match. Range
/// predicates must compare lexicographically in the correct order.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeOnlyColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ToRow (Id INTEGER PRIMARY KEY, OpenAt TEXT NOT NULL);
            INSERT INTO ToRow VALUES
                (1, '08:30:00'),
                (2, '12:00:00'),
                (3, '17:45:00');
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
    public async Task TimeOnly_round_trips_through_materializer()
    {
        var rows = (await _ctx.Query<ToRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(new TimeOnly(8, 30), rows[0].OpenAt);
        Assert.Equal(new TimeOnly(12, 0), rows[1].OpenAt);
        Assert.Equal(new TimeOnly(17, 45), rows[2].OpenAt);
    }

    [Fact]
    public async Task TimeOnly_equality_filter_matches_single_row()
    {
        var target = new TimeOnly(12, 0);
        var rows = await _ctx.Query<ToRow>().Where(r => r.OpenAt == target).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    [Fact]
    public async Task TimeOnly_range_filter_matches_afternoon_rows()
    {
        var cutoff = new TimeOnly(12, 0);
        var rows = (await _ctx.Query<ToRow>().Where(r => r.OpenAt >= cutoff).ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2, rows[0].Id);
        Assert.Equal(3, rows[1].Id);
    }

    [Table("ToRow")]
    public sealed class ToRow
    {
        [Key] public int Id { get; set; }
        public TimeOnly OpenAt { get; set; }
    }
}
