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
/// Pins materializer + WHERE round-trip on a <see cref="DateOnly"/> column.
/// SQLite has no native DATE type so the value is stored as TEXT in
/// <c>yyyy-MM-dd</c> form; the materializer must convert back to DateOnly,
/// and predicates against a constant DateOnly must produce the right row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateOnlyColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DoRow (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL);
            INSERT INTO DoRow VALUES
                (1, '2024-01-15'),
                (2, '2024-06-30'),
                (3, '2025-03-01');
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
    public async Task DateOnly_round_trips_through_materializer()
    {
        var rows = (await _ctx.Query<DoRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(new DateOnly(2024, 1, 15), rows[0].BirthDate);
        Assert.Equal(new DateOnly(2024, 6, 30), rows[1].BirthDate);
        Assert.Equal(new DateOnly(2025, 3, 1),  rows[2].BirthDate);
    }

    [Fact]
    public async Task DateOnly_equality_filter_matches_single_row()
    {
        var target = new DateOnly(2024, 6, 30);
        var rows = await _ctx.Query<DoRow>().Where(r => r.BirthDate == target).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    [Fact]
    public async Task DateOnly_range_filter_matches_rows_in_year()
    {
        var lo = new DateOnly(2024, 1, 1);
        var hi = new DateOnly(2024, 12, 31);
        var rows = (await _ctx.Query<DoRow>()
            .Where(r => r.BirthDate >= lo && r.BirthDate <= hi)
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(2, rows[1].Id);
    }

    [Table("DoRow")]
    public sealed class DoRow
    {
        [Key] public int Id { get; set; }
        public DateOnly BirthDate { get; set; }
    }
}
