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
/// Pins per-group MIN/MAX over <see cref="DateTime"/> and <see cref="DateOnly"/>
/// columns. SQLite stores both as TEXT in ISO form; aggregation must still
/// compare correctly chronologically. Sanity check that the DateOnly canonical-
/// ISO binding from ceb61a2 doesn't degrade in aggregate contexts.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupDateTimeAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GdRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Created TEXT NOT NULL, BirthDay TEXT NOT NULL);
            INSERT INTO GdRow VALUES
                (1, 'NA',   '2024-01-15 09:00:00', '2024-01-15'),
                (2, 'NA',   '2024-03-20 14:30:00', '2024-03-20'),
                (3, 'NA',   '2024-02-10 11:15:00', '2024-02-10'),
                (4, 'EMEA', '2024-06-05 08:45:00', '2024-06-05'),
                (5, 'EMEA', '2024-04-22 16:00:00', '2024-04-22');
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
    public async Task GroupBy_min_max_datetime_per_group_returns_chronological_extremes()
    {
        var groups = (await _ctx.Query<GdRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, Earliest = g.Min(r => r.Created), Latest = g.Max(r => r.Created) })
            .ToListAsync())
            .OrderBy(g => g.Region).ToArray();

        Assert.Equal(2, groups.Length);
        // EMEA earliest = 2024-04-22, latest = 2024-06-05
        Assert.Equal(new DateTime(2024, 4, 22, 16, 0, 0), groups[0].Earliest);
        Assert.Equal(new DateTime(2024, 6, 5,  8, 45, 0), groups[0].Latest);
        // NA earliest = 2024-01-15, latest = 2024-03-20
        Assert.Equal(new DateTime(2024, 1, 15, 9, 0, 0),  groups[1].Earliest);
        Assert.Equal(new DateTime(2024, 3, 20, 14, 30, 0), groups[1].Latest);
    }

    [Fact]
    public async Task GroupBy_min_max_dateonly_per_group_returns_chronological_extremes()
    {
        var groups = (await _ctx.Query<GdRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, FirstDay = g.Min(r => r.BirthDay), LastDay = g.Max(r => r.BirthDay) })
            .ToListAsync())
            .OrderBy(g => g.Region).ToArray();

        Assert.Equal(2, groups.Length);
        Assert.Equal(new DateOnly(2024, 4, 22), groups[0].FirstDay);
        Assert.Equal(new DateOnly(2024, 6, 5),  groups[0].LastDay);
        Assert.Equal(new DateOnly(2024, 1, 15), groups[1].FirstDay);
        Assert.Equal(new DateOnly(2024, 3, 20), groups[1].LastDay);
    }

    [Table("GdRow")]
    public sealed class GdRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public DateTime Created { get; set; }
        public DateOnly BirthDay { get; set; }
    }
}
