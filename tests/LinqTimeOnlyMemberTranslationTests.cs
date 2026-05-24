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
/// Exercises TimeOnly Hour / Minute / Second member translations against real rows. Failure
/// of any of these tests means the provider's TranslateFunction stopped recognizing the
/// TimeOnly type or its member names.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeOnlyMemberTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ToRow (Id INTEGER PRIMARY KEY, T TEXT NOT NULL);
            INSERT INTO ToRow VALUES
                (1, '09:30:45'),
                (2, '12:00:00'),
                (3, '23:59:59');
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
    public async Task Hour_filters_by_hour_component()
    {
        var hits = await _ctx.Query<ToRow>().Where(r => r.T.Hour == 23).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(3, hits[0].Id);
    }

    [Fact]
    public async Task Minute_filters_by_minute_component()
    {
        var hits = await _ctx.Query<ToRow>().Where(r => r.T.Minute == 30).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Fact]
    public async Task Second_filters_by_second_component()
    {
        var hits = await _ctx.Query<ToRow>().Where(r => r.T.Second == 45).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Table("ToRow")]
    public sealed class ToRow
    {
        [Key] public int Id { get; set; }
        public TimeOnly T { get; set; }
    }
}
