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
/// Verifies DateTimeOffset member translations against real rows. Providers translate
/// DateTime and DateTimeOffset through the same TranslateFunction switch, so these tests
/// catch any regression that re-narrows the type guard to DateTime only — and they also
/// exercise the materializer's string → DateTimeOffset conversion fallback.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetMemberTests : IAsyncLifetime
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
                (1, '2020-01-15 09:30:45+00:00'),
                (2, '2021-06-30 12:00:00+00:00'),
                (3, '2022-12-31 23:59:59+00:00');
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
    public async Task Year_filters_DateTimeOffset_row_by_year()
    {
        var hits = await _ctx.Query<DtoRow>().Where(r => r.Stamp.Year == 2021).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(2, hits[0].Id);
    }

    [Fact]
    public async Task Day_filters_DateTimeOffset_row_by_day()
    {
        var hits = await _ctx.Query<DtoRow>().Where(r => r.Stamp.Day == 31).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(3, hits[0].Id);
    }

    [Fact]
    public async Task Hour_filters_DateTimeOffset_row_by_hour()
    {
        var hits = await _ctx.Query<DtoRow>().Where(r => r.Stamp.Hour == 12).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(2, hits[0].Id);
    }

    [Table("DtoRow")]
    public sealed class DtoRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
