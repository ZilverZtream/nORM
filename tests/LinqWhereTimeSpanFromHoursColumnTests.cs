using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probe pin for `Where(p =&gt; TimeSpan.FromHours(p.H) &gt; threshold)`.
/// Sister of LinqProjectionTimeSpanFactoryTests (49d489c) -- the WHERE
/// path uses ETSV which dispatches method calls through the same
/// provider TranslateMethodCall, so the lowering should work
/// symmetrically with projection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereTimeSpanFromHoursColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WtsfhItem (Id INTEGER PRIMARY KEY, H REAL NOT NULL);
            INSERT INTO WtsfhItem VALUES (1, 0.5), (2, 2.0), (3, 5.0), (4, 0.25);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WtsfhItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_TimeSpan_FromHours_of_column_greater_than_one_hour_filters_per_row()
    {
        var oneHour = TimeSpan.FromHours(1);
        var ids = await _ctx.Query<WtsfhItem>()
            .Where(p => TimeSpan.FromHours(p.H) > oneHour)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        // H > 1: rows 2 (2.0), 3 (5.0). Rows 1 (0.5), 4 (0.25) excluded.
        Assert.Equal(new[] { 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WtsfhItem")]
    public sealed class WtsfhItem
    {
        [Key] public int Id { get; set; }
        public double H { get; set; }
    }
}
