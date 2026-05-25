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
/// Strict pin for <c>TimeOnly.IsBetween(start, end)</c> inside WHERE.
/// Sister to the projection fix in 2cc2637. ETSV routes through
/// TranslateFunction so the provider entry should cover the WHERE
/// path; pin guards regressions.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereTimeOnlyIsBetweenTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WtoibItem (Id INTEGER PRIMARY KEY, T TEXT NOT NULL);
            INSERT INTO WtoibItem VALUES
                (1, '08:00:00'),
                (2, '12:00:00'),
                (3, '23:30:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WtoibItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_TimeOnly_IsBetween_normal_range_returns_only_in_window_rows()
    {
        var start = new TimeOnly(9, 0);
        var end = new TimeOnly(17, 0);
        var ids = await _ctx.Query<WtoibItem>()
            .Where(p => p.T.IsBetween(start, end))
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WtoibItem")]
    public sealed class WtoibItem
    {
        [Key] public int Id { get; set; }
        public TimeOnly T { get; set; }
    }
}
