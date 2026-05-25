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
/// Strict pin for <c>TimeOnly.Parse(string)</c> in projection. Sister
/// to DateOnly.Parse. Microsoft.Data.Sqlite stores TimeOnly as
/// canonical 'HH:mm:ss[.fffffff]' text; SQL emission is identity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeOnlyParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtopItem (Id INTEGER PRIMARY KEY, TimeText TEXT NOT NULL);
            INSERT INTO PtopItem VALUES
                (1, '09:00:00'),
                (2, '23:59:59'),
                (3, '00:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtopItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeOnly_Parse_string_column_projects_TimeOnly_per_row()
    {
        var result = await _ctx.Query<PtopItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = TimeOnly.Parse(p.TimeText) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(new TimeOnly(9, 0, 0), result[0].T);
        Assert.Equal(new TimeOnly(23, 59, 59), result[1].T);
        Assert.Equal(new TimeOnly(0, 0, 0), result[2].T);
    }

    [Table("PtopItem")]
    public sealed class PtopItem
    {
        [Key] public int Id { get; set; }
        public string TimeText { get; set; } = string.Empty;
    }
}
