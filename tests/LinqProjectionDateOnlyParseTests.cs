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
/// Strict pin for <c>DateOnly.Parse(string)</c> in projection. Sister to
/// TimeSpan.Parse / Guid.Parse / DateTime.Parse / bool.Parse. Common
/// shape: a legacy column stores date as 'yyyy-MM-dd' text; project as
/// DateOnly. Microsoft.Data.Sqlite stores DateOnly as canonical
/// 'yyyy-MM-dd' text, so SQL emission is identity and the materializer's
/// GetFieldValue&lt;DateOnly&gt; round-trips the text.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateOnlyParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdopItem (Id INTEGER PRIMARY KEY, DateText TEXT NOT NULL);
            INSERT INTO PdopItem VALUES
                (1, '2026-05-25'),
                (2, '2026-12-31'),
                (3, '2000-01-01');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdopItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateOnly_Parse_string_column_projects_DateOnly_per_row()
    {
        var result = await _ctx.Query<PdopItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateOnly.Parse(p.DateText) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(new DateOnly(2026, 5, 25), result[0].D);
        Assert.Equal(new DateOnly(2026, 12, 31), result[1].D);
        Assert.Equal(new DateOnly(2000, 1, 1), result[2].D);
    }

    [Table("PdopItem")]
    public sealed class PdopItem
    {
        [Key] public int Id { get; set; }
        public string DateText { get; set; } = string.Empty;
    }
}
