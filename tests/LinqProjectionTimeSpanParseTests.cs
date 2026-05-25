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
/// Strict pin for <c>TimeSpan.Parse(string)</c> in projection. Sister to
/// Guid.Parse / DateTime.Parse / bool.Parse just landed. Common shape:
/// a legacy column stores duration as "HH:MM:SS" text; project as TimeSpan.
/// Microsoft.Data.Sqlite stores TimeSpan as text in the canonical
/// "HH:MM:SS[.fffffff]" format, so SQL emission is identity and the
/// materializer's GetFieldValue&lt;TimeSpan&gt; round-trips the text.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtspItem (Id INTEGER PRIMARY KEY, DurationText TEXT NOT NULL);
            INSERT INTO PtspItem VALUES
                (1, '01:30:00'),
                (2, '00:00:45'),
                (3, '10:15:30');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtspItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeSpan_Parse_string_column_projects_TimeSpan_per_row()
    {
        var result = await _ctx.Query<PtspItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = TimeSpan.Parse(p.DurationText) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(new TimeSpan(1, 30, 0), result[0].T);
        Assert.Equal(new TimeSpan(0, 0, 45), result[1].T);
        Assert.Equal(new TimeSpan(10, 15, 30), result[2].T);
    }

    [Table("PtspItem")]
    public sealed class PtspItem
    {
        [Key] public int Id { get; set; }
        public string DurationText { get; set; } = string.Empty;
    }
}
