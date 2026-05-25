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
/// Strict pin + implement-first for <c>DateTime.Parse(string)</c> in
/// projection. Common shape: a legacy column stores timestamp as TEXT
/// in a string property; project DateTime for downstream consumption.
/// SQLite stores DateTime as TEXT natively, so the value column is
/// already DateTime-compatible text -- the conversion is essentially
/// identity, but the materializer needs to read the result as DateTime
/// rather than the source's string type.
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Returns the unparsed string with wrong projected type.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtpItem (Id INTEGER PRIMARY KEY, StampText TEXT NOT NULL);
            INSERT INTO PdtpItem VALUES
                (1, '2026-05-24 12:00:00'),
                (2, '2026-05-25 14:30:00'),
                (3, '2026-05-26 00:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_Parse_string_column_projects_DateTime_per_row()
    {
        var result = await _ctx.Query<PdtpItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateTime.Parse(p.StampText) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(new DateTime(2026, 5, 24, 12, 0, 0), result[0].D);
        Assert.Equal(new DateTime(2026, 5, 25, 14, 30, 0), result[1].D);
        Assert.Equal(new DateTime(2026, 5, 26, 0, 0, 0), result[2].D);
    }

    [Table("PdtpItem")]
    public sealed class PdtpItem
    {
        [Key] public int Id { get; set; }
        public string StampText { get; set; } = string.Empty;
    }
}
