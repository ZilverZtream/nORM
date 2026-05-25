using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Strict pin for <c>DateTimeOffset.ParseExact(s, format, formatProvider)</c>
/// in projection. The TranslateMethodCall ParseExact branch was wired
/// for both typeof(DateTime) and typeof(DateTimeOffset) in e337893
/// since they share the switch; pin verifies the round-trip works
/// end-to-end and materializes as DateTimeOffset.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeOffsetParseExactTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtopeItem (Id INTEGER PRIMARY KEY, DateText TEXT NOT NULL);
            INSERT INTO PdtopeItem VALUES
                (1, '20260525'),
                (2, '20261231');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtopeItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTimeOffset_ParseExact_yyyyMMdd_returns_DateTimeOffset_per_row()
    {
        var r = await _ctx.Query<PdtopeItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateTimeOffset.ParseExact(p.DateText, "yyyyMMdd", CultureInfo.InvariantCulture) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 5, 25), r[0].D.DateTime);
        Assert.Equal(new DateTime(2026, 12, 31), r[1].D.DateTime);
    }

    [Table("PdtopeItem")]
    public sealed class PdtopeItem
    {
        [Key] public int Id { get; set; }
        public string DateText { get; set; } = string.Empty;
    }
}
