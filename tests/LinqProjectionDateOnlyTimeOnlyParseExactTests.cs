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
/// Strict pin for <c>DateOnly.ParseExact / TimeOnly.ParseExact</c> in
/// projection. Sister to DateTime.ParseExact (e337893). DateOnly stores
/// as 'yyyy-MM-dd' so yyyyMMdd → SUBSTR-rebuild produces compatible
/// text; TimeOnly stores as 'HH:mm:ss.fffffff' so the HH-only formats
/// need similar slicing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateOnlyTimeOnlyParseExactTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdotpItem (Id INTEGER PRIMARY KEY, DateText TEXT NOT NULL);
            INSERT INTO PdotpItem VALUES
                (1, '20260525'),
                (2, '20261231');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdotpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateOnly_ParseExact_yyyyMMdd_returns_DateOnly_per_row()
    {
        var r = await _ctx.Query<PdotpItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateOnly.ParseExact(p.DateText, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateOnly(2026, 5, 25), r[0].D);
        Assert.Equal(new DateOnly(2026, 12, 31), r[1].D);
    }

    [Table("PdotpItem")]
    public sealed class PdotpItem
    {
        [Key] public int Id { get; set; }
        public string DateText { get; set; } = string.Empty;
    }
}
