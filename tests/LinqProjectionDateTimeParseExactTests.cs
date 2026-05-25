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
/// Strict pin for <c>DateTime.ParseExact(s, format, formatProvider)</c>
/// in projection. Common shape: legacy column stores 'yyyyMMdd' or
/// 'dd/MM/yyyy' text and the caller projects a DateTime. SQLite has
/// no full strftime-parse primitive but specific common formats can
/// be rewritten into the canonical 'yyyy-MM-dd HH:MM:SS' that the
/// materializer expects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeParseExactTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtpeItem (Id INTEGER PRIMARY KEY, DateText TEXT NOT NULL);
            INSERT INTO PdtpeItem VALUES
                (1, '20260525'),
                (2, '20261231');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtpeItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_ParseExact_yyyyMMdd_returns_DateTime_per_row()
    {
        var r = await _ctx.Query<PdtpeItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateTime.ParseExact(p.DateText, "yyyyMMdd", CultureInfo.InvariantCulture) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 5, 25), r[0].D);
        Assert.Equal(new DateTime(2026, 12, 31), r[1].D);
    }

    [Table("PdtpeItem")]
    public sealed class PdtpeItem
    {
        [Key] public int Id { get; set; }
        public string DateText { get; set; } = string.Empty;
    }
}
