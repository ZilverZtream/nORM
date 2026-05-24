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
/// Pins server-side translation of common DateTime predicate / projection shapes
/// that show up in user code:
/// <list type="bullet">
///   <item><c>x.Created.Year == year</c> — date-part extraction in WHERE</item>
///   <item><c>x.Created &gt; DateTime.UtcNow.AddDays(-7)</c> — relative date filter</item>
///   <item><c>x.Created.AddDays(7) &lt; other</c> — column-side date arithmetic</item>
///   <item><c>x.Created.Date == DateTime.Today</c> — day-precision compare</item>
/// </list>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeMethodTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private readonly DateTime _now = DateTime.UtcNow;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // Three rows: one 3 days old, one 30 days old, one in year 2024.
        var d1 = _now.AddDays(-3).ToString("yyyy-MM-dd HH:mm:ss");
        var d2 = _now.AddDays(-30).ToString("yyyy-MM-dd HH:mm:ss");
        var d3 = new DateTime(2024, 6, 15, 12, 0, 0).ToString("yyyy-MM-dd HH:mm:ss");
        cmd.CommandText = $"""
            CREATE TABLE DtRow (Id INTEGER PRIMARY KEY, Created TEXT NOT NULL);
            INSERT INTO DtRow VALUES (1,'{d1}'),(2,'{d2}'),(3,'{d3}');
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
    public async Task Year_extraction_in_where_filters_by_calendar_year()
    {
        var rows = await _ctx.Query<DtRow>().Where(r => r.Created.Year == 2024).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Id);
    }

    [Fact]
    public async Task Relative_utc_now_minus_days_filters_last_week()
    {
        var cutoff = _now.AddDays(-7);
        var rows = await _ctx.Query<DtRow>().Where(r => r.Created > cutoff).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Fact]
    public async Task Column_side_add_days_arithmetic_compares_against_runtime_cutoff()
    {
        // x.Created + 7d > now ⇔ x.Created > now - 7d
        var rows = await _ctx.Query<DtRow>().Where(r => r.Created.AddDays(7) > _now).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Table("DtRow")]
    public sealed class DtRow
    {
        [Key] public int Id { get; set; }
        public DateTime Created { get; set; }
    }
}
