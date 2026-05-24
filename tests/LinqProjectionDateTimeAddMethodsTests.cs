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
/// Pins <c>p.DateColumn.AddDays(n)</c> / <c>.AddMonths(n)</c> / <c>.AddYears(n)</c>
/// inside a Select projection. Sister of <see cref="LinqProjectionDateTimePartsTests"/>
/// for the method-call side: routes through SCV.VisitMethodCall + 5008637's
/// TranslateFunction call. Instance method = <c>node.Object</c> + arity-2 args,
/// which exercises a different shape than the static Math methods. Also verifies
/// the SQLite TEXT-stored datetime round-trips back into DateTime through the
/// materializer.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeAddMethodsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtaRow (Id INTEGER PRIMARY KEY, Anchor TEXT NOT NULL);
            INSERT INTO DtaRow VALUES
              (1, '2024-01-15 00:00:00'),
              (2, '2025-07-04 00:00:00');
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
    public async Task Projection_datetime_adddays_returns_date_shifted_by_days()
    {
        var rows = (await _ctx.Query<DtaRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Later = p.Anchor.AddDays(10) })
            .ToListAsync())
            .Select(r => (r.Id, r.Later))
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(new DateTime(2024, 1, 25), rows[0].Later);
        Assert.Equal(new DateTime(2025, 7, 14), rows[1].Later);
    }

    [Fact]
    public async Task Projection_datetime_addmonths_returns_date_shifted_by_months()
    {
        var rows = (await _ctx.Query<DtaRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Later = p.Anchor.AddMonths(2) })
            .ToListAsync())
            .Select(r => (r.Id, r.Later))
            .ToArray();
        Assert.Equal(new DateTime(2024, 3, 15), rows[0].Later);
        Assert.Equal(new DateTime(2025, 9, 4), rows[1].Later);
    }

    [Fact]
    public async Task Projection_datetime_addyears_returns_date_shifted_by_years()
    {
        var rows = (await _ctx.Query<DtaRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Later = p.Anchor.AddYears(3) })
            .ToListAsync())
            .Select(r => (r.Id, r.Later))
            .ToArray();
        Assert.Equal(new DateTime(2027, 1, 15), rows[0].Later);
        Assert.Equal(new DateTime(2028, 7, 4), rows[1].Later);
    }

    [Table("DtaRow")]
    public sealed class DtaRow
    {
        [Key] public int Id { get; set; }
        public DateTime Anchor { get; set; }
    }
}
