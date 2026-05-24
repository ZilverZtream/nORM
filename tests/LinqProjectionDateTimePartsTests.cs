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
/// Pins <c>p.DateColumn.Year</c> / <c>.Month</c> / <c>.Day</c> inside a Select
/// projection. ExpressionToSqlVisitor (WHERE side) routes these through
/// <c>_provider.TranslateFunction(memberName, typeof(DateTime), exprSql)</c>
/// and SqliteProvider returns <c>CAST(strftime('%Y', col) AS INTEGER)</c>, but
/// the projection side (SelectClauseVisitor.VisitMember) only consults
/// <c>_mapping.ColumnsByName</c> and throws "Member 'Year' is not mapped to a
/// column" otherwise. Highest silent-wrongness risk: the WHERE-side parity
/// implies "this works" but the projection-side throws hard.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimePartsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtpRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT NOT NULL);
            INSERT INTO DtpRow VALUES
              (1, '2024-01-15 10:30:00'),
              (2, '2025-07-04 14:45:00'),
              (3, '2026-12-31 23:59:00');
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
    public async Task Projection_datetime_year_returns_year_part_per_row()
    {
        var rows = (await _ctx.Query<DtpRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Y = r.CreatedAt.Year })
            .ToListAsync())
            .Select(r => (r.Id, r.Y))
            .ToArray();
        Assert.Equal(new[] { (1, 2024), (2, 2025), (3, 2026) }, rows);
    }

    [Fact]
    public async Task Projection_datetime_month_returns_month_part_per_row()
    {
        var rows = (await _ctx.Query<DtpRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, M = r.CreatedAt.Month })
            .ToListAsync())
            .Select(r => (r.Id, r.M))
            .ToArray();
        Assert.Equal(new[] { (1, 1), (2, 7), (3, 12) }, rows);
    }

    [Fact]
    public async Task Projection_datetime_day_returns_day_part_per_row()
    {
        var rows = (await _ctx.Query<DtpRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, D = r.CreatedAt.Day })
            .ToListAsync())
            .Select(r => (r.Id, r.D))
            .ToArray();
        Assert.Equal(new[] { (1, 15), (2, 4), (3, 31) }, rows);
    }

    [Table("DtpRow")]
    public sealed class DtpRow
    {
        [Key] public int Id { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
