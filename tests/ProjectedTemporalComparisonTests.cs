using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A comparison RESULT projected in a Select (e.g. <c>Select(r =&gt; r.T == noon)</c>) must be evaluated by
/// value, exactly as the same comparison in a WHERE. ETSV (predicate visitor) canonicalizes TEXT-stored
/// temporal/decimal comparisons; the SelectClauseVisitor (projection visitor) must mirror it ("fix both
/// visitors"). Rows 1 and 2 are the same TimeOnly / decimal in different non-canonical TEXT, so the
/// projected equality must be true for both.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectedTemporalComparisonTests
{
    [Table("ProjCmp_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        [Column("Tval")] public TimeOnly T { get; set; }
        public decimal Dec { get; set; }
        public TimeSpan Ts { get; set; }
        public DateTimeOffset Dto { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE ProjCmp_Test (Id INTEGER PRIMARY KEY, Tval TEXT NOT NULL, Dec TEXT NOT NULL, " +
                "Ts TEXT NOT NULL, Dto TEXT NOT NULL);" +
                "INSERT INTO ProjCmp_Test VALUES " +
                "(1,'12:00:00','24.5','1.00:00:00','2026-05-25 12:00:00+00:00')," +
                "(2,'12:00:00.0000000','24.500','1.00:00:00.0000000','2026-05-25 14:00:00+02:00')," +
                "(3,'13:30:00','99.9','2.00:00:00','2026-05-25 09:00:00+00:00');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Projected_timeonly_equality_is_by_value()
    {
        using var ctx = NewCtx();
        var noon = new TimeOnly(12, 0, 0);
        var rows = await ctx.Query<Row>().Select(r => new { r.Id, M = r.T == noon }).OrderBy(r => r.Id).ToListAsync();
        Assert.True(rows.Single(r => r.Id == 1).M);
        Assert.True(rows.Single(r => r.Id == 2).M);   // 12:00:00.0000000 == 12:00:00
        Assert.False(rows.Single(r => r.Id == 3).M);
    }

    [Fact]
    public async Task Projected_decimal_equality_is_by_value()
    {
        using var ctx = NewCtx();
        var rows = await ctx.Query<Row>().Select(r => new { r.Id, M = r.Dec == 24.5m }).OrderBy(r => r.Id).ToListAsync();
        Assert.True(rows.Single(r => r.Id == 1).M);
        Assert.True(rows.Single(r => r.Id == 2).M);   // 24.500 == 24.5
        Assert.False(rows.Single(r => r.Id == 3).M);
    }

    [Fact]
    public async Task Projected_timespan_equality_is_by_value()
    {
        using var ctx = NewCtx();
        var oneDay = TimeSpan.FromDays(1);
        var rows = await ctx.Query<Row>().Select(r => new { r.Id, M = r.Ts == oneDay }).OrderBy(r => r.Id).ToListAsync();
        Assert.True(rows.Single(r => r.Id == 1).M);
        Assert.True(rows.Single(r => r.Id == 2).M);   // 1.00:00:00.0000000 == 1 day
        Assert.False(rows.Single(r => r.Id == 3).M);
    }

    [Fact]
    public async Task Projected_dto_equality_is_by_value()
    {
        using var ctx = NewCtx();
        var instant = new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero);
        var rows = await ctx.Query<Row>().Select(r => new { r.Id, M = r.Dto == instant }).OrderBy(r => r.Id).ToListAsync();
        Assert.True(rows.Single(r => r.Id == 1).M);
        Assert.True(rows.Single(r => r.Id == 2).M);   // same instant, +02:00 offset
        Assert.False(rows.Single(r => r.Id == 3).M);
    }

    // Mixed projection: a DateTimeOffset column compared to a DateTime literal. C# promotes the
    // DateTime (Utc kind → zero offset) to DateTimeOffset via its implicit operator, so this is an
    // instant comparison — row 2 (same instant, +02:00) must still project true. The SCV temporal
    // block gates DateTimeOffset canonicalization on BOTH sides being DTO; a DateTime literal on the
    // right is a Convert node, so this checks that mixed form is not left as a raw-text compare.
    [Fact]
    public async Task Projected_dto_equals_datetime_literal_is_by_instant()
    {
        using var ctx = NewCtx();
        var utcNoon = new DateTime(2026, 5, 25, 12, 0, 0, DateTimeKind.Utc);
        var rows = await ctx.Query<Row>().Select(r => new { r.Id, M = r.Dto == utcNoon }).OrderBy(r => r.Id).ToListAsync();
        Assert.True(rows.Single(r => r.Id == 1).M);
        Assert.True(rows.Single(r => r.Id == 2).M);   // 2026-05-25 14:00:00+02:00 == 12:00Z
        Assert.False(rows.Single(r => r.Id == 3).M);
    }
}
