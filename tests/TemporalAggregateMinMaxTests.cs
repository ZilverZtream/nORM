using System;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// MIN/MAX over a TEXT-stored temporal column must be by value, not by lexical TEXT order, and must
/// materialize back (not throw). SQLite's MIN/MAX use the column collation (BINARY = lexical), which
/// mis-orders multi-day <see cref="TimeSpan"/> ("10.00:00:00" sorts below "9.23:59:59") and mixed-offset
/// <see cref="DateTimeOffset"/> (by wall-clock text, not instant). TimeOnly/DateOnly text sorts
/// chronologically so they are fine; decimal aggregates are already value-correct.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalAggregateMinMaxTests
{
    [Table("TsAgg_Test")]
    public sealed class TsRow { [Key] public int Id { get; set; } public TimeSpan Duration { get; set; } }

    [Table("DtoAgg_Test")]
    public sealed class DtoRow { [Key] public int Id { get; set; } public DateTimeOffset Dto { get; set; } }

    private static DbContext NewCtx(string ddlAndData)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand()) { c.CommandText = ddlAndData; c.ExecuteNonQuery(); }
        return new DbContext(cn, new SqliteProvider());
    }

    private const string TsData =
        "CREATE TABLE TsAgg_Test (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);" +
        "INSERT INTO TsAgg_Test VALUES (1,'10.00:00:00'),(2,'9.23:59:59'),(3,'2.00:00:00');";

    // Row 1 = 2026-05-25 23:00Z; Row 2 = 2026-05-25 22:00-05:00 = 2026-05-26 03:00Z (later instant, but
    // lexically smaller because "22" < "23").
    private const string DtoData =
        "CREATE TABLE DtoAgg_Test (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL);" +
        "INSERT INTO DtoAgg_Test VALUES (1,'2026-05-25 23:00:00+00:00'),(2,'2026-05-25 22:00:00-05:00');";

    [Fact]
    public void Max_timespan_is_by_duration_not_lexical()
    {
        using var ctx = NewCtx(TsData);
        Assert.Equal(TimeSpan.FromDays(10), ctx.Query<TsRow>().Max(r => r.Duration));
    }

    [Fact]
    public void Min_timespan_is_by_duration_not_lexical()
    {
        using var ctx = NewCtx(TsData);
        Assert.Equal(TimeSpan.FromDays(2), ctx.Query<TsRow>().Min(r => r.Duration));
    }

    [Fact]
    public void Max_datetimeoffset_is_by_instant_not_lexical()
    {
        using var ctx = NewCtx(DtoData);
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 22, 0, 0, TimeSpan.FromHours(-5)),
            ctx.Query<DtoRow>().Max(r => r.Dto)); // == compares instant
    }

    [Fact]
    public void Min_datetimeoffset_is_by_instant_not_lexical()
    {
        using var ctx = NewCtx(DtoData);
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 23, 0, 0, TimeSpan.Zero),
            ctx.Query<DtoRow>().Min(r => r.Dto));
    }

    [Fact]
    public void GroupBy_max_timespan_is_by_duration()
    {
        using var ctx = NewCtx(TsData);
        var maxes = ctx.Query<TsRow>().GroupBy(r => 1).Select(g => g.Max(x => x.Duration)).ToList();
        Assert.Equal(TimeSpan.FromDays(10), maxes.Single());
    }

    [Fact]
    public void GroupBy_min_datetimeoffset_is_by_instant()
    {
        using var ctx = NewCtx(DtoData);
        var mins = ctx.Query<DtoRow>().GroupBy(r => 1).Select(g => g.Min(x => x.Dto)).ToList();
        Assert.Equal(new DateTimeOffset(2026, 5, 25, 23, 0, 0, TimeSpan.Zero), mins.Single());
    }
}
