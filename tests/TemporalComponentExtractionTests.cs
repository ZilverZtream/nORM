using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Server-side DateTime/DateTimeOffset component extraction must match .NET: TimeOfDay preserves sub-second
/// precision; Millisecond TRUNCATES (floor) like .NET, not rounds; and DateTimeOffset components return the
/// WALL-CLOCK value in the stored offset, not the UTC-normalized instant. Each projected component must equal
/// the materialized value (self-consistency), and predicates must not silently drop/add rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalComponentExtractionTests
{
    [Table("TceDt")] public class DtRow { [Key] public int Id { get; set; } public DateTime Stamp { get; set; } }
    [Table("TceDto")] public class DtoRow { [Key] public int Id { get; set; } public DateTimeOffset Stamp { get; set; } }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, string createSql)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = createSql; cmd.ExecuteNonQuery(); }
        return await Task.FromResult(new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task TimeOfDay_projection_preserves_subsecond()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, "CREATE TABLE TceDt (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);");
        ctx.Add(new DtRow { Id = 1, Stamp = new DateTime(2026, 5, 24, 12, 0, 0).AddTicks(5_000_000) }); // 12:00:00.5
        await ctx.SaveChangesAsync();
        var t = ctx.Query<DtRow>().Select(x => x.Stamp.TimeOfDay).ToList().Single();
        Assert.Equal(new TimeSpan(12, 0, 0) + TimeSpan.FromMilliseconds(500), t);
    }

    [Fact]
    public async Task TimeOfDay_predicate_keeps_subsecond_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, "CREATE TABLE TceDt (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);");
        ctx.Add(new DtRow { Id = 1, Stamp = new DateTime(2026, 5, 24, 12, 0, 0).AddTicks(5_000_000) });
        await ctx.SaveChangesAsync();
        var bound = new TimeSpan(12, 0, 0);
        var ids = ctx.Query<DtRow>().Where(x => x.Stamp.TimeOfDay > bound).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());   // 12:00:00.5 > 12:00:00
    }

    [Fact]
    public async Task Millisecond_truncates_not_rounds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, "CREATE TABLE TceDt (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);");
        var stamp = new DateTime(2026, 5, 24, 12, 30, 45).AddTicks(4_569_999); // .4569999 -> ms 456 (floor)
        ctx.Add(new DtRow { Id = 1, Stamp = stamp });
        await ctx.SaveChangesAsync();
        var ms = ctx.Query<DtRow>().Select(x => x.Stamp.Millisecond).ToList().Single();
        Assert.Equal(stamp.Millisecond, ms);   // .NET floors to 456
        Assert.Equal(456, ms);
    }

    [Fact]
    public async Task DateTimeOffset_hour_is_wall_clock_not_utc()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, "CREATE TABLE TceDto (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);");
        var dto = new DateTimeOffset(2026, 5, 24, 14, 0, 0, TimeSpan.FromHours(2)); // wall-clock hour = 14 (UTC 12)
        ctx.Add(new DtoRow { Id = 1, Stamp = dto });
        await ctx.SaveChangesAsync();
        var hour = ctx.Query<DtoRow>().Select(x => x.Stamp.Hour).ToList().Single();
        Assert.Equal(14, hour);
        var ids = ctx.Query<DtoRow>().Where(x => x.Stamp.Hour == 14).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());
    }
}
