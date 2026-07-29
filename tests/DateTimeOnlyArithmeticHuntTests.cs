using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: DateOnly/TimeOnly arithmetic, composition, comparison and
/// component extraction on SQLite, verified against a LINQ-to-Objects oracle.
/// Tests named *_BUG assert the oracle-correct value and FAIL FIRST, documenting a
/// silent-wrong result. Tests named *_Clean assert correct behavior and PASS.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeOnlyArithmeticHuntTests
{
    private readonly ITestOutputHelper _out;
    public DateTimeOnlyArithmeticHuntTests(ITestOutputHelper o) { _out = o; }

    [Table("TRow")]
    public class TRow
    {
        [Key] public int Id { get; set; }
        public DateOnly D { get; set; }
        public TimeOnly T { get; set; }
        public DateTime Dt { get; set; }
    }

    private static async Task<DbContext> NewCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TRow (Id INTEGER PRIMARY KEY, D TEXT NOT NULL, T TEXT NOT NULL, Dt TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return await Task.FromResult(new DbContext(cn, new SqliteProvider()));
    }

    private static string RawText(SqliteConnection cn, string col, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {col} FROM TRow WHERE Id = {id}";
        return Convert.ToString(cmd.ExecuteScalar())!;
    }

    // ---------------------------------------------------------------------
    //  Diagnostic (always passes) — records the stored representation.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task Diagnostic_dump_storage_and_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var t = new TimeOnly(12, 0, 0, 500);
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021, 1, 31), T = t, Dt = new DateTime(2021, 1, 1, 12, 0, 0).AddMilliseconds(500) });
        await ctx.SaveChangesAsync();
        _out.WriteLine("stored T   = " + RawText(cn, "T", 1));
        _out.WriteLine("stored Dt  = " + RawText(cn, "Dt", 1));
        var addH = ctx.Query<TRow>().Select(x => x.T.AddHours(1)).ToList().Single();
        _out.WriteLine("AddHours(1) actual = " + addH.ToString("HH:mm:ss.fffffff") + "  oracle = " + t.AddHours(1).ToString("HH:mm:ss.fffffff"));
    }

    // ---------------------------------------------------------------------
    //  BUG 1: TimeOnly.AddHours drops the receiver's sub-second fraction.
    //  Root cause: AddSecondsToTimeOnlySql formats result with '%H:%M:%S'.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_AddHours_preserves_subsecond_BUG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var t = new TimeOnly(12, 0, 0, 500);        // 12:00:00.5000000
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021, 1, 1), T = t, Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var actual = ctx.Query<TRow>().Select(x => x.T.AddHours(1)).ToList().Single();
        Assert.Equal(t.AddHours(1), actual);        // oracle 13:00:00.5000000
    }

    // ---------------------------------------------------------------------
    //  BUG 2: TimeOnly.AddMinutes drops the receiver's sub-second fraction.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_AddMinutes_preserves_subsecond_BUG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var t = new TimeOnly(9, 15, 30, 250);       // 09:15:30.2500000
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021, 1, 1), T = t, Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var actual = ctx.Query<TRow>().Select(x => x.T.AddMinutes(5)).ToList().Single();
        Assert.Equal(t.AddMinutes(5), actual);      // oracle 09:20:30.2500000
    }

    // ---------------------------------------------------------------------
    //  BUG 3: TimeOnly.Add(TimeSpan) drops the delta's sub-second component.
    //  Root cause: (long)span.TotalSeconds truncates to whole seconds AND
    //  the '%H:%M:%S' output format strips any fraction.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_Add_TimeSpan_subsecond_delta_BUG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var t = new TimeOnly(12, 0, 0, 0);
        var delta = TimeSpan.FromMilliseconds(1500);   // 1.5 s
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021, 1, 1), T = t, Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var actual = ctx.Query<TRow>().Select(x => x.T.Add(delta)).ToList().Single();
        Assert.Equal(t.Add(delta), actual);          // oracle 12:00:01.5000000
    }

    // ---------------------------------------------------------------------
    //  BUG 4: TimeOnly.FromDateTime drops the sub-second fraction.
    //  Root cause: time(col) formats 'HH:mm:ss', truncating to whole seconds.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_FromDateTime_preserves_subsecond_BUG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var dt = new DateTime(2021, 3, 4, 8, 9, 10).AddTicks(1234567);   // .1234567
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021, 1, 1), T = new TimeOnly(1, 0, 0), Dt = dt });
        await ctx.SaveChangesAsync();
        var actual = ctx.Query<TRow>().Select(x => TimeOnly.FromDateTime(x.Dt)).ToList().Single();
        Assert.Equal(TimeOnly.FromDateTime(dt), actual);   // oracle 08:09:10.1234567
    }

    // ---------------------------------------------------------------------
    //  BUG 5: predicate row-selection — TimeOnly.AddHours in WHERE drops a
    //  row whose true (sub-second-bearing) result should satisfy the filter.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_AddHours_predicate_keeps_subsecond_row_BUG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        // row1: 12:00:00.5 -> +1h = 13:00:00.5  (> 13:00:00 -> keep)
        // row2: 12:00:00.0 -> +1h = 13:00:00.0  (not > 13:00:00 -> drop)
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021,1,1), T = new TimeOnly(12,0,0,500), Dt = DateTime.UnixEpoch });
        ctx.Add(new TRow { Id = 2, D = new DateOnly(2021,1,1), T = new TimeOnly(12,0,0,0),   Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var cutoff = new TimeOnly(13, 0, 0);
        var ids = ctx.Query<TRow>().Where(x => x.T.AddHours(1) > cutoff).Select(x => x.Id).ToList();
        var oracle = new[] { new { Id = 1, T = new TimeOnly(12,0,0,500) }, new { Id = 2, T = new TimeOnly(12,0,0,0) } }
            .Where(x => x.T.AddHours(1) > cutoff).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, ids.ToArray());          // oracle {1}
    }

    // ---------------------------------------------------------------------
    //  CLEAN: midnight wrap (no sub-second) matches .NET modulo-24h wrap.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_AddHours_midnight_wrap_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var t = new TimeOnly(23, 30, 0, 0);
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021,1,1), T = t, Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var fwd = ctx.Query<TRow>().Select(x => x.T.AddHours(3)).ToList().Single();
        Assert.Equal(t.AddHours(3), fwd);              // 02:30 next-day wrap
        var back = ctx.Query<TRow>().Select(x => x.T.AddHours(-26)).ToList().Single();
        Assert.Equal(t.AddHours(-26), back);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: DateOnly.AddMonths day clamp (Jan 31 + 1m = Feb 28/29).
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_AddMonths_clamp_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2021,1,31), T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch });
        ctx.Add(new TRow { Id = 2, D = new DateOnly(2020,1,31), T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch }); // leap
        ctx.Add(new TRow { Id = 3, D = new DateOnly(2021,3,31), T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch }); // -1m
        await ctx.SaveChangesAsync();
        var r1 = ctx.Query<TRow>().Where(x => x.Id == 1).Select(x => x.D.AddMonths(1)).ToList().Single();
        var r2 = ctx.Query<TRow>().Where(x => x.Id == 2).Select(x => x.D.AddMonths(1)).ToList().Single();
        var r3 = ctx.Query<TRow>().Where(x => x.Id == 3).Select(x => x.D.AddMonths(-1)).ToList().Single();
        Assert.Equal(new DateOnly(2021,2,28), r1);
        Assert.Equal(new DateOnly(2020,2,29), r2);
        Assert.Equal(new DateOnly(2021,2,28), r3);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: DateOnly.AddYears leap-day clamp (2024-02-29 + 1y = 2025-02-28).
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_AddYears_leapday_clamp_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        ctx.Add(new TRow { Id = 1, D = new DateOnly(2024,2,29), T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<TRow>().Select(x => x.D.AddYears(1)).ToList().Single();
        Assert.Equal(new DateOnly(2025,2,28), r);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: DateOnly comparison + ordering over TEXT 'yyyy-MM-dd'.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_comparison_and_ordering_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var ds = new[] { new DateOnly(2021,12,31), new DateOnly(2021,1,1), new DateOnly(2022,6,15), new DateOnly(2021,6,15) };
        for (int i = 0; i < ds.Length; i++)
            ctx.Add(new TRow { Id = i + 1, D = ds[i], T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var cut = new DateOnly(2021,6,15);
        var gt = ctx.Query<TRow>().Where(x => x.D > cut).OrderBy(x => x.D).Select(x => x.D).ToList();
        var oracle = ds.Where(d => d > cut).OrderBy(d => d).ToList();
        Assert.Equal(oracle, gt);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: DateOnly.DayNumber / FromDayNumber round-trip.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_DayNumber_roundtrip_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var d = new DateOnly(2021, 7, 29);
        ctx.Add(new TRow { Id = 1, D = d, T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var dn = ctx.Query<TRow>().Select(x => x.D.DayNumber).ToList().Single();
        Assert.Equal(d.DayNumber, dn);
        var rt = ctx.Query<TRow>().Select(x => DateOnly.FromDayNumber(x.D.DayNumber)).ToList().Single();
        Assert.Equal(d, rt);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: DateOnly components (Year/Month/Day/DayOfWeek/DayOfYear).
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_components_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var d = new DateOnly(2021, 7, 29);   // Thursday
        ctx.Add(new TRow { Id = 1, D = d, T = new TimeOnly(1,0,0), Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<TRow>().Select(x => new { x.D.Year, x.D.Month, x.D.Day, W = (int)x.D.DayOfWeek, x.D.DayOfYear }).ToList().Single();
        Assert.Equal(d.Year, r.Year);
        Assert.Equal(d.Month, r.Month);
        Assert.Equal(d.Day, r.Day);
        Assert.Equal((int)d.DayOfWeek, r.W);
        Assert.Equal(d.DayOfYear, r.DayOfYear);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: TimeOnly comparison sub-second precision + ordering.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_subsecond_comparison_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var ts = new[] { new TimeOnly(12,0,0,500), new TimeOnly(12,0,0,0), new TimeOnly(12,0,0,300) };
        for (int i = 0; i < ts.Length; i++)
            ctx.Add(new TRow { Id = i + 1, D = new DateOnly(2021,1,1), T = ts[i], Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var cut = new TimeOnly(12, 0, 0, 250);
        var gt = ctx.Query<TRow>().Where(x => x.T > cut).OrderBy(x => x.T).Select(x => x.T).ToList();
        var oracle = ts.Where(t => t > cut).OrderBy(t => t).ToList();
        Assert.Equal(oracle, gt);
    }

    // ---------------------------------------------------------------------
    //  CLEAN: DateOnly.ToDateTime(TimeOnly) composition keeps time-of-day.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_ToDateTime_compose_Clean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var d = new DateOnly(2021, 5, 6);
        var t = new TimeOnly(7, 8, 9, 250);
        ctx.Add(new TRow { Id = 1, D = d, T = t, Dt = DateTime.UnixEpoch });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<TRow>().Select(x => x.D.ToDateTime(x.T)).ToList().Single();
        Assert.Equal(d.ToDateTime(t), r);
    }
}
