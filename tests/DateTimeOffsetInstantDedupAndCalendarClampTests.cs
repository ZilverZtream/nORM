using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// DateTimeOffset Distinct and GroupBy use INSTANT equality (matching C#: two
/// offsets naming the same moment are equal), and SQLite DateTime.AddMonths clamps
/// month-end overflow like C# (Jan 31 + 1 month = Feb 29, not Mar 2).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeOffsetInstantDedupAndCalendarClampTests
{
    [Table("DtoDedup_Evt")]
    private class Evt
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset At { get; set; }
        public DateTime Day { get; set; }
        public string Tag { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DtoDedup_Evt (Id INTEGER PRIMARY KEY, At TEXT NOT NULL, Day TEXT NOT NULL, Tag TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Evt>().HasKey(e => e.Id)
        });
    }

    private static void Seed(DbContext ctx)
    {
        // Same INSTANT, different offsets: 10:00Z == 12:00+02:00 == 05:00-05:00.
        ctx.Add(new Evt { Id = 1, At = new DateTimeOffset(2024, 6, 1, 12, 0, 0, TimeSpan.FromHours(2)), Day = new DateTime(2024, 1, 31), Tag = "a" });
        ctx.Add(new Evt { Id = 2, At = new DateTimeOffset(2024, 6, 1, 5, 0, 0, TimeSpan.FromHours(-5)), Day = new DateTime(2024, 1, 31), Tag = "b" });
        ctx.Add(new Evt { Id = 3, At = new DateTimeOffset(2024, 6, 1, 10, 0, 0, TimeSpan.Zero), Day = new DateTime(2024, 3, 31), Tag = "c" });
        ctx.Add(new Evt { Id = 4, At = new DateTimeOffset(2024, 6, 2, 10, 0, 0, TimeSpan.Zero), Day = new DateTime(2024, 3, 31), Tag = "d" });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
    }

    [Fact]
    public void dto_distinct_uses_instant_semantics()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        Seed(ctx);
        // LINQ-to-Objects: DateTimeOffset equality is INSTANT equality → 1,2,3 collapse to one.
        var instants = ctx.Query<Evt>().Select(e => e.At).Distinct().ToList();
        Assert.Equal(2, instants.Count);
    }

    [Fact]
    public void dto_groupby_uses_instant_semantics()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        Seed(ctx);
        var groups = ctx.Query<Evt>().GroupBy(e => e.At)
            .Select(g => new { g.Key, N = g.Count() })
            .ToList().OrderBy(g => g.Key).ToList();
        Assert.Equal(2, groups.Count);
        Assert.Equal(3, groups[0].N);
        Assert.Equal(1, groups[1].N);
    }

    [Fact]
    public void sqlite_addmonths_overflow_clamps_like_csharp()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        Seed(ctx);
        // C#: Jan 31 + 1 month = Feb 29 (2024 leap); Mar 31 + 1 month = Apr 30.
        var rows = ctx.Query<Evt>().Where(e => e.Id == 1 || e.Id == 3)
            .Select(e => new { e.Id, Next = e.Day.AddMonths(1) })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new DateTime(2024, 2, 29), rows[0].Next);
        Assert.Equal(new DateTime(2024, 4, 30), rows[1].Next);
    }
}
