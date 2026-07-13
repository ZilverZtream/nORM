using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// DateTimeOffset.Add* keeps the offset and shifts the clock time; the double-argument
/// overloads are tick-exact like their DateTime counterparts. On SQLite the stored TEXT
/// carries a trailing '+HH:MM' offset, so the arithmetic must shift the clock portion
/// and re-attach the same offset — datetime()-based forms silently drop the offset and
/// truncate sub-second precision.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeOffsetAddArithmeticTests
{
    [Table("DtoAdd_Event")]
    private class Event
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public DateTimeOffset At { get; set; }
    }

    private static readonly DateTimeOffset Plus5 =
        new DateTimeOffset(2020, 1, 31, 10, 20, 30, TimeSpan.FromHours(5));
    private static readonly DateTimeOffset MinusTwoWithFraction =
        new DateTimeOffset(2020, 3, 15, 23, 59, 59, TimeSpan.FromHours(-2)).AddMilliseconds(500);

    private static void AssertParity<T>(Func<IQueryable<Event>, System.Collections.Generic.List<T>> query)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        using var _ = cn;
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DtoAdd_Event (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    At TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Event { At = Plus5 });
        ctx.Add(new Event { At = MinusTwoWithFraction });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();

        var reference = new[]
        {
            new Event { Id = 1, At = Plus5 },
            new Event { Id = 2, At = MinusTwoWithFraction },
        };
        var expected = query(reference.AsQueryable());
        var actual = query(ctx.Query<Event>());
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AddDays_fractional_keeps_offset_and_is_tick_exact()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddDays(1.5)).ToList());

    [Fact]
    public void AddHours_negative_fractional_keeps_offset()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddHours(-2.5)).ToList());

    [Fact]
    public void AddMinutes_fractional_keeps_offset()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddMinutes(90.5)).ToList());

    [Fact]
    public void AddSeconds_fractional_keeps_subsecond_and_offset()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddSeconds(1.25)).ToList());

    [Fact]
    public void AddMilliseconds_is_tick_exact()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddMilliseconds(2.7)).ToList());

    [Fact]
    public void AddTicks_keeps_offset()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddTicks(7500000)).ToList());

    [Fact]
    public void AddMonths_clamps_day_overflow_and_keeps_offset()
        // Jan 31 + 1 month clamps to Feb 29 (2020 is a leap year).
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddMonths(1)).ToList());

    [Fact]
    public void AddYears_keeps_offset_and_fraction()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.At.AddYears(1)).ToList());

    [Fact]
    public void Fractional_add_predicate_matches_dotnet_rows()
    {
        var cutoff = new DateTimeOffset(2020, 2, 1, 12, 0, 0, TimeSpan.FromHours(5));
        AssertParity(q => q.Where(e => e.At.AddDays(1.5) > cutoff).Select(e => e.Id).OrderBy(i => i).ToList());
    }
}
