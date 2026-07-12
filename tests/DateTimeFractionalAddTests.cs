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
/// .NET DateTime.AddDays/AddHours/AddMinutes/AddSeconds/AddMilliseconds take a double
/// and round the delta to the nearest millisecond (DateTime.Add(value, scale) semantics).
/// SQL date-arithmetic primitives disagree: SQLite's datetime() truncates the result to
/// whole seconds (dropping both the fractional shift AND any sub-second fraction the
/// original value had). These tests pin the .NET semantics on the SQLite provider.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeFractionalAddTests
{
    [Table("FracAdd_Event")]
    private class Event
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }

    private static readonly DateTime Whole = new DateTime(2020, 1, 1, 6, 30, 15);
    private static readonly DateTime Fractional = new DateTime(2020, 3, 15, 23, 59, 59).AddMilliseconds(500);

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FracAdd_Event (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Stamp TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Event { Stamp = Whole });
        ctx.Add(new Event { Stamp = Fractional });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return (cn, ctx);
    }

    private static void AssertParity(Func<IQueryable<Event>, System.Collections.Generic.List<DateTime>> query)
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var reference = new[] { new Event { Id = 1, Stamp = Whole }, new Event { Id = 2, Stamp = Fractional } };
        var expected = query(reference.AsQueryable());
        var actual = query(ctx.Query<Event>());
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AddDays_with_fractional_argument_shifts_by_exact_fraction()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddDays(1.5)).ToList());

    [Fact]
    public void AddHours_with_negative_fractional_argument()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddHours(-2.5)).ToList());

    [Fact]
    public void AddMinutes_with_fractional_argument()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddMinutes(90.5)).ToList());

    [Fact]
    public void AddSeconds_with_fractional_argument_keeps_subsecond_result()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddSeconds(1.25)).ToList());

    [Fact]
    public void AddMilliseconds_rounds_to_nearest_millisecond_like_dotnet()
        // .NET AddMilliseconds(2.7) adds 3ms (rounds); exact-fraction SQL adds 2.7ms.
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddMilliseconds(2.7)).ToList());

    [Fact]
    public void Whole_unit_add_preserves_existing_subsecond_fraction()
        // AddDays(1) on a value carrying .500 must keep the fraction; SQLite's
        // datetime() truncates output to whole seconds.
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddDays(1)).ToList());

    [Fact]
    public void AddMonths_preserves_existing_subsecond_fraction()
        => AssertParity(q => q.OrderBy(e => e.Id).Select(e => e.Stamp.AddMonths(1)).ToList());

    [Fact]
    public void Fractional_add_in_predicate_matches_dotnet_rows()
    {
        var cutoff = new DateTime(2020, 1, 2, 18, 0, 0);
        AssertParity(q => q.Where(e => e.Stamp.AddDays(1.5) > cutoff).OrderBy(e => e.Id).Select(e => e.Stamp).ToList());
    }
}
