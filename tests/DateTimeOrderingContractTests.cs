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
/// Contract for DateTime / DateTimeOffset ordering (Query/LINQ matrix cell).
///
/// Plain <see cref="DateTime"/> orders chronologically matching .NET <c>DateTime.CompareTo</c>: nORM
/// stores it as fixed-width <c>yyyy-MM-dd HH:mm:ss</c> text plus a positional fractional-second suffix,
/// which sorts correctly across sub-second (100ns) differences and the full Min/Max range. It is
/// deliberately NOT wrapped in a numeric coercion (that would overflow <see cref="DateTime.MaxValue"/>'s
/// .9999999 fraction). <see cref="DateTimeOffset"/> orders by UTC INSTANT, matching
/// <c>DateTimeOffset.CompareTo</c> - nORM normalizes the offset-suffixed text to its instant, so mixed
/// offsets sort by the moment they represent, not by wall-clock text. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DateTimeOrderingContractTests
{
    [Table("DtOrdContract")]
    private sealed class DT { [Key] public int Id { get; set; } public DateTime Val { get; set; } }

    [Table("DtoOrdContract")]
    private sealed class DTO { [Key] public int Id { get; set; } public DateTimeOffset Val { get; set; } }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = ddl;
        c.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task OrderBy_datetime_matches_dotnet_including_subsecond_and_boundaries()
    {
        var baseTime = new DateTime(2020, 1, 1, 0, 0, 0);
        var rows = new (int Id, DateTime Val)[]
        {
            (1, baseTime),
            (2, baseTime.AddTicks(5_000_000)),                 // +0.5s
            (3, new DateTime(2019, 12, 31, 23, 59, 59, 999)),
            (4, DateTime.MinValue),
            (5, DateTime.MaxValue),                            // .9999999 fraction
            (6, baseTime.AddTicks(1)),                         // +100ns
            (7, new DateTime(2020, 6, 15, 12, 30, 45).AddTicks(1_230_000)),
        };
        using var ctx = new DbContext(Open("CREATE TABLE DtOrdContract (Id INTEGER PRIMARY KEY, Val TEXT);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new DT { Id = id, Val = val });

        var norm = ((INormQueryable<DT>)ctx.Query<DT>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 4, 3, 1, 6, 2, 7, 5 }, norm);
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);

        // Boundary + sub-second values round-trip exactly (100ns tick fidelity through TEXT storage).
        var back = ((INormQueryable<DT>)ctx.Query<DT>())
            .AsNoTracking().OrderBy(e => e.Id).Select(e => e.Val).ToList();
        Assert.Equal(rows.Select(r => r.Val).ToList(), back);
    }

    [Fact]
    public async Task OrderBy_datetimeoffset_orders_by_utc_instant_not_wallclock()
    {
        var rows = new (int Id, DateTimeOffset Val)[]
        {
            (1, new DateTimeOffset(2020, 1, 1, 12, 0, 0, TimeSpan.FromHours(0))),   // 12:00Z
            (2, new DateTimeOffset(2020, 1, 1, 12, 0, 0, TimeSpan.FromHours(5))),   // 07:00Z  earliest instant
            (3, new DateTimeOffset(2020, 1, 1, 12, 0, 0, TimeSpan.FromHours(-5))),  // 17:00Z  latest instant
            (4, new DateTimeOffset(2020, 1, 1, 10, 0, 0, TimeSpan.FromHours(0))),   // 10:00Z
        };
        using var ctx = new DbContext(Open("CREATE TABLE DtoOrdContract (Id INTEGER PRIMARY KEY, Val TEXT);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new DTO { Id = id, Val = val });

        var norm = ((INormQueryable<DTO>)ctx.Query<DTO>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        // Instant order (07:00Z, 10:00Z, 12:00Z, 17:00Z), matching DateTimeOffset.CompareTo.
        Assert.Equal(new[] { 2, 4, 1, 3 }, norm);
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);

        // Pin the intended contract: NOT wall-clock text order, which would put 10:00 first then the
        // three 12:00 rows by their offset suffix.
        Assert.NotEqual(new[] { 4, 1, 2, 3 }, norm);
    }
}
