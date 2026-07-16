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
/// Contract for temporal-type write/read round-trip fidelity (write-path matrix cell:
/// DateTime / DateTimeOffset / DateOnly / TimeOnly / TimeSpan).
///
///  * DateTime round-trips its wall-clock TICKS exactly (full 100ns precision). Its KIND does not
///    round-trip: values written as Utc or Local read back as Unspecified, because the stored text
///    carries no Kind - DESIGN-EXCEPTION, identical to EF Core's SQLite storage. Use DateTimeOffset
///    for instant-preserving semantics.
///  * DateTimeOffset preserves BOTH the ticks and the ORIGINAL OFFSET (+05:00 / -05:00 / Z read back
///    verbatim, not normalized to UTC or local).
///  * DateOnly round-trips exactly incl. MinValue / MaxValue / leap day.
///  * TimeOnly round-trips exactly incl. MaxValue (23:59:59.9999999) and sub-millisecond values.
///  * TimeSpan round-trips exactly incl. negative multi-day durations, single-tick offsets, and
///    TimeSpan.MinValue / MaxValue.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TemporalTypeRoundTripContractTests
{
    [Table("TemporalRtContract")]
    private sealed class T
    {
        [Key] public int Id { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public DateOnly Don { get; set; }
        public TimeOnly Ton { get; set; }
        public TimeSpan Ts { get; set; }
    }

    private static readonly DateTime Instant =
        new DateTime(2020, 6, 15, 10, 30, 45, DateTimeKind.Unspecified).AddTicks(1234567);

    private static readonly (int Id, DateTime Dt, DateTimeOffset Dto, DateOnly Don, TimeOnly Ton, TimeSpan Ts)[] Rows =
    {
        (1, DateTime.SpecifyKind(Instant, DateTimeKind.Utc),
            new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(5)),
            new DateOnly(2020, 2, 29), new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(9999999)),
            new TimeSpan(10, 5, 30, 15) + TimeSpan.FromTicks(1)),
        (2, DateTime.SpecifyKind(Instant, DateTimeKind.Local),
            new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(-5)),
            DateOnly.MinValue, TimeOnly.MinValue,
            -new TimeSpan(9, 23, 59, 59)),
        (3, DateTime.SpecifyKind(Instant, DateTimeKind.Unspecified),
            new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.Zero),
            DateOnly.MaxValue, TimeOnly.MaxValue,
            TimeSpan.MaxValue),
        (4, DateTime.MinValue,
            DateTimeOffset.MinValue,
            new DateOnly(2000, 1, 1), new TimeOnly(0, 0, 0, 0, 1),
            TimeSpan.MinValue),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE TemporalRtContract (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, Dto TEXT NOT NULL, Don TEXT NOT NULL, Ton TEXT NOT NULL, Ts TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in Rows)
            await ctx.InsertAsync(new T { Id = r.Id, Dt = r.Dt, Dto = r.Dto, Don = r.Don, Ton = r.Ton, Ts = r.Ts });
        return ctx;
    }

    [Fact]
    public async Task Ticks_and_values_round_trip_exactly_for_all_five_temporal_types()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();

        for (int i = 0; i < Rows.Length; i++)
        {
            Assert.Equal(Rows[i].Dt.Ticks, back[i].Dt.Ticks);       // 100ns wall-clock fidelity
            Assert.Equal(Rows[i].Dto.Ticks, back[i].Dto.Ticks);
            Assert.Equal(Rows[i].Don, back[i].Don);
            Assert.Equal(Rows[i].Ton.Ticks, back[i].Ton.Ticks);
            Assert.Equal(Rows[i].Ts.Ticks, back[i].Ts.Ticks);
        }
    }

    [Fact]
    public async Task DateTimeOffset_preserves_the_original_offset()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();

        // Not normalized to UTC or local: the written offset comes back verbatim.
        Assert.Equal(TimeSpan.FromHours(5), back[0].Dto.Offset);
        Assert.Equal(TimeSpan.FromHours(-5), back[1].Dto.Offset);
        Assert.Equal(TimeSpan.Zero, back[2].Dto.Offset);
    }

    [Fact]
    public async Task DateTime_kind_normalizes_to_unspecified_with_wall_clock_preserved()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();

        // The stored text carries no Kind, so Utc- and Local-written values read back Unspecified
        // (wall-clock ticks intact) - identical to EF Core's SQLite storage. Use DateTimeOffset for
        // instant-preserving semantics.
        Assert.Equal(DateTimeKind.Unspecified, back[0].Dt.Kind);   // written Utc
        Assert.Equal(DateTimeKind.Unspecified, back[1].Dt.Kind);   // written Local
        Assert.Equal(DateTimeKind.Unspecified, back[2].Dt.Kind);   // written Unspecified
        Assert.Equal(Rows[0].Dt.Ticks, back[0].Dt.Ticks);
    }
}
