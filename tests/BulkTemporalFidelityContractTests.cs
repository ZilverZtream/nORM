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
/// Contract for temporal-type value fidelity through the BULK insert path (bulk-path matrix cell).
///
/// All five temporal types bulk-insert with storage text BYTE-IDENTICAL to the direct path and read
/// back tick-exact: DateTime at full 100ns precision including MaxValue's .9999999 fraction,
/// DateTimeOffset with the ORIGINAL OFFSET preserved through bulk (+05:00 / -05:00 / Z), DateOnly
/// including the leap day and Min/Max, TimeOnly including MaxValue, and TimeSpan including negative
/// multi-day durations and Min/Max.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkTemporalFidelityContractTests
{
    [Table("BulkTemporalFidelity")]
    private sealed class BulkT
    {
        [Key] public int Id { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public DateOnly Don { get; set; }
        public TimeOnly Ton { get; set; }
        public TimeSpan Ts { get; set; }
    }

    [Table("DirTemporalFidelity")]
    private sealed class DirT
    {
        [Key] public int Id { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public DateOnly Don { get; set; }
        public TimeOnly Ton { get; set; }
        public TimeSpan Ts { get; set; }
    }

    private static readonly (int Id, DateTime Dt, DateTimeOffset Dto, DateOnly Don, TimeOnly Ton, TimeSpan Ts)[] Rows =
    {
        (1, new DateTime(2020, 6, 15, 10, 30, 45).AddTicks(1234567),
            new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(5)),
            new DateOnly(2020, 2, 29), new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(9999999)),
            new TimeSpan(10, 5, 30, 15) + TimeSpan.FromTicks(1)),
        (2, DateTime.MaxValue,
            new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(-5)),
            DateOnly.MinValue, TimeOnly.MinValue,
            -new TimeSpan(9, 23, 59, 59)),
        (3, DateTime.MinValue,
            new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.Zero),
            DateOnly.MaxValue, TimeOnly.MaxValue,
            TimeSpan.MinValue),
    };

    private static (SqliteConnection cn, DbContext ctx) Fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkTemporalFidelity (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, Dto TEXT NOT NULL, Don TEXT NOT NULL, Ton TEXT NOT NULL, Ts TEXT NOT NULL);" +
                "CREATE TABLE DirTemporalFidelity  (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, Dto TEXT NOT NULL, Don TEXT NOT NULL, Ton TEXT NOT NULL, Ts TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Bulk_temporal_storage_matches_direct_and_reads_back_tick_exact()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(Rows.Select(r => new BulkT
            {
                Id = r.Id, Dt = r.Dt, Dto = r.Dto, Don = r.Don, Ton = r.Ton, Ts = r.Ts,
            }).ToList());
            foreach (var r in Rows)
                await ctx.InsertAsync(new DirT { Id = r.Id, Dt = r.Dt, Dto = r.Dto, Don = r.Don, Ton = r.Ton, Ts = r.Ts });

            // Storage-text identity for all five temporal columns.
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT COUNT(*) FROM BulkTemporalFidelity a JOIN DirTemporalFidelity b ON a.Id=b.Id AND a.Dt=b.Dt AND a.Dto=b.Dto AND a.Don=b.Don AND a.Ton=b.Ton AND a.Ts=b.Ts;";
                Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
            }

            var back = ((INormQueryable<BulkT>)ctx.Query<BulkT>()).AsNoTracking().OrderBy(t => t.Id).ToList();
            for (int i = 0; i < Rows.Length; i++)
            {
                Assert.Equal(Rows[i].Dt.Ticks, back[i].Dt.Ticks);
                Assert.Equal(Rows[i].Dto.Ticks, back[i].Dto.Ticks);
                Assert.Equal(Rows[i].Dto.Offset, back[i].Dto.Offset);   // offset preserved through bulk
                Assert.Equal(Rows[i].Don, back[i].Don);
                Assert.Equal(Rows[i].Ton.Ticks, back[i].Ton.Ticks);
                Assert.Equal(Rows[i].Ts.Ticks, back[i].Ts.Ticks);
            }
        }
    }
}
