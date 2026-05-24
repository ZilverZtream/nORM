using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the classic <c>GroupBy(p =&gt; p.CreatedAt.Date)</c> bucket-by-day
/// idiom. Routing: the GroupBy key selector visits the DateTime.Date member
/// access; it should lower to <c>date(col)</c> and the GROUP BY clause should
/// emit that SQL — not the raw column. Silent-wrongness shape: if <c>.Date</c>
/// is dropped, every distinct timestamp becomes its own group, so two rows on
/// the same calendar day (different times) appear as separate buckets instead
/// of a single one.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByDateTimeDateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GbdRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT NOT NULL, Amount INTEGER NOT NULL);
            -- Two days, three rows on 2024-01-15 (different times), two rows on 2024-01-16.
            -- Expected: 2 groups: 2024-01-15 (count 3, sum 30+50+70=150), 2024-01-16 (count 2, sum 20+40=60).
            -- Silent-wrongness check: if .Date drops, we'd get 5 separate groups (one per timestamp).
            INSERT INTO GbdRow VALUES
              (1, '2024-01-15 08:00:00', 30),
              (2, '2024-01-15 14:30:00', 50),
              (3, '2024-01-15 22:45:00', 70),
              (4, '2024-01-16 09:00:00', 20),
              (5, '2024-01-16 17:00:00', 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_datetime_date_buckets_rows_by_calendar_day_count_only()
    {
        var rows = (await _ctx.Query<GbdRow>()
            .GroupBy(p => p.CreatedAt.Date)
            .Select(g => new { Day = g.Key, Count = g.Count() })
            .OrderBy(r => r.Day)
            .ToListAsync())
            .Select(r => (r.Day, r.Count))
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(new DateTime(2024, 1, 15), rows[0].Day);
        Assert.Equal(3, rows[0].Count);
        Assert.Equal(new DateTime(2024, 1, 16), rows[1].Day);
        Assert.Equal(2, rows[1].Count);
    }

    [Fact]
    public async Task GroupBy_datetime_date_buckets_rows_with_sum_aggregate()
    {
        var rows = (await _ctx.Query<GbdRow>()
            .GroupBy(p => p.CreatedAt.Date)
            .Select(g => new { Day = g.Key, Total = g.Sum(x => x.Amount) })
            .OrderBy(r => r.Day)
            .ToListAsync())
            .Select(r => (r.Day, r.Total))
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(new DateTime(2024, 1, 15), rows[0].Day);
        Assert.Equal(150, rows[0].Total);
        Assert.Equal(new DateTime(2024, 1, 16), rows[1].Day);
        Assert.Equal(60, rows[1].Total);
    }

    [Table("GbdRow")]
    public sealed class GbdRow
    {
        [Key] public int Id { get; set; }
        public DateTime CreatedAt { get; set; }
        public int Amount { get; set; }
    }
}
