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
/// Pins <c>g.Where(pred).Sum(selector)</c> inside a grouped projection.
/// The natural SQL lowering is a conditional aggregate: SUM(CASE WHEN pred
/// THEN selector ELSE 0 END) — sums only the rows in the group that satisfy
/// the predicate. Without server-side translation the projection either
/// crashes or returns wrong values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupWhereThenAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GwaRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Status TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO GwaRow VALUES
                (1, 'NA',   'Paid',    100),
                (2, 'NA',   'Pending',  50),
                (3, 'NA',   'Paid',     30),
                (4, 'EMEA', 'Paid',    200),
                (5, 'EMEA', 'Pending', 150),
                (6, 'EMEA', 'Pending',  75);
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
    public async Task GroupBy_with_where_then_sum_aggregates_only_filtered_rows()
    {
        // Per-region sum of Amount where Status == "Paid":
        //   NA   → 100 + 30 = 130
        //   EMEA → 200
        var groups = (await _ctx.Query<GwaRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, PaidTotal = g.Where(r => r.Status == "Paid").Sum(r => r.Amount) })
            .ToListAsync())
            .OrderBy(g => g.Region).ToArray();

        Assert.Equal(2, groups.Length);
        Assert.Equal(("EMEA", 200), (groups[0].Region, groups[0].PaidTotal));
        Assert.Equal(("NA",   130), (groups[1].Region, groups[1].PaidTotal));
    }

    [Table("GwaRow")]
    public sealed class GwaRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
