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
/// Greatest-N-per-group: projecting a value from the single top-ordered row of each group,
/// e.g. "the amount of each customer's latest order". The projected column differs from the
/// sort key, so it cannot be expressed with Min/Max — it requires a correlated single-row
/// subquery. Verifies correlation (each group gets its own row, not a global one), ordering
/// direction, and First vs Last semantics.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GroupByGreatestNPerGroupTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GnOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, OrderDate INTEGER NOT NULL, Amount INTEGER NOT NULL);
            -- Customer 1: dates 1/2/3 -> amounts 10/20/30 (latest=30, earliest=10)
            -- Customer 2: dates 4/5   -> amounts 40/50    (latest=50, earliest=40)
            INSERT INTO GnOrder VALUES
              (1, 1, 1, 10),
              (2, 1, 3, 30),
              (3, 1, 2, 20),
              (4, 2, 5, 50),
              (5, 2, 4, 40);
            CREATE TABLE GnRegionRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO GnRegionRow VALUES
              (1, 'NA',   90),
              (2, 'NA',   60),
              (3, 'EMEA', 70);
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
    public async Task Latest_order_amount_per_customer_via_OrderByDescending_First_member()
    {
        var rows = (await _ctx.Query<GnOrder>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { CustomerId = g.Key, LatestAmount = g.OrderByDescending(o => o.OrderDate).First().Amount })
            .ToListAsync())
            .ToDictionary(r => r.CustomerId, r => r.LatestAmount);

        Assert.Equal(30, rows[1]); // customer 1's newest order (date 3) is amount 30, NOT the global newest
        Assert.Equal(50, rows[2]);
    }

    [Fact]
    public async Task Latest_order_amount_per_customer_via_Select_First()
    {
        var rows = (await _ctx.Query<GnOrder>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { CustomerId = g.Key, LatestAmount = g.OrderByDescending(o => o.OrderDate).Select(o => o.Amount).First() })
            .ToListAsync())
            .ToDictionary(r => r.CustomerId, r => r.LatestAmount);

        Assert.Equal(30, rows[1]);
        Assert.Equal(50, rows[2]);
    }

    [Fact]
    public async Task Earliest_order_amount_per_customer_via_OrderBy_First()
    {
        var rows = (await _ctx.Query<GnOrder>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { CustomerId = g.Key, EarliestAmount = g.OrderBy(o => o.OrderDate).First().Amount })
            .ToListAsync())
            .ToDictionary(r => r.CustomerId, r => r.EarliestAmount);

        Assert.Equal(10, rows[1]); // customer 1's oldest order (date 1) is amount 10
        Assert.Equal(40, rows[2]);
    }

    [Fact]
    public async Task Last_of_ascending_matches_latest_via_OrderBy_Last()
    {
        var rows = (await _ctx.Query<GnOrder>()
            .GroupBy(o => o.CustomerId)
            .Select(g => new { CustomerId = g.Key, LatestAmount = g.OrderBy(o => o.OrderDate).Last().Amount })
            .ToListAsync())
            .ToDictionary(r => r.CustomerId, r => r.LatestAmount);

        // Last() of ascending order == First() of descending order == latest.
        Assert.Equal(30, rows[1]);
        Assert.Equal(50, rows[2]);
    }

    [Fact]
    public async Task Top_scoring_id_per_region_via_string_key_and_Select_FirstOrDefault()
    {
        // String group key + Select(...).FirstOrDefault(): the correlation predicate is on a
        // text column, and the projected Id is unrelated to the Score sort key.
        var rows = (await _ctx.Query<GnRegionRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, TopId = g.OrderByDescending(r => r.Score).Select(r => r.Id).FirstOrDefault() })
            .ToListAsync())
            .ToDictionary(r => r.Region, r => r.TopId);

        Assert.Equal(1, rows["NA"]);   // NA's highest score (90) is row Id 1
        Assert.Equal(3, rows["EMEA"]);
    }

    [Table("GnOrder")]
    public sealed class GnOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int OrderDate { get; set; }
        public int Amount { get; set; }
    }

    [Table("GnRegionRow")]
    public sealed class GnRegionRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = "";
        public int Score { get; set; }
    }
}
