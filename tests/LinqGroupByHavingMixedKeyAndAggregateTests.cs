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
/// Pins <c>GroupBy(k).Select(g => new {Cat=g.Key, Total=g.Sum(...)})
/// .Where(x => x.Cat == "EU" &amp;&amp; x.Total &gt; 50)</c>. The predicate
/// mixes a KEY reference and an AGGREGATE reference in a single boolean
/// expression — both need to translate inside HAVING. Combines the
/// VisitParameter group-key emit fix (e72ca37) with the aggregate-in-HAVING
/// fix (418d974). Silent-wrongness risk if either half short-circuits and
/// emits empty SQL on one side of the AND.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByHavingMixedKeyAndAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GbkSale (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Amount REAL NOT NULL);
            -- Per-region totals: EU=90 (matches both), US=30 (matches Region only), AS=60 (matches Total only).
            INSERT INTO GbkSale VALUES
                (1, 'EU', 30.0),
                (2, 'EU', 30.0),
                (3, 'EU', 30.0),
                (4, 'US', 30.0),
                (5, 'AS', 30.0),
                (6, 'AS', 30.0);
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
    public async Task Where_with_key_equality_and_aggregate_comparison_routes_both_through_having()
    {
        // Only EU satisfies BOTH: Region == "EU" AND Total > 50.
        var rows = (await _ctx.Query<GbkSale>()
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .Where(x => x.Region == "EU" && x.Total > 50)
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal("EU", rows[0].Region);
        Assert.Equal(90.0, rows[0].Total);
    }

    [Fact]
    public async Task OrderBy_bare_key_then_thenby_aggregate_emits_both_columns_in_order_by()
    {
        // Sister coverage for 83f4d50: ThenBy uses the same OrderByTranslator dispatch
        // entry as OrderBy, so the grouping-key registration also fires here. Without
        // it, the OrderBy on the bare key would emit ORDER BY  ASC (syntax error) — the
        // ThenBy(aggregate) leg further depends on the aggregate-routing fix from
        // 0f47081. Three regions with equal-key ordering (EU=90, US=30, AS=60) but
        // distinct totals — sort first by region then by total descending.
        var rows = (await _ctx.Query<GbkSale>()
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Region)
            .ThenByDescending(x => x.Total)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("AS", rows[0].Region); Assert.Equal(60.0, rows[0].Total);
        Assert.Equal("EU", rows[1].Region); Assert.Equal(90.0, rows[1].Total);
        Assert.Equal("US", rows[2].Region); Assert.Equal(30.0, rows[2].Total);
    }

    [Table("GbkSale")]
    public sealed class GbkSale
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
