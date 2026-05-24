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
/// Pins <c>GroupBy(k).Select(g => new {Key, SumA=g.Sum(a), SumB=g.Sum(b)})
/// .Where(x => x.SumA &gt; x.SumB)</c> — both sides of the HAVING predicate are
/// aggregates over the same group. Should emit
/// <c>HAVING SUM(A) &gt; SUM(B)</c>. Silent-wrongness risk if only one side
/// translates and the other crashes or gets dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByHavingMultiAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GbmRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, A REAL NOT NULL, B REAL NOT NULL);
            -- Per-group totals (A, B):
            --   Alpha: (60, 30)  → SumA > SumB ✓
            --   Bravo: (10, 50)  → SumA < SumB ✗
            --   Charlie: (20, 20) → SumA == SumB ✗ (strict >)
            INSERT INTO GbmRow VALUES
                (1, 'Alpha',   30.0, 10.0),
                (2, 'Alpha',   30.0, 20.0),
                (3, 'Bravo',    5.0, 25.0),
                (4, 'Bravo',    5.0, 25.0),
                (5, 'Charlie', 10.0, 10.0),
                (6, 'Charlie', 10.0, 10.0);
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
    public async Task Where_with_two_aggregate_sides_filters_by_aggregate_comparison()
    {
        var rows = (await _ctx.Query<GbmRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Category = g.Key, SumA = g.Sum(r => r.A), SumB = g.Sum(r => r.B) })
            .Where(x => x.SumA > x.SumB)
            .ToListAsync())
            .OrderBy(r => r.Category)
            .ToArray();
        Assert.Single(rows);
        Assert.Equal("Alpha", rows[0].Category);
        Assert.Equal(60.0, rows[0].SumA);
        Assert.Equal(30.0, rows[0].SumB);
    }

    [Fact]
    public async Task Where_with_only_key_predicate_after_groupby_aggregate_projection_still_filters_correctly()
    {
        // Regression for the widened `isGrouping = _groupBy.Count > 0` check in
        // WhereTranslator: a Where that only touches the group key (no aggregate)
        // still needs to translate correctly when routed through HAVING.
        // Categories: Alpha, Bravo, Charlie. StartsWith("A") matches Alpha only.
        var rows = (await _ctx.Query<GbmRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Category = g.Key, SumA = g.Sum(r => r.A) })
            .Where(x => x.Category.StartsWith("A"))
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal("Alpha", rows[0].Category);
        Assert.Equal(60.0, rows[0].SumA);
    }

    [Table("GbmRow")]
    public sealed class GbmRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double A { get; set; }
        public double B { get; set; }
    }
}
