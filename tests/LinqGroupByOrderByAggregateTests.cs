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
/// Pins <c>GroupBy(k).Select(g => new { g.Key, Total = g.Sum(...) }).OrderBy(x => x.Total)</c>.
/// The OrderBy references a projected aggregate, not a source column — translator must
/// either reference the aggregate by its SELECT alias or repeat the aggregate expression
/// in the ORDER BY. Silent-wrongness risk if the ORDER BY is dropped or computed against
/// the wrong column.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByOrderByAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GobSale (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount REAL NOT NULL);
            INSERT INTO GobSale VALUES
                (1, 'Books', 10.0),
                (2, 'Books', 20.0),
                (3, 'Music',  5.0),
                (4, 'Games', 100.0),
                (5, 'Games', 100.0),
                (6, 'Games', 100.0);
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
    public async Task GroupBy_then_orderby_on_projected_aggregate_returns_rows_in_aggregate_order()
    {
        // Totals: Music=5, Books=30, Games=300. OrderBy Total ASC → Music, Books, Games.
        var rows = (await _ctx.Query<GobSale>()
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Total)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Music", rows[0].Category);
        Assert.Equal(5.0, rows[0].Total);
        Assert.Equal("Books", rows[1].Category);
        Assert.Equal(30.0, rows[1].Total);
        Assert.Equal("Games", rows[2].Category);
        Assert.Equal(300.0, rows[2].Total);
    }

    [Fact]
    public async Task GroupBy_then_orderby_descending_on_projected_aggregate_returns_largest_first()
    {
        var rows = (await _ctx.Query<GobSale>()
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderByDescending(x => x.Total)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Games", rows[0].Category);
        Assert.Equal(300.0, rows[0].Total);
        Assert.Equal("Books", rows[1].Category);
        Assert.Equal("Music", rows[2].Category);
    }

    [Table("GobSale")]
    public sealed class GobSale
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
