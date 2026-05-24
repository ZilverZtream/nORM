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
/// Pins <c>GroupBy(k).Select(g => new {Key, Total=g.Sum(...)}).Where(x => x.Total > N)</c>.
/// The Where references a projected aggregate, so it must emit SQL
/// <c>HAVING SUM(Amount) > N</c> — NOT <c>WHERE Amount > N</c> (which would filter
/// before grouping and silently change the result). Silent-wrongness risk: if the
/// predicate is pushed pre-GROUP BY, you get the wrong rows back without any error.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByHavingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GbhSale (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount REAL NOT NULL);
            -- Per-row amounts are all < 50 but Books total = 90, Games total = 150.
            -- A naive `WHERE Amount > 50` pre-GROUP-BY would drop EVERY row.
            INSERT INTO GbhSale VALUES
                (1, 'Books', 30.0),
                (2, 'Books', 30.0),
                (3, 'Books', 30.0),
                (4, 'Music',  5.0),
                (5, 'Music',  5.0),
                (6, 'Games', 50.0),
                (7, 'Games', 50.0),
                (8, 'Games', 50.0);
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
    public async Task Where_after_groupby_aggregate_projection_filters_on_group_total_not_row_value()
    {
        // Totals: Music=10, Books=90, Games=150. Where Total > 50 → {Books, Games}.
        // If naively translated as WHERE Amount > 50, only the 3 Games rows survive →
        // Games total=150 (correct value, wrong reason) but Books would be missing.
        var rows = (await _ctx.Query<GbhSale>()
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .Where(x => x.Total > 50)
            .OrderBy(x => x.Total)
            .ToListAsync())
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Books", rows[0].Category);
        Assert.Equal(90.0, rows[0].Total);
        Assert.Equal("Games", rows[1].Category);
        Assert.Equal(150.0, rows[1].Total);
    }

    [Table("GbhSale")]
    public sealed class GbhSale
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
