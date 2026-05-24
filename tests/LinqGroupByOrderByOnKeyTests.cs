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
/// Sister of <see cref="LinqGroupByHavingMultiAggregateTests"/> for OrderBy.
/// Pins <c>GroupBy(s => s.Category).Select(g => new {Category = g.Key, Total = g.Sum(...)})
/// .OrderBy(x => x.Category)</c>. After SelectTranslator's 3-arg rewrite, `x.Category`
/// collapses to the bare key parameter `k`. WhereTranslator already registers every
/// projection-lambda parameter as a grouping key so the visitor's <c>VisitParameter</c>
/// fix (e72ca37) fires there — OrderByTranslator must do the same, otherwise the
/// visitor falls through to the base ParameterExpression path which emits NOTHING and
/// SQLite throws "near 'ORDER': syntax error" (empty ORDER BY column).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByOrderByOnKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GokSale (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount REAL NOT NULL);
            INSERT INTO GokSale VALUES
                (1, 'Charlie', 30.0),
                (2, 'Charlie', 20.0),
                (3, 'Alpha',   10.0),
                (4, 'Alpha',    5.0),
                (5, 'Bravo',   50.0);
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
    public async Task OrderBy_on_bare_key_after_groupby_aggregate_projection_returns_rows_in_key_order()
    {
        // Three categories: Alpha (15), Bravo (50), Charlie (50). OrderBy Category ASC →
        // Alpha, Bravo, Charlie (alphabetical).
        var rows = (await _ctx.Query<GokSale>()
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Category)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Alpha",   rows[0].Category);
        Assert.Equal("Bravo",   rows[1].Category);
        Assert.Equal("Charlie", rows[2].Category);
    }

    [Table("GokSale")]
    public sealed class GokSale
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
