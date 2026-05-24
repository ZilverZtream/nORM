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
/// Pins the <c>Distinct().OrderBy().Take(n)</c> pagination chain over an
/// anonymous projection. Common shape for "top-N distinct categories":
/// the SQL must DISTINCT the projection first, then sort the distinct
/// rows, then limit — not the other way around. A naive translation
/// that LIMITs before applying DISTINCT returns wrong results.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctOrderByTakePaginationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DotRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Stock INTEGER NOT NULL);
            -- 4 distinct categories, with duplicates per category
            INSERT INTO DotRow VALUES
                (1, 'Books',   5),
                (2, 'Books',  10),
                (3, 'Music',   3),
                (4, 'Music',   8),
                (5, 'Games',  20),
                (6, 'Games',  15),
                (7, 'Toys',   12),
                (8, 'Toys',    2);
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
    public async Task Distinct_then_orderby_then_take_returns_top_n_distinct_categories()
    {
        // Distinct categories: Books, Games, Music, Toys (4). Take(2) ordered ascending
        // should return Books and Games.
        var top = (await _ctx.Query<DotRow>()
            .Select(r => r.Category)
            .Distinct()
            .OrderBy(c => c)
            .Take(2)
            .ToListAsync())
            .ToArray();

        Assert.Equal(2, top.Length);
        Assert.Equal("Books", top[0]);
        Assert.Equal("Games", top[1]);
    }

    [Table("DotRow")]
    public sealed class DotRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Stock { get; set; }
    }
}
