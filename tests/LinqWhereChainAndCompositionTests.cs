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
/// Pins <c>Where(p1).Where(p2)</c> — two consecutive Where calls that LINQ
/// semantics define as the AND-conjunction of both predicates. nORM should
/// emit a single WHERE clause with `(p1) AND (p2)`. Silent-wrongness risk
/// if the second Where overwrites the first, or if either is dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereChainAndCompositionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WccRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount REAL NOT NULL);
            INSERT INTO WccRow VALUES
                (1, 'A', 10.0),
                (2, 'A', 50.0),
                (3, 'A', 99.0),
                (4, 'B', 10.0),
                (5, 'B', 99.0);
            -- Category='A' AND Amount > 25 → rows 2 and 3.
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
    public async Task Where_chain_ands_both_predicates_into_one_where_clause()
    {
        var rows = (await _ctx.Query<WccRow>()
            .Where(r => r.Category == "A")
            .Where(r => r.Amount > 25)
            .OrderBy(r => r.Id)
            .ToListAsync())
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2, rows[0].Id); Assert.Equal(50.0, rows[0].Amount);
        Assert.Equal(3, rows[1].Id); Assert.Equal(99.0, rows[1].Amount);
    }

    [Fact]
    public async Task Where_chain_with_three_predicates_ands_all_of_them()
    {
        // Adding a third Where narrows further. Amount < 99 leaves only row 2.
        var rows = (await _ctx.Query<WccRow>()
            .Where(r => r.Category == "A")
            .Where(r => r.Amount > 25)
            .Where(r => r.Amount < 99)
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    [Table("WccRow")]
    public sealed class WccRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
