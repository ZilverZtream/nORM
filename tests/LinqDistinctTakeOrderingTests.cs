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
/// Pins <c>Select(proj).Distinct().Take(n)</c>. DISTINCT must apply BEFORE
/// LIMIT — otherwise the LIMIT picks N rows from the unfiltered source and
/// then the (in-CLR or post-LIMIT) DISTINCT may not return enough rows.
/// SQL semantics require DISTINCT-in-subquery / DISTINCT before LIMIT.
/// Silent-wrongness risk if the emit order is reversed.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctTakeOrderingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtoRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL);
            -- 10 rows but only 3 distinct categories. Heavy duplication in the first 6 rows
            -- (Books × 4, Music × 2) before any 'Games' row appears. A naive `Take(3)` BEFORE
            -- DISTINCT picks up Books × 3 — DISTINCT-then-Take(3) returns {Books, Music, Games}.
            INSERT INTO DtoRow VALUES
                (1, 'Books'),
                (2, 'Books'),
                (3, 'Books'),
                (4, 'Books'),
                (5, 'Music'),
                (6, 'Music'),
                (7, 'Games'),
                (8, 'Music'),
                (9, 'Games'),
                (10,'Games');
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
    public async Task Distinct_then_take_returns_top_n_distinct_categories_not_n_raw_rows()
    {
        // With OrderBy + Distinct + Take(3), the 3 distinct categories — Books, Games, Music —
        // ordered ascending: {Books, Games, Music}. If LIMIT applied before DISTINCT, the first
        // 3 rows (Books, Books, Books) → DISTINCT → {Books} — only 1 row.
        var top = (await _ctx.Query<DtoRow>()
            .Select(r => r.Category)
            .Distinct()
            .OrderBy(c => c)
            .Take(3)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, top.Length);
        Assert.Equal("Books", top[0]);
        Assert.Equal("Games", top[1]);
        Assert.Equal("Music", top[2]);
    }

    [Table("DtoRow")]
    public sealed class DtoRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
    }
}
