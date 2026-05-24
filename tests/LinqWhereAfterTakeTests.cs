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
/// Sister probe of <see cref="LinqTakeThenOrderByTests"/> for Where after
/// Take/Skip. LINQ semantics: take the windowed sequence, THEN filter inside
/// the window — the predicate may shrink the result well below the LIMIT.
/// Naive translation that appends the WHERE to the same query produces
/// `WHERE p LIMIT n`, which SQL evaluates as "filter the whole table then
/// limit" — wrong rows, silent.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WatRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Active INTEGER NOT NULL);
            -- Rows: (1,10,0),(2,20,1),(3,30,0),(4,40,1),(5,50,1).
            -- OrderBy(Id).Take(3) → {(1,10,0),(2,20,1),(3,30,0)}.
            -- Where(r.Active == 1) on that window → only (2,20,1).
            -- A naive "WHERE Active=1 LIMIT 3" would return {(2,20,1),(4,40,1),(5,50,1)} —
            -- 3 rows from the full-table-filtered set, not 1 row from the limited window.
            INSERT INTO WatRow VALUES (1,10,0),(2,20,1),(3,30,0),(4,40,1),(5,50,1);
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
    public async Task Where_after_take_either_filters_inside_window_or_throws_with_actionable_pin()
    {
        System.Exception? caught = null;
        WatRow[]? result = null;
        try
        {
            result = (await _ctx.Query<WatRow>()
                .OrderBy(r => r.Id)
                .Take(3)
                .Where(r => r.Active == 1)
                .ToListAsync()).ToArray();
        }
        catch (System.Exception ex)
        {
            caught = ex;
        }

        if (caught != null)
        {
            // Acceptable: throw with pointing-at-workaround message (mirror of bca0523).
            Assert.IsType<NormUnsupportedFeatureException>(caught);
            Assert.Contains("Where", caught.Message, System.StringComparison.Ordinal);
            Assert.Contains("Take", caught.Message, System.StringComparison.Ordinal);
            return;
        }

        // Acceptable: correctly filtered the window — only (2,20,1) survives.
        Assert.NotNull(result);
        var dump = string.Join(", ", result!.Select(r => $"({r.Id},{r.Score},{r.Active})"));
        Assert.True(result.Length == 1, $"Expected 1 row, got {result.Length}: [{dump}] — likely the silent-wrongness bug (filter ran on full table, then LIMIT applied).");
        Assert.Equal(2, result[0].Id);
    }

    [Table("WatRow")]
    public sealed class WatRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public int Active { get; set; }
    }
}
