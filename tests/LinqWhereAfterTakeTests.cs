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
    public async Task Where_after_take_filters_inside_the_window()
    {
        // OrderBy(Id).Take(3) → rows 1,2,3. Active=1 inside that window → only row 2.
        // Full-table filter would also include rows 4 and 5.
        var rows = (await _ctx.Query<WatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Where(r => r.Active == 1)
            .ToListAsync())
            .OrderBy(r => r.Id)
            .ToArray();
        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    [Fact]
    public async Task Where_after_skip_filters_only_after_offset()
    {
        // Skip(2) → rows 3,4,5. Active=1 matches rows 4 and 5.
        var rows = (await _ctx.Query<WatRow>()
            .OrderBy(r => r.Id)
            .Skip(2)
            .Where(r => r.Active == 1)
            .ToListAsync())
            .OrderBy(r => r.Id)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(4, rows[0].Id);
        Assert.Equal(5, rows[1].Id);
    }

    [Fact]
    public async Task Where_after_take_with_no_window_match_returns_empty()
    {
        // First 2 rows V=10,20. None > 25 (row 3 has V=30 outside the window).
        var rows = (await _ctx.Query<WatRow>()
            .OrderBy(r => r.Id)
            .Take(2)
            .Where(r => r.Score > 25)
            .ToListAsync())
            .ToArray();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Where_after_take_followed_by_OrderBy_uses_derived_table_alias()
    {
        // Take(3) → {1,2,3}. Where(Active=1) inside the window → {2}.
        // Then OrderBy(Score DESC) re-sorts the 1-element result.
        // A stale correlated-param alias (T0 from the outer OrderBy before Take)
        // would produce `no such column: T0.Id` on the outer ORDER BY.
        var rows = await _ctx.Query<WatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Where(r => r.Active == 1)
            .OrderByDescending(r => r.Score)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    [Table("WatRow")]
    public sealed class WatRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public int Active { get; set; }
    }
}
