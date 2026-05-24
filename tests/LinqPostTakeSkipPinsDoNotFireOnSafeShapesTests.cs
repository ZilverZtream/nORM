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
/// Positive coverage for the post-Take/Skip silent-wrongness pin family
/// (bca0523 / 47acc83 / 54c16ae / 4fcd795 / c2cce55 / 3427495 / 3716e13 /
/// f0ccf06 / b4f5ae4). These pins fire on `…Take(n).<op>(…)` — but they
/// MUST NOT fire when the operators come BEFORE the Take, when Take is the
/// terminal, or when ToList simply materialises the window. Each test here
/// exercises a safe shape and verifies it executes without the pin tripping.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqPostTakeSkipPinsDoNotFireOnSafeShapesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Active INTEGER NOT NULL);
            INSERT INTO PsRow VALUES (1,'A',1),(2,'A',0),(3,'B',1),(4,'B',1),(5,'C',0);
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
    public async Task Bare_take_with_tolist_materializes_window_without_tripping_pin()
    {
        var rows = (await _ctx.Query<PsRow>().OrderBy(r => r.Id).Take(3).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 3 }, rows);
    }

    [Fact]
    public async Task Where_before_take_is_pre_paging_filter_and_does_not_trip_pin()
    {
        // Active filter BEFORE Take — Active rows are {1,3,4}; Take 2 → {1,3}.
        var rows = (await _ctx.Query<PsRow>()
            .Where(r => r.Active == 1)
            .OrderBy(r => r.Id)
            .Take(2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3 }, rows);
    }

    [Fact]
    public async Task Orderby_before_take_and_thenby_compose_without_tripping_pin()
    {
        // OrderBy.ThenBy.Take is the canonical sort-then-window. ThenBy is NOT
        // pinned by bca0523 (only top-level OrderBy/OrderByDescending after Take).
        var rows = (await _ctx.Query<PsRow>()
            .OrderBy(r => r.Category)
            .ThenByDescending(r => r.Id)
            .Take(3)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        // Cat A→{2,1}, Cat B→{4,3}, Cat C→{5}. Take 3 = {2,1,4}.
        Assert.Equal(new[] { 2, 1, 4 }, rows);
    }

    [Fact]
    public async Task Distinct_before_take_top_n_distinct_does_not_trip_pin()
    {
        // The canonical top-N-distinct shape — Distinct BEFORE Take is exactly
        // what bca0523/54c16ae's workaround example points at.
        var rows = (await _ctx.Query<PsRow>()
            .Select(r => r.Category)
            .Distinct()
            .OrderBy(c => c)
            .Take(2)
            .ToListAsync())
            .ToArray();
        Assert.Equal(new[] { "A", "B" }, rows);
    }

    [Fact]
    public async Task Groupby_before_take_groups_then_windows_does_not_trip_pin()
    {
        // GroupBy.Select.Take is the canonical group-then-window shape (c2cce55's
        // workaround). Three groups by Category, then Take 2 by Key.
        var rows = (await _ctx.Query<PsRow>()
            .GroupBy(r => r.Category)
            .Select(g => new { Cat = g.Key, Count = g.Count() })
            .OrderBy(x => x.Cat)
            .Take(2)
            .ToListAsync())
            .Select(x => (x.Cat, x.Count))
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(("A", 2), rows[0]);
        Assert.Equal(("B", 2), rows[1]);
    }

    [Fact]
    public async Task Count_without_take_does_not_trip_pin()
    {
        // Bare Count without paging — 4fcd795 must not fire.
        var n = await _ctx.Query<PsRow>().CountAsync();
        Assert.Equal(5, n);
    }

    [Fact]
    public async Task Where_then_take_then_count_via_predicate_overload_works_when_predicate_runs_before_take()
    {
        // Where.Take returning the count via a separate query — the canonical
        // counting pattern. Both queries are safe shapes.
        var matching = await _ctx.Query<PsRow>().Where(r => r.Active == 1).CountAsync();
        Assert.Equal(3, matching);
    }

    [Table("PsRow")]
    public sealed class PsRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Active { get; set; }
    }
}
