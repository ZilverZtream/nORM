using System;
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
/// Pins <c>q.OrderBy(Id).Take(N).OrderBy(other)</c>: the outer OrderBy must
/// re-sort the windowed N rows, not the whole table. Naive translation that
/// folds both OrderBys into a single ORDER BY clause silently sorts the full
/// table by the outer key and then LIMITs — pulling rows from outside the
/// original Take window.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OatRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO OatRow VALUES
                (1, 50),
                (2, 40),
                (3, 30),
                (4, 999),
                (5, 1);
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
    public async Task OrderBy_after_take_resorts_only_the_windowed_rows()
    {
        // Step 1 (inner): OrderBy(Id).Take(3) → rows 1,2,3 (Id ascending) → V values [50,40,30].
        // Step 2 (outer): OrderBy(V) → reorder those 3 by V ascending → [30,40,50] → Ids [3,2,1].
        // Naive translation would emit `... ORDER BY V LIMIT 3` against the full table and
        // return [5,3,2] (smallest V's in the whole table) — silent-wrongness.
        var rows = (await _ctx.Query<OatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .OrderBy(r => r.V)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(3, rows[0].Id);
        Assert.Equal(2, rows[1].Id);
        Assert.Equal(1, rows[2].Id);
    }

    [Fact]
    public async Task OrderBy_after_skip_resorts_only_the_skipped_subset()
    {
        // Skip(2) → rows 3,4,5 → V values [30,999,1].
        // OrderByDescending(V) → [999,30,1] → Ids [4,3,5].
        var rows = (await _ctx.Query<OatRow>()
            .OrderBy(r => r.Id)
            .Skip(2)
            .OrderByDescending(r => r.V)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(4, rows[0].Id);
        Assert.Equal(3, rows[1].Id);
        Assert.Equal(5, rows[2].Id);
    }

    [Fact]
    public async Task OrderBy_after_take_with_intervening_where_resorts_the_filtered_window()
    {
        // OrderBy(Id).Take(3) → rows 1,2,3 (V = 50,40,30); Where(V < 45) keeps rows
        // 2,3; the outer OrderBy(V) re-sorts those → Ids [3,2]. The Take is no longer
        // the immediate source of the outer OrderBy — the whole chain must ride
        // inside the derived-table wrap.
        var rows = (await _ctx.Query<OatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Where(r => r.V < 45)
            .OrderBy(r => r.V)
            .ToListAsync())
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(3, rows[0].Id);
        Assert.Equal(2, rows[1].Id);
    }

    [Fact]
    public async Task Then_by_after_windowed_resort_composes_without_rewrapping()
    {
        // The outer OrderBy wraps the window; ThenBy must extend that ordering, not
        // wrap the window a second time.
        var rows = (await _ctx.Query<OatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .OrderBy(r => r.V % 20)   // 50→10, 40→0, 30→10
            .ThenBy(r => r.Id)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(2, rows[0].Id);  // V%20 == 0
        Assert.Equal(1, rows[1].Id);  // V%20 == 10, lower Id first
        Assert.Equal(3, rows[2].Id);
    }

    [Fact]
    public async Task Where_after_take_with_intervening_select_filters_the_window()
    {
        // OrderBy(Id).Take(3) → rows 1,2,3 (V = 50,40,30); the pass-through
        // AsNoTracking between the window and the Where means the window is no
        // longer the immediate source — the whole chain must ride inside the
        // derived-table wrap so the predicate filters only the windowed rows.
        var rows = (await _ctx.Query<OatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Where(r => r.V >= 40)
            .Where(r => r.V <= 45)
            .ToListAsync())
            .ToArray();
        var row = Assert.Single(rows);
        Assert.Equal(2, row.Id);
    }

    [Table("OatRow")]
    public sealed class OatRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
