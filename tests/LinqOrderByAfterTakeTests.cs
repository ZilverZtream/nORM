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

    [Table("OatRow")]
    public sealed class OatRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
