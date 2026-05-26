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
/// Pins compound ordering after a windowed source:
/// <c>OrderBy(a).Take(n).OrderBy(b).ThenBy(c)</c>.
/// The inner <c>OrderBy(a).Take(n)</c> creates a derived table via
/// <see cref="TranslateAfterTakeSkipWindow"/>. The outer <c>OrderBy(b).ThenBy(c)</c>
/// must both reference the derived-table alias, not the original table, so the
/// generated SQL is:
/// <c>SELECT * FROM (SELECT … ORDER BY a LIMIT n) AS __wob0 ORDER BY __wob0.b, __wob0.c</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTakeOrderByThenByTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    // Id  Score  Name
    //  1     30  red
    //  2     20  blue
    //  3     30  green
    //  4     10  blue    ← same Name as row 2
    //  5     20  red     ← same Name as row 1
    //
    // OrderByDescending(Score).Take(4) inner window:
    //   Score=30: rows 1(red), 3(green) by rowid
    //   Score=20: rows 2(blue), 5(red) by rowid
    //   Score=10: row 4(blue) ← excluded by LIMIT 4
    //   Window = {(1,30,red), (3,30,green), (2,20,blue), (5,20,red)}

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TotbRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Name TEXT NOT NULL);
            INSERT INTO TotbRow VALUES
                (1, 30, 'red'),
                (2, 20, 'blue'),
                (3, 30, 'green'),
                (4, 10, 'blue'),
                (5, 20, 'red');
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
    public async Task OrderBy_Take_then_OrderBy_ThenBy_resorts_window_by_compound_key()
    {
        // Window (top-4 by Score DESC): (1,30,red),(3,30,green),(2,20,blue),(5,20,red)
        // Re-sort by Name ASC, ThenBy Score DESC:
        //   blue(2,20) → 1st (only one blue in window)
        //   green(3,30) → 2nd
        //   red tie: (1,30,red) vs (5,20,red) → Score DESC: 30 > 20 → row 1 first
        //   red(1,30) → 3rd, red(5,20) → 4th
        var rows = (await _ctx.Query<TotbRow>()
            .OrderByDescending(r => r.Score)
            .Take(4)
            .OrderBy(r => r.Name)
            .ThenByDescending(r => r.Score)
            .ToListAsync())
            .ToArray();

        Assert.Equal(4, rows.Length);
        // Row 4 (blue,10) must NOT appear — it was excluded by Take(4)
        Assert.DoesNotContain(rows, r => r.Id == 4);
        Assert.Equal(new[] { "blue", "green", "red", "red" }, rows.Select(r => r.Name).ToArray());
        Assert.Equal(new[] { 2, 3, 1, 5 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task OrderBy_Take_then_OrderBy_ThenBy_ascending_both()
    {
        // Window (top-4 by Score DESC): (1,30,red),(3,30,green),(2,20,blue),(5,20,red)
        // Re-sort by Name ASC, ThenBy Score ASC:
        //   blue(2,20) → 1st
        //   green(3,30) → 2nd
        //   red tie: (5,20) vs (1,30) → Score ASC: 20 < 30 → row 5 first
        //   red(5,20) → 3rd, red(1,30) → 4th
        var rows = (await _ctx.Query<TotbRow>()
            .OrderByDescending(r => r.Score)
            .Take(4)
            .OrderBy(r => r.Name)
            .ThenBy(r => r.Score)
            .ToListAsync())
            .ToArray();

        Assert.Equal(4, rows.Length);
        Assert.DoesNotContain(rows, r => r.Id == 4);
        Assert.Equal(new[] { "blue", "green", "red", "red" }, rows.Select(r => r.Name).ToArray());
        Assert.Equal(new[] { 2, 3, 5, 1 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task OrderBy_Take_then_OrderByDescending_ThenBy_mixed_direction()
    {
        // Window (top-4 by Score DESC): (1,30,red),(3,30,green),(2,20,blue),(5,20,red)
        // Re-sort by Name DESC, ThenBy Id ASC:
        //   red(1,30) and red(5,20) — DESC name, tie → Id ASC: row 1 before row 5
        //   green(3,30)
        //   blue(2,20)
        var rows = (await _ctx.Query<TotbRow>()
            .OrderByDescending(r => r.Score)
            .Take(4)
            .OrderByDescending(r => r.Name)
            .ThenBy(r => r.Id)
            .ToListAsync())
            .ToArray();

        Assert.Equal(4, rows.Length);
        Assert.DoesNotContain(rows, r => r.Id == 4);
        Assert.Equal(new[] { "red", "red", "green", "blue" }, rows.Select(r => r.Name).ToArray());
        Assert.Equal(new[] { 1, 5, 3, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("TotbRow")]
    public sealed class TotbRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
