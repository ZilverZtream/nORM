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
/// Fifth in the post-Take/Skip silent-wrongness family (bca0523 / 47acc83 /
/// 54c16ae / 4fcd795 / this). <c>q.Take(3).GroupBy(...).Select(...)</c>
/// must group only the windowed 3 rows. A naive flat SQL emits
/// <c>SELECT … GROUP BY … LIMIT 3</c> where GROUP BY runs on the full table
/// and LIMIT picks 3 groups, returning wrong group totals.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GbaRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount REAL NOT NULL);
            -- Full-table groups: A→(10+20+30)=60, B→(40+50)=90.
            -- OrderBy(Id).Take(3) → first 3 rows = all A's. Windowed group: A→60. ONE row.
            -- A naive `GROUP BY Category LIMIT 3` against full table = {A:60, B:90} (LIMIT capped at 2 rows).
            INSERT INTO GbaRow VALUES
                (1,'A',10.0),(2,'A',20.0),(3,'A',30.0),
                (4,'B',40.0),(5,'B',50.0);
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
    public async Task GroupBy_after_take_groups_windowed_rows_or_throws_actionable_pin()
    {
        System.Exception? caught = null;
        (string Cat, double Total)[]? result = null;
        try
        {
            result = (await _ctx.Query<GbaRow>()
                .OrderBy(r => r.Id)
                .Take(3)
                .GroupBy(r => r.Category)
                .Select(g => new { Cat = g.Key, Total = g.Sum(r => r.Amount) })
                .ToListAsync())
                .Select(x => (x.Cat, x.Total))
                .ToArray();
        }
        catch (System.Exception ex)
        {
            caught = ex;
        }

        if (caught != null)
        {
            Assert.IsType<NormUnsupportedFeatureException>(caught);
            Assert.Contains("GroupBy", caught.Message, System.StringComparison.Ordinal);
            Assert.Contains("Take", caught.Message, System.StringComparison.Ordinal);
            return;
        }

        // Acceptable: GroupBy ran on the windowed 3 (all A's) → single group A=60.
        Assert.NotNull(result);
        var dump = string.Join(", ", result!.Select(r => $"{r.Cat}={r.Total}"));
        Assert.True(result.Length == 1 && result[0].Cat == "A" && result[0].Total == 60.0,
            $"Expected single group {{A=60}} from windowed top-3, got [{dump}] — likely silent-wrongness (GROUP BY on full table, LIMIT on groups).");
    }

    [Table("GbaRow")]
    public sealed class GbaRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
