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
/// Extends the post-Take/Skip silent-wrongness family probe to Join.
/// <c>q.OrderBy(Id).Take(2).Join(right, …)</c> must join only the windowed
/// 2 rows. A naive flat SQL emits
/// <c>… INNER JOIN right ON … ORDER BY Id LIMIT 2</c> where the JOIN runs
/// against the full table and LIMIT picks 2 rows from the joined result —
/// wrong rows, possibly amplified if join multiplies cardinality.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqJoinAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE JatLeft  (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            CREATE TABLE JatRight (Code TEXT NOT NULL PRIMARY KEY, Tag TEXT NOT NULL);
            -- Differentiating data: the first 2 rows (by Id ASC) have codes that DON'T
            -- exist in Right, so a windowed-then-joined query yields 0 rows. A naive
            -- full-table-join-then-LIMIT query would yield 2 rows (A, B match rows 3 and 4).
            INSERT INTO JatLeft  VALUES (1,'X'),(2,'Y'),(3,'A'),(4,'B'),(5,'C');
            INSERT INTO JatRight VALUES ('A','tagA'),('B','tagB'),('C','tagC');
            -- OrderBy(Id).Take(2).Join(right) — windowed: rows 1,2 (X,Y) have no Right matches → 0 rows.
            -- Naive `INNER JOIN … ORDER BY L.Id LIMIT 2` on full join → 2 rows (A, B).
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
    public async Task Join_after_take_joins_only_windowed_rows_or_throws_actionable_pin()
    {
        System.Exception? caught = null;
        (string Code, string Tag)[]? result = null;
        try
        {
            result = (await _ctx.Query<JatLeft>()
                .OrderBy(l => l.Id)
                .Take(2)
                .Join(_ctx.Query<JatRight>(),
                    l => l.Code,
                    r => r.Code,
                    (l, r) => new { l.Code, r.Tag })
                .ToListAsync())
                .Select(x => (x.Code, x.Tag))
                .ToArray();
        }
        catch (System.Exception ex)
        {
            caught = ex;
        }

        // The pin must fire — silent-wrongness (0 vs 2 rows) is unacceptable.
        Assert.NotNull(caught);
        Assert.IsType<NormUnsupportedFeatureException>(caught);
        Assert.Contains("Join", caught.Message, System.StringComparison.Ordinal);
        Assert.Contains("Take", caught.Message, System.StringComparison.Ordinal);
        Assert.Contains("Contains", caught.Message, System.StringComparison.Ordinal);
    }

    [Table("JatLeft")]
    public sealed class JatLeft
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    [Table("JatRight")]
    public sealed class JatRight
    {
        [Key] public string Code { get; set; } = string.Empty;
        public string Tag { get; set; } = string.Empty;
    }
}
