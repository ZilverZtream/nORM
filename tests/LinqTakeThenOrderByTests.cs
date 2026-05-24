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
/// Pins <c>OrderBy(a).Take(n).OrderBy(b)</c> — the canonical
/// "take top-N by one criterion, then re-sort the window by another" shape.
/// LINQ semantics: take the first n rows by the first ordering, then
/// resort them by the second key. SQL idiom: subquery wrap
/// <c>SELECT * FROM (SELECT ... ORDER BY a LIMIT n) AS T0 ORDER BY b</c>.
/// Silent-wrongness risk if nORM emits a single flat ORDER BY (which would
/// just resort the limited window globally) or drops the inner OrderBy.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTakeThenOrderByTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TtoRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Name TEXT NOT NULL);
            -- 5 rows: by Score DESC the top 3 are (40,'d'), (30,'a'), (20,'b').
            -- Resorted by Name ASC: 'a','b','d'.
            INSERT INTO TtoRow VALUES
                (1,30,'a'),
                (2,20,'b'),
                (3,10,'e'),
                (4,40,'d'),
                (5, 5,'c');
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
    public async Task OrderBy_after_take_throws_with_workaround_hint_for_silent_wrongness()
    {
        // Pre-fix: silently appended the outer ORDER BY to the flat list and emitted
        // `ORDER BY Score DESC, Name ASC LIMIT 3` — wrong rows (full-table joint sort
        // then limit) instead of top-3-by-Score then resort. Pin now requires the
        // translator to throw with the materialize / ThenBy workaround.
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await _ctx.Query<TtoRow>()
                .OrderByDescending(r => r.Score)
                .Take(3)
                .OrderBy(r => r.Name)
                .ToListAsync();
        });
        Assert.Contains("OrderBy", ex.Message, System.StringComparison.Ordinal);
        Assert.Contains("Take", ex.Message, System.StringComparison.Ordinal);
        Assert.Contains("ThenBy", ex.Message, System.StringComparison.Ordinal);
        Assert.Contains("client-side", ex.Message, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OrderBy_then_thenby_then_take_works_as_joint_sort_limit()
    {
        // The documented ThenBy workaround — picks top-3 by (Score DESC, Name ASC) jointly.
        // With our data, Score DESC orders give: (40,d), (30,a), (20,b), (10,e), (5,c).
        // Top-3 by Score DESC = {d, a, b}. (Score-ties don't exist so Name is irrelevant.)
        var names = (await _ctx.Query<TtoRow>()
            .OrderByDescending(r => r.Score)
            .ThenBy(r => r.Name)
            .Take(3)
            .ToListAsync())
            .Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "d", "a", "b" }, names);
    }

    [Table("TtoRow")]
    public sealed class TtoRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
