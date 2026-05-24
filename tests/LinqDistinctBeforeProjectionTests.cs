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
/// Pins <c>Distinct()</c> followed by a projection. nORM (matching EF Core's
/// behaviour) flattens `Query&lt;T&gt;().Distinct().Select(x =&gt; new { ... })`
/// to `SELECT DISTINCT &lt;proj-cols&gt; FROM table` — the Distinct applies to
/// the projected shape rather than the entity shape. Locks this in as
/// documented behaviour so a future refactor doesn't silently break it (or
/// flip it the other way).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctBeforeProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DbpRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Stock INTEGER NOT NULL);
            -- Five rows but only 4 distinct (entity-shape includes Id so all entities are
            -- distinct). The projection (Category, Stock) has duplicates within categories.
            INSERT INTO DbpRow VALUES
                (1, 'Books',  10),
                (2, 'Books',  10),
                (3, 'Music',  5),
                (4, 'Games',  20),
                (5, 'Games',  20);
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
    public async Task Distinct_then_select_flattens_to_select_distinct_over_projection()
    {
        // 5 entities → projection pairs: Books/10 (×2), Music/5, Games/20 (×2).
        // nORM flattens to `SELECT DISTINCT Category, Stock FROM table` → 3 distinct pairs.
        var pairs = (await _ctx.Query<DbpRow>()
            .Distinct()
            .Select(r => new { r.Category, r.Stock })
            .ToListAsync())
            .OrderBy(p => p.Category).ThenBy(p => p.Stock).ToArray();

        Assert.Equal(3, pairs.Length);
        Assert.Equal(("Books", 10), (pairs[0].Category, pairs[0].Stock));
        Assert.Equal(("Games", 20), (pairs[1].Category, pairs[1].Stock));
        Assert.Equal(("Music", 5),  (pairs[2].Category, pairs[2].Stock));
    }

    [Table("DbpRow")]
    public sealed class DbpRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Stock { get; set; }
    }
}
