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
/// Pins server-side translation of `Select(anon).Distinct().CountAsync()`. The
/// SelectMany / Distinct / Count chain composes through Provider.CreateQuery and
/// has historically produced the wrong IQueryable&lt;TAnon&gt; instance shape,
/// causing the count to be computed against the raw entity stream rather than
/// the distinct projection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctCountTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DcRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL);
            -- 4 rows but only 2 distinct (A, B) pairs: (x,1) appears 3 times, (y,2) appears 1
            INSERT INTO DcRow VALUES (1,'x',1),(2,'x',1),(3,'x',1),(4,'y',2);
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
    public async Task Distinct_over_anonymous_projection_counts_distinct_pairs()
    {
        var count = await _ctx.Query<DcRow>()
            .Select(r => new { r.A, r.B })
            .Distinct()
            .CountAsync();
        // Only 2 distinct (A, B) pairs exist — (x,1) and (y,2).
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Distinct_over_anonymous_projection_long_counts_distinct_pairs()
    {
        var count = await _ctx.Query<DcRow>()
            .Select(r => new { r.A, r.B })
            .Distinct()
            .LongCountAsync();
        Assert.Equal(2L, count);
    }

    [Fact]
    public async Task Distinct_over_anonymous_projection_materializes_distinct_rows()
    {
        var pairs = (await _ctx.Query<DcRow>()
            .Select(r => new { r.A, r.B })
            .Distinct()
            .ToListAsync())
            .OrderBy(p => p.A).ThenBy(p => p.B).ToArray();
        Assert.Equal(2, pairs.Length);
        Assert.Equal(("x", 1), (pairs[0].A, pairs[0].B));
        Assert.Equal(("y", 2), (pairs[1].A, pairs[1].B));
    }

    [Table("DcRow")]
    public sealed class DcRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
    }
}
