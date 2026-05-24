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
/// Extends <see cref="LinqWhereWithCtxSubqueryAnyTests"/> / Contains-tests:
/// <c>Where(p =&gt; ctx.Query&lt;Other&gt;().Where(o =&gt; o.IsActive).Any(o =&gt; o.RefId == p.Id))</c>
/// — a filtered semi-join where the subquery has BOTH an inner predicate AND
/// the correlated Any predicate. Three independent silent-wrongness risks:
/// (1) inner Where filter dropped → semi-join over-matches; (2) outer Any
/// predicate dropped → subquery degrades to "any row at all" → uncorrelated;
/// (3) correlation column rebound to wrong side → cross-product matches.
/// SQL shape: <c>WHERE EXISTS(SELECT 1 FROM Other WHERE IsActive AND RefId = p.Id)</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereCtxSubqueryAnyFilteredTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WcafLeft  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE WcafRight (Id INTEGER PRIMARY KEY, RefId INTEGER NOT NULL, IsActive INTEGER NOT NULL);
            INSERT INTO WcafLeft  VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E');
            -- Right references:
            --   (10, RefId=2, IsActive=1)  -- active link to 2
            --   (11, RefId=3, IsActive=0)  -- INACTIVE link to 3  (must NOT match filtered Any)
            --   (12, RefId=4, IsActive=1)  -- active link to 4
            --   (13, RefId=2, IsActive=0)  -- INACTIVE link to 2  (2 still matches via row 10)
            -- Filtered Any → Left rows with at least one ACTIVE right link → {2, 4}.
            -- Unfiltered Any would also include 3 (row 11 exists even if inactive) → {2, 3, 4}.
            INSERT INTO WcafRight VALUES (10,2,1),(11,3,0),(12,4,1),(13,2,0);
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
    public async Task Where_with_ctx_subquery_filtered_any_emits_exists_with_inner_where_and_correlation()
    {
        // If the inner Where(o => o.IsActive) is dropped, Id 3 leaks in (its only
        // right link is inactive). If the Any predicate (o.RefId == p.Id) is dropped,
        // every Left row matches as long as any active Right row exists anywhere.
        var ctxLocal = _ctx;
        var ids = (await _ctx.Query<WcafLeft>()
            .Where(p => ctxLocal.Query<WcafRight>().Where(o => o.IsActive).Any(o => o.RefId == p.Id))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4 }, ids);
    }

    [Fact]
    public async Task Where_with_ctx_subquery_filtered_any_negated_returns_complement()
    {
        // Anti-join over the filtered set → Left rows {1, 3, 5} have NO active right.
        var ctxLocal = _ctx;
        var ids = (await _ctx.Query<WcafLeft>()
            .Where(p => !ctxLocal.Query<WcafRight>().Where(o => o.IsActive).Any(o => o.RefId == p.Id))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3, 5 }, ids);
    }

    [Table("WcafLeft")]
    public sealed class WcafLeft
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("WcafRight")]
    public sealed class WcafRight
    {
        [Key] public int Id { get; set; }
        public int RefId { get; set; }
        public bool IsActive { get; set; }
    }
}
