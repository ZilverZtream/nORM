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
/// Sister of <see cref="LinqWhereWithCtxSubqueryAnyTests"/> for the Contains
/// spelling — <c>Where(p =&gt; ctx.Query&lt;Other&gt;().Select(o =&gt; o.Id).Contains(p.Id))</c>.
/// Semantically equivalent to the EXISTS form (semi-join) but written via
/// Contains over a projected key set. SQL translation: `WHERE p.Id IN
/// (SELECT o.Id FROM Other o)`. Silent-wrongness risk if the filter is
/// dropped or the IN subquery binds the wrong column.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereWithCtxSubqueryContainsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WccLeft  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE WccRight (Id INTEGER PRIMARY KEY, RefId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO WccLeft  VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D');
            -- Right has RefId values {2, 4} → Left rows {2, 4} match.
            INSERT INTO WccRight VALUES (10,2,'x'),(11,4,'y');
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
    public async Task Where_with_ctx_subquery_contains_emits_in_subquery_and_returns_matching_left_rows()
    {
        var ctxLocal = _ctx;
        var rows = (await _ctx.Query<WccLeft>()
            .Where(p => ctxLocal.Query<WccRight>().Select(o => o.RefId).Contains(p.Id))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4 }, rows);
    }

    [Fact]
    public async Task Where_with_ctx_subquery_contains_negated_returns_non_matching_left_rows()
    {
        // `WHERE p.Id NOT IN (subquery)` — left rows {1, 3} have no Right reference.
        var ctxLocal = _ctx;
        var rows = (await _ctx.Query<WccLeft>()
            .Where(p => !ctxLocal.Query<WccRight>().Select(o => o.RefId).Contains(p.Id))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3 }, rows);
    }

    [Table("WccLeft")]
    public sealed class WccLeft
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("WccRight")]
    public sealed class WccRight
    {
        [Key] public int Id { get; set; }
        public int RefId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
