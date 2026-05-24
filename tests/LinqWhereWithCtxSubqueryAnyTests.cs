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
/// Pins <c>Where(p =&gt; ctx.Query&lt;Other&gt;().Any(o =&gt; o.Id == p.Id))</c>
/// — an EXISTS-style filter expressed via a ctx-captured subquery rather
/// than a navigation property. Unlike the same shape in a PROJECTION
/// (dad1fec, which throws because the materialiser can't build the column),
/// in a Where predicate nORM correctly emits a correlated
/// <c>WHERE EXISTS(SELECT 1 FROM Other o WHERE o.Id = p.Id)</c> subquery.
/// This is a useful "semi-join" filter idiom — pin so it stays working.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereWithCtxSubqueryAnyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WcsLeft  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE WcsRight (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL);
            INSERT INTO WcsLeft  VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D');
            INSERT INTO WcsRight VALUES (2,'x'),(4,'y');
            -- Left rows with matching Right row by Id = {2, 4}.
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
    public async Task Where_with_ctx_subquery_any_emits_correlated_exists_and_returns_matching_left_rows()
    {
        var ctxLocal = _ctx;
        var rows = (await _ctx.Query<WcsLeft>()
            .Where(p => ctxLocal.Query<WcsRight>().Any(o => o.Id == p.Id))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4 }, rows);
    }

    [Fact]
    public async Task Where_with_ctx_subquery_any_negated_returns_non_matching_left_rows()
    {
        // `WHERE NOT EXISTS(...)` form — left rows {1, 3} have no Right match.
        var ctxLocal = _ctx;
        var rows = (await _ctx.Query<WcsLeft>()
            .Where(p => !ctxLocal.Query<WcsRight>().Any(o => o.Id == p.Id))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3 }, rows);
    }

    [Table("WcsLeft")]
    public sealed class WcsLeft
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("WcsRight")]
    public sealed class WcsRight
    {
        [Key] public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
