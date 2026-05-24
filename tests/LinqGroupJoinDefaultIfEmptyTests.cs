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
/// Pins the canonical LINQ LEFT JOIN idiom
/// <c>from p in Parents join c in Children on p.Id equals c.ParentId into g
/// from c in g.DefaultIfEmpty() select new {p.Name, c?.Tag}</c>.
/// The compiler lowers this to <c>GroupJoin(...)</c> followed by
/// <c>SelectMany(g, (p, c) => ..., DefaultIfEmpty)</c>. Silent-wrongness risk:
/// if the translator treats it as an INNER JOIN (forgetting DefaultIfEmpty),
/// parents with no children are dropped instead of getting a NULL right side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupJoinDefaultIfEmptyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GjdParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE GjdChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO GjdParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Alice: a1, a2. Bob: b1. Carol: NO children (must still appear via LEFT JOIN).
            INSERT INTO GjdChild  VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1');
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
    public async Task GroupJoin_with_defaultifempty_returns_left_join_rows_including_parentless_match()
    {
        var rows = (await (
            from p in _ctx.Query<GjdParent>()
            join c in _ctx.Query<GjdChild>() on p.Id equals c.ParentId into g
            from c in g.DefaultIfEmpty()
            select new { Parent = p.Name, Tag = c.Tag }
        ).ToListAsync())
            .OrderBy(r => r.Parent).ThenBy(r => r.Tag)
            .ToArray();
        var dump = string.Join(" | ", rows.Select(r => $"{r.Parent}={r.Tag ?? "<null>"}"));
        // Expect 4 rows: 3 matched (Alice/a1, Alice/a2, Bob/b1) + 1 unmatched (Carol/null).
        Assert.True(rows.Length == 4, $"Expected 4 rows, got {rows.Length}: [{dump}]");
        var carol = rows.SingleOrDefault(r => r.Parent == "Carol");
        Assert.NotNull(carol);
        Assert.Null(carol!.Tag);
        Assert.Equal(new[] { "a1", "a2" }, rows.Where(r => r.Parent == "Alice").Select(r => r.Tag).OrderBy(t => t).ToArray());
        Assert.Equal(new[] { "b1" },        rows.Where(r => r.Parent == "Bob").Select(r => r.Tag).ToArray());
    }

    [Table("GjdParent")]
    public sealed class GjdParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("GjdChild")]
    public sealed class GjdChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
