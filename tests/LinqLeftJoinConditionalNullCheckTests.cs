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
/// Exercises LEFT JOIN (GroupJoin + SelectMany + DefaultIfEmpty) projections that
/// contain idiomatic null-guards on the inner entity: <c>c == null ? null : c.Tag</c>.
///
/// SQL LEFT JOIN already NULLs unmatched right-side columns, making the guard
/// semantically redundant. The rewriter strips it so ExtractNeededColumns can
/// resolve the column and the materializer correctly reads the right ordinal.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqLeftJoinConditionalNullCheckTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LjcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE LjcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO LjcParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            INSERT INTO LjcChild  VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1');
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
    public async Task Left_join_projection_with_inner_null_check_returns_correct_rows()
    {
        // c == null ? null : c.Tag is the idiomatic EF-style null-guard.
        // LEFT JOIN already NULLs the right side for unmatched rows, so the
        // guard is redundant — the translator strips it so the query works.
        var rows = (await (from p in _ctx.Query<LjcParent>()
                           join c in _ctx.Query<LjcChild>() on p.Id equals c.ParentId into g
                           from c in g.DefaultIfEmpty()
                           select new { Parent = p.Name, Tag = c == null ? null : c.Tag })
                          .ToListAsync())
                  .OrderBy(r => r.Parent).ThenBy(r => r.Tag).ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Contains(rows, r => r.Parent == "Alice" && r.Tag == "a1");
        Assert.Contains(rows, r => r.Parent == "Alice" && r.Tag == "a2");
        Assert.Contains(rows, r => r.Parent == "Bob"   && r.Tag == "b1");
        Assert.Contains(rows, r => r.Parent == "Carol" && r.Tag == null);
    }

    [Fact]
    public async Task Left_join_projection_with_reversed_null_check_returns_correct_rows()
    {
        // c != null ? c.Tag : null — same semantics, reversed order.
        var rows = (await (from p in _ctx.Query<LjcParent>()
                           join c in _ctx.Query<LjcChild>() on p.Id equals c.ParentId into g
                           from c in g.DefaultIfEmpty()
                           select new { Parent = p.Name, Tag = c != null ? c.Tag : null })
                          .ToListAsync())
                  .OrderBy(r => r.Parent).ThenBy(r => r.Tag).ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Contains(rows, r => r.Parent == "Carol" && r.Tag == null);
        Assert.Contains(rows, r => r.Parent == "Alice" && r.Tag == "a1");
    }

    [Table("LjcParent")]
    public sealed class LjcParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("LjcChild")]
    public sealed class LjcChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
