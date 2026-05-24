using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>Query&lt;L&gt;().Where(predicate).GroupJoin(Query&lt;R&gt;(), …)</c> —
/// the outer source has a WHERE filter chained BEFORE the GroupJoin. The emitted
/// SQL must apply the WHERE to the LEFT side of the LEFT JOIN so the filter narrows
/// the outer rows BEFORE the join collects right-side groups. Silent-wrongness risk:
/// if the WHERE is dropped or applied AFTER the LEFT JOIN with NULLed right rows
/// (post-join filter), the result set differs.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupJoinSourceWhereTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GjwParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Active INTEGER NOT NULL);
            CREATE TABLE GjwChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO GjwParent VALUES (1,'Alice',1),(2,'Bob',0),(3,'Carol',1);
            INSERT INTO GjwChild VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1'),(4,3,'c1');
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
    public async Task GroupJoin_with_outer_where_filters_left_side_before_join()
    {
        // Only Active=1 parents (Alice, Carol) — Bob (Active=0) must be excluded.
        // Expected groups: Alice → [a1, a2], Carol → [c1]. (Bob and his child b1 dropped.)
        var rows = (await _ctx.Query<GjwParent>()
            .Where(p => p.Active == 1)
            .GroupJoin(
                _ctx.Query<GjwChild>(),
                p => p.Id,
                c => c.ParentId,
                (p, cs) => new { Parent = p.Name, ChildTags = cs.Select(c => c.Tag).ToList() })
            .ToListAsync())
            .OrderBy(r => r.Parent)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].Parent);
        Assert.Equal(new[] { "a1", "a2" }, rows[0].ChildTags.OrderBy(t => t).ToArray());
        Assert.Equal("Carol", rows[1].Parent);
        Assert.Equal(new[] { "c1" }, rows[1].ChildTags.ToArray());
    }

    [Fact]
    public async Task GroupJoin_then_orderby_on_parent_field_returns_rows_in_parent_order()
    {
        // Verify the LEFT JOIN materialiser respects an ORDER BY on the outer (parent) field
        // applied AFTER the GroupJoin. Without correct outer-aliased ORDER BY, the row order
        // would be undefined and the assertion below would flap.
        var rows = (await _ctx.Query<GjwParent>()
            .GroupJoin(
                _ctx.Query<GjwChild>(),
                p => p.Id,
                c => c.ParentId,
                (p, cs) => new { Parent = p.Name, Count = cs.Count() })
            .OrderBy(r => r.Parent)
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Alice", rows[0].Parent); Assert.Equal(2, rows[0].Count);
        Assert.Equal("Bob",   rows[1].Parent); Assert.Equal(1, rows[1].Count);
        Assert.Equal("Carol", rows[2].Parent); Assert.Equal(1, rows[2].Count);
    }

    [Table("GjwParent")]
    public sealed class GjwParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Active { get; set; }
    }

    [Table("GjwChild")]
    public sealed class GjwChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
