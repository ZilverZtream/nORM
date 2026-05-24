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
/// Sister of <see cref="LinqGroupJoinSourceWhereTests"/> for Where applied AFTER the
/// GroupJoin on a projected member. Same root question as f2bf6f2: does the
/// <c>_groupJoinResultSelector</c> channel that ExpandProjection now falls through
/// to also cover Where (not just OrderBy)? Both translators share the
/// ExpandProjection helper, so the fix should automatically apply — but if the
/// WhereTranslator routes differently or registers different parameters, this
/// would expose it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupJoinWhereOnProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GjwpParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE GjwpChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO GjwpParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            INSERT INTO GjwpChild VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1'),(4,3,'c1');
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
    public async Task GroupJoin_then_where_on_projected_parent_field_filters_outer_rows()
    {
        // GroupJoin then Where on `r.ParentName` should narrow to just Alice/Bob.
        var rows = (await _ctx.Query<GjwpParent>()
            .GroupJoin(
                _ctx.Query<GjwpChild>(),
                p => p.Id,
                c => c.ParentId,
                (p, cs) => new { ParentName = p.Name, ChildTags = cs.Select(c => c.Tag).ToList() })
            .Where(r => r.ParentName == "Alice" || r.ParentName == "Bob")
            .ToListAsync())
            .OrderBy(r => r.ParentName)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].ParentName);
        Assert.Equal(new[] { "a1", "a2" }, rows[0].ChildTags.OrderBy(t => t).ToArray());
        Assert.Equal("Bob",   rows[1].ParentName);
        Assert.Equal(new[] { "b1" },         rows[1].ChildTags.ToArray());
    }

    [Table("GjwpParent")]
    public sealed class GjwpParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("GjwpChild")]
    public sealed class GjwpChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
