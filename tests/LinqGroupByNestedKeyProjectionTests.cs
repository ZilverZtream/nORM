using System;
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
/// Pins server-side translation + materialization of
/// <c>GroupBy(x =&gt; new { x.A, x.B }).Select(g =&gt; new { Key = g.Key, Count = g.Count() })</c>.
/// The composite key is projected as a NESTED anonymous type (g.Key kept whole rather
/// than flattened into top-level members). nORM must emit one SELECT column per composite
/// key member and reconstruct the nested anonymous type at materialization time.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByNestedKeyProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NkRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL, V INTEGER NOT NULL);
            INSERT INTO NkRow VALUES
                (1,'x',1,100),
                (2,'x',1,200),
                (3,'x',1,300),
                (4,'x',2,400),
                (5,'x',2,500),
                (6,'y',1,600);
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
    public async Task Composite_key_projected_as_nested_anonymous_type_with_aggregate()
    {
        var groups = (await _ctx.Query<NkRow>()
            .GroupBy(r => new { r.A, r.B })
            .Select(g => new { Key = g.Key, Count = g.Count(), Total = g.Sum(r => r.V) })
            .ToListAsync())
            .OrderBy(x => x.Key.A).ThenBy(x => x.Key.B).ToArray();

        Assert.Equal(3, groups.Length);
        Assert.Equal(("x", 1, 3, 600), (groups[0].Key.A, groups[0].Key.B, groups[0].Count, groups[0].Total));
        Assert.Equal(("x", 2, 2, 900), (groups[1].Key.A, groups[1].Key.B, groups[1].Count, groups[1].Total));
        Assert.Equal(("y", 1, 1, 600), (groups[2].Key.A, groups[2].Key.B, groups[2].Count, groups[2].Total));
    }

    [Fact]
    public async Task Composite_key_param_form_projected_as_nested_anonymous_type()
    {
        var groups = (await _ctx.Query<NkRow>()
            .GroupBy(r => new { r.A, r.B }, (k, g) => new { Key = k, Count = g.Count() })
            .ToListAsync())
            .OrderBy(x => x.Key.A).ThenBy(x => x.Key.B).ToArray();

        Assert.Equal(3, groups.Length);
        Assert.Equal(("x", 1, 3), (groups[0].Key.A, groups[0].Key.B, groups[0].Count));
        Assert.Equal(("x", 2, 2), (groups[1].Key.A, groups[1].Key.B, groups[1].Count));
        Assert.Equal(("y", 1, 1), (groups[2].Key.A, groups[2].Key.B, groups[2].Count));
    }

    [Table("NkRow")]
    public sealed class NkRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
        public int V { get; set; }
    }
}
