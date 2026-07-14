using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>Select(p => new { p.Name, Count = ctx.Query&lt;C&gt;().Count(c => c.Pid == p.Id) })</c> —
/// a correlated scalar subquery in the projection. This is the canonical
/// "parent with child count" shape, common in real applications. The translator
/// must emit a correlated scalar subquery
/// <c>(SELECT COUNT(*) FROM C WHERE C.Pid = T0.Id) AS Count</c> against the
/// outer parent alias. Silent-wrongness risk if the subquery binds the wrong
/// alias (returns child count for the wrong parent) or is dropped entirely.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSubqueryInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SipParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SipChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO SipParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            INSERT INTO SipChild  VALUES (1,1,'a'),(2,1,'b'),(3,1,'c'),(4,2,'d'),(5,2,'e');
            -- Carol has zero children — correlated subquery must produce 0, not drop the row.
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
    public async Task Direct_context_subquery_in_projection_translates_as_correlated_count()
    {
        // ctx.Query<C>() aggregates inside a projection lower to a correlated
        // scalar subquery — no navigation property required. Carol has zero
        // children: the subquery must produce 0, not drop the row.
        var ctxLocal = _ctx;
        var rows = (await _ctx.Query<SipParent>()
                .Select(p => new
                {
                    p.Name,
                    Count = ctxLocal.Query<SipChild>().Count(c => c.ParentId == p.Id)
                })
                .ToListAsync())
            .OrderBy(x => x.Name).ToList();

        Assert.Equal(new[] { ("Alice", 3), ("Bob", 2), ("Carol", 0) },
            rows.Select(x => (x.Name, x.Count)).ToArray());
    }


    [Table("SipParent")]
    public sealed class SipParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("SipChild")]
    public sealed class SipChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
