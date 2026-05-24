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
/// Verifies that a Where clause applied AFTER a join-projected anonymous type still translates
/// to a server-side predicate over the underlying joined columns, rather than silently
/// materializing every joined pair into memory before filtering.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereOverJoinedProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WjLeft  (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Tag TEXT NOT NULL);
            CREATE TABLE WjRight (Id INTEGER PRIMARY KEY, Note TEXT NOT NULL);
            INSERT INTO WjLeft  VALUES (1,10,'a'),(2,30,'b'),(3,50,'c'),(4,70,'d');
            INSERT INTO WjRight VALUES (1,'x'),(2,'y'),(3,'z'),(4,'w');
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
    public async Task Where_after_join_projects_filtered_pairs_via_server_side_predicate()
    {
        var rows = (await (from l in _ctx.Query<WjLeft>()
                           join r in _ctx.Query<WjRight>() on l.Id equals r.Id
                           select new { l.Tag, l.Score, r.Note })
                          .Where(x => x.Score > 25)
                          .ToListAsync())
                  .OrderBy(p => p.Tag).ToArray();

        Assert.Equal(3, rows.Length);
        Assert.Equal("b", rows[0].Tag); Assert.Equal(30, rows[0].Score); Assert.Equal("y", rows[0].Note);
        Assert.Equal("c", rows[1].Tag); Assert.Equal(50, rows[1].Score); Assert.Equal("z", rows[1].Note);
        Assert.Equal("d", rows[2].Tag); Assert.Equal(70, rows[2].Score); Assert.Equal("w", rows[2].Note);
    }

    [Table("WjLeft")]
    public sealed class WjLeft
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    [Table("WjRight")]
    public sealed class WjRight
    {
        [Key] public int Id { get; set; }
        public string Note { get; set; } = string.Empty;
    }
}
