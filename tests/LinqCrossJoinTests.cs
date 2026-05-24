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
/// Verifies that the LINQ cross-product shape `from a in A from b in B select ...` lowers to a
/// CROSS JOIN at the SQL layer and returns every (a, b) pair with the projected columns mapped
/// to the right anonymous-type members.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCrossJoinTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CjLeft  (Id INTEGER PRIMARY KEY, L TEXT NOT NULL);
            CREATE TABLE CjRight (Id INTEGER PRIMARY KEY, R TEXT NOT NULL);
            INSERT INTO CjLeft  VALUES (1,'a'),(2,'b');
            INSERT INTO CjRight VALUES (1,'x'),(2,'y'),(3,'z');
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
    public async Task Cross_product_returns_every_pair_with_projected_columns()
    {
        var rows = (await (from l in _ctx.Query<CjLeft>()
                           from r in _ctx.Query<CjRight>()
                           select new { L = l.L, R = r.R })
                           .ToListAsync())
                  .OrderBy(p => p.L).ThenBy(p => p.R).ToArray();
        Assert.Equal(6, rows.Length);
        Assert.Equal(("a", "x"), (rows[0].L, rows[0].R));
        Assert.Equal(("a", "y"), (rows[1].L, rows[1].R));
        Assert.Equal(("a", "z"), (rows[2].L, rows[2].R));
        Assert.Equal(("b", "x"), (rows[3].L, rows[3].R));
        Assert.Equal(("b", "y"), (rows[4].L, rows[4].R));
        Assert.Equal(("b", "z"), (rows[5].L, rows[5].R));
    }

    [Table("CjLeft")]
    public sealed class CjLeft
    {
        [Key] public int Id { get; set; }
        public string L { get; set; } = string.Empty;
    }

    [Table("CjRight")]
    public sealed class CjRight
    {
        [Key] public int Id { get; set; }
        public string R { get; set; } = string.Empty;
    }
}
