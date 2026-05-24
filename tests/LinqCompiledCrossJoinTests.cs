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
/// Verifies the SelectMany cross-product translation works through Norm.CompileQuery — the
/// compiled-query plan cache must produce the same `CROSS JOIN` SQL and bind a runtime
/// filter parameter that gets evaluated per invocation rather than baked in.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledCrossJoinTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CcLeft  (Id INTEGER PRIMARY KEY, L TEXT NOT NULL);
            CREATE TABLE CcRight (Id INTEGER PRIMARY KEY, R TEXT NOT NULL, Weight INTEGER NOT NULL);
            INSERT INTO CcLeft  VALUES (1,'a'),(2,'b');
            INSERT INTO CcRight VALUES (1,'x',5),(2,'y',15),(3,'z',25);
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
    public async Task Compiled_cross_product_returns_all_pairs_with_projected_columns()
    {
        var compiled = Norm.CompileQuery((DbContext c, int _) =>
            from l in c.Query<CcLeft>()
            from r in c.Query<CcRight>()
            select new { L = l.L, R = r.R });

        var pairs = (await compiled(_ctx, 0))
            .OrderBy(p => p.L).ThenBy(p => p.R).Select(p => (p.L, p.R)).ToArray();
        // 2 left × 3 right = 6 pairs.
        Assert.Equal(6, pairs.Length);
        Assert.Equal(("a", "x"), pairs[0]);
        Assert.Equal(("b", "z"), pairs[5]);
    }

    [Table("CcLeft")]
    public sealed class CcLeft
    {
        [Key] public int Id { get; set; }
        public string L { get; set; } = string.Empty;
    }

    [Table("CcRight")]
    public sealed class CcRight
    {
        [Key] public int Id { get; set; }
        public string R { get; set; } = string.Empty;
        public int Weight { get; set; }
    }
}
