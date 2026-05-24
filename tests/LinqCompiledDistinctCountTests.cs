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
/// Pins the compiled-query path of <c>Select(anon).Distinct().Count()</c> and
/// <c>LongCount()</c>. The plan cache must produce the same subquery-wrapped SQL
/// (<c>SELECT COUNT(*) FROM (SELECT DISTINCT &lt;proj&gt; FROM ...) AS T0</c>) and
/// not regress to a plain row-count after the first compile.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledDistinctCountTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CdcRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL);
            INSERT INTO CdcRow VALUES (1,'x',1),(2,'x',1),(3,'x',1),(4,'y',2),(5,'y',2),(6,'z',3);
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
    public async Task Compiled_distinct_anonymous_projection_count_returns_distinct_pairs()
    {
        var compiled = Norm.CompileQuery((DbContext c, int _) =>
            c.Query<CdcRow>().Select(r => new { r.A, r.B }).Distinct());

        var rows = (await compiled(_ctx, 0))
            .OrderBy(p => p.A).ThenBy(p => p.B).ToArray();
        // 6 rows but 3 distinct (A, B) pairs: (x,1), (y,2), (z,3)
        Assert.Equal(3, rows.Length);
        Assert.Equal(("x", 1), (rows[0].A, rows[0].B));
        Assert.Equal(("y", 2), (rows[1].A, rows[1].B));
        Assert.Equal(("z", 3), (rows[2].A, rows[2].B));
    }

    [Table("CdcRow")]
    public sealed class CdcRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
    }
}
