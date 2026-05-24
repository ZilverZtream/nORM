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
/// Pins server-side translation of <c>GroupBy(x =&gt; new { x.A, x.B }).Select(g =&gt;
/// new { g.Key, Count = g.Count(), Total = g.Sum(x =&gt; x.V) })</c>. Composite
/// anonymous keys must lower to a multi-column <c>GROUP BY A, B</c> and the
/// projected key columns must round-trip into the materialized anonymous type
/// without losing track of which column belongs to which key field.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByAnonymousKeyShapeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GkRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL, V INTEGER NOT NULL);
            -- 6 rows, 3 distinct (A, B) groups:
            --   (x,1): rows 1,2,3 → V sums to 100+200+300=600
            --   (x,2): rows 4,5   → V sums to 400+500=900
            --   (y,1): row 6      → V sum = 600
            INSERT INTO GkRow VALUES
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
    public async Task GroupBy_anonymous_key_with_count_and_sum_aggregates_per_group()
    {
        var groups = (await _ctx.Query<GkRow>()
            .GroupBy(r => new { r.A, r.B })
            .Select(g => new { g.Key.A, g.Key.B, Count = g.Count(), Total = g.Sum(r => r.V) })
            .ToListAsync())
            .OrderBy(x => x.A).ThenBy(x => x.B).ToArray();

        Assert.Equal(3, groups.Length);
        Assert.Equal(("x", 1, 3, 600), (groups[0].A, groups[0].B, groups[0].Count, groups[0].Total));
        Assert.Equal(("x", 2, 2, 900), (groups[1].A, groups[1].B, groups[1].Count, groups[1].Total));
        Assert.Equal(("y", 1, 1, 600), (groups[2].A, groups[2].B, groups[2].Count, groups[2].Total));
    }

    [Table("GkRow")]
    public sealed class GkRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
        public int V { get; set; }
    }
}
