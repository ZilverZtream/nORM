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
/// Triple-nested aggregate result: a composite <c>g.Key</c> kept as a nested
/// anonymous type AND an explicit nested anonymous type for the aggregate
/// payload. Both shapes need flat per-member SELECT columns and a multi-level
/// constructor materialiser:
///
///   <c>.Select(g =&gt; new {
///         Key = g.Key,                                 // anon {A, B}
///         Stats = new { Total = g.Sum(r =&gt; r.V),
///                       Count = g.Count() }            // anon {Total, Count}
///      })</c>
///
/// Exercises the same nested-construction path as the composite-key projection
/// (commit 1536484) but in the aggregate-payload position too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByTripleNestedAggregateProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TnRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL, V INTEGER NOT NULL);
            INSERT INTO TnRow VALUES
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
    public async Task Composite_key_plus_nested_aggregate_payload_rebuilds_both_anonymous_types()
    {
        var rows = (await _ctx.Query<TnRow>()
            .GroupBy(r => new { r.A, r.B })
            .Select(g => new {
                Key = g.Key,
                Stats = new { Total = g.Sum(r => r.V), Count = g.Count() }
            })
            .ToListAsync())
            .OrderBy(x => x.Key.A).ThenBy(x => x.Key.B).ToArray();

        Assert.Equal(3, rows.Length);
        Assert.Equal(("x", 1, 600, 3), (rows[0].Key.A, rows[0].Key.B, rows[0].Stats.Total, rows[0].Stats.Count));
        Assert.Equal(("x", 2, 900, 2), (rows[1].Key.A, rows[1].Key.B, rows[1].Stats.Total, rows[1].Stats.Count));
        Assert.Equal(("y", 1, 600, 1), (rows[2].Key.A, rows[2].Key.B, rows[2].Stats.Total, rows[2].Stats.Count));
    }

    [Fact]
    public async Task Flat_key_plus_nested_aggregate_payload_rebuilds_aggregate_anon_only()
    {
        var rows = (await _ctx.Query<TnRow>()
            .GroupBy(r => r.A)
            .Select(g => new {
                Cat = g.Key,
                Stats = new { Total = g.Sum(r => r.V), Count = g.Count() }
            })
            .ToListAsync())
            .OrderBy(x => x.Cat).ToArray();

        Assert.Equal(2, rows.Length);
        Assert.Equal(("x", 1500, 5), (rows[0].Cat, rows[0].Stats.Total, rows[0].Stats.Count));
        Assert.Equal(("y", 600,  1), (rows[1].Cat, rows[1].Stats.Total, rows[1].Stats.Count));
    }

    [Table("TnRow")]
    public sealed class TnRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
        public int V { get; set; }
    }
}
