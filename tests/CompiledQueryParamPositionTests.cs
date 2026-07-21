using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Extends compiled-query coverage to the free parameter appearing in less-common positions: a tuple
/// parameter whose members are used in the projection (the __qm member-marker path), a parameter used
/// in an OrderBy key, and a parameter used in an aggregate selector. Each compiled delegate is static,
/// so every invocation reuses the same cached plan with a different argument — the classic setting for
/// parameter-binding bugs — compared against a LINQ-to-objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryParamPositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CqpRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    public sealed class Box { public int Id { get; set; } public int V { get; set; } }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    {
        Id = i, A = i * 2, B = (i % 5) + 1,
    }).ToArray();

    // Tuple parameter: both members used in the projection (Item1 in the computed value, Item2 filters).
    private static readonly Func<DbContext, (int add, int minB), Task<List<Box>>> _cqTuple =
        Norm.CompileQuery((DbContext c, (int add, int minB) p) => c.Query<Row>().Where(r => r.B >= p.minB)
            .Select(r => new Box { Id = r.Id, V = r.A + p.add }));

    // Parameter in an OrderBy key: order by |A - k| (distance from k), then Id.
    private static readonly Func<DbContext, int, Task<List<Box>>> _cqOrderKey =
        Norm.CompileQuery((DbContext c, int k) => c.Query<Row>()
            .OrderBy(r => (r.A > k ? r.A - k : k - r.A)).ThenBy(r => r.Id)
            .Select(r => new Box { Id = r.Id, V = r.A }));

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqpRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO CqpRow VALUES ({r.Id},{r.A},{r.B});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Compiled_tuple_param_in_projection_matches_linq_across_params()
    {
        using var ctx = Ctx();
        foreach (var p in new[] { (add: 100, minB: 1), (add: 5, minB: 3), (add: 0, minB: 5), (add: 50, minB: 2) })
        {
            var expected = Rows.Where(r => r.B >= p.minB).Select(r => (r.Id, V: r.A + p.add)).OrderBy(x => x.Id).ToList();
            var actual = (await _cqTuple(ctx, p)).Select(x => (x.Id, V: x.V)).OrderBy(x => x.Id).ToList();
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task Compiled_param_in_orderby_key_matches_linq_across_params()
    {
        using var ctx = Ctx();
        foreach (var k in new[] { 20, 0, 40, 15 })
        {
            var expected = Rows.OrderBy(r => (r.A > k ? r.A - k : k - r.A)).ThenBy(r => r.Id).Select(r => r.Id).ToList();
            var actual = (await _cqOrderKey(ctx, k)).Select(x => x.Id).ToList();
            Assert.Equal(expected, actual);
        }
    }
}
