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
/// Extends compiled-query parameter coverage to a GroupBy key and a conditional projection — further
/// positions where the compiled value parameter could be dropped or mis-bound. Each delegate is static,
/// so every invocation reuses the same cached plan with a different argument, compared against a
/// LINQ-to-objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryGroupAndConditionalTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CqgRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
    }

    public sealed class Bucket { public int Key { get; set; } public int N { get; set; } }
    public sealed class Flag { public int Id { get; set; } public int V { get; set; } }

    private static readonly Row[] Rows = Enumerable.Range(1, 30).Select(i => new Row { Id = i, A = i * 3 }).ToArray();

    // Compiled parameter as the GroupBy key modulus.
    private static readonly Func<DbContext, int, Task<List<Bucket>>> _cqGroup =
        Norm.CompileQuery((DbContext c, int m) => c.Query<Row>().GroupBy(r => r.A % m)
            .Select(g => new Bucket { Key = g.Key, N = g.Count() }));

    // Compiled parameter in a conditional (ternary) projection.
    private static readonly Func<DbContext, int, Task<List<Flag>>> _cqCond =
        Norm.CompileQuery((DbContext c, int k) => c.Query<Row>()
            .Select(r => new Flag { Id = r.Id, V = r.A > k ? 1 : 0 }));

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqgRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO CqgRow VALUES ({r.Id},{r.A});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Compiled_param_in_groupby_key_matches_linq_across_params()
    {
        using var ctx = Ctx();
        foreach (var m in new[] { 3, 5, 7, 2 })
        {
            var expected = Rows.GroupBy(r => r.A % m).Select(g => (Key: g.Key, N: g.Count())).OrderBy(x => x.Key).ToList();
            var actual = (await _cqGroup(ctx, m)).Select(b => (Key: b.Key, N: b.N)).OrderBy(x => x.Key).ToList();
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task Compiled_param_in_conditional_projection_matches_linq_across_params()
    {
        using var ctx = Ctx();
        foreach (var k in new[] { 0, 30, 60, 15 })
        {
            var expected = Rows.Select(r => (r.Id, V: r.A > k ? 1 : 0)).OrderBy(x => x.Id).ToList();
            var actual = (await _cqCond(ctx, k)).Select(f => (f.Id, V: f.V)).OrderBy(x => x.Id).ToList();
            Assert.Equal(expected, actual);
        }
    }
}
