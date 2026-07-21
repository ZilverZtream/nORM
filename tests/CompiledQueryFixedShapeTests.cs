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
/// Verifies this session's translation fixes (widening float cast, computed projections) hold through
/// the COMPILED-query path (Norm.CompileQuery), and that a free parameter binds correctly across
/// repeated invocations of the same cached plan (a historical source of closure/param-binding bugs).
/// Each compiled delegate is static — every invocation exercises the same cached plan with a
/// different argument, compared against a LINQ-to-objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryFixedShapeTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CqfRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    public sealed class Ratio { public int Id { get; set; } public double Value { get; set; } }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    {
        Id = i, A = i * 3, B = (i % 5) + 1,
    }).ToArray();

    // Float-cast division in a compiled projection, filtered by a free parameter.
    private static readonly Func<DbContext, int, Task<List<Ratio>>> _cqFloatRatio =
        Norm.CompileQuery((DbContext c, int minB) => c.Query<Row>().Where(r => r.B >= minB)
            .Select(r => new Ratio { Id = r.Id, Value = (double)r.A / r.B }));

    // Computed projection with the parameter used in both predicate and projection (T must be a class).
    private static readonly Func<DbContext, int, Task<List<Ratio>>> _cqComputed =
        Norm.CompileQuery((DbContext c, int k) => c.Query<Row>().Where(r => r.A > k)
            .Select(r => new Ratio { Id = r.Id, Value = r.A - k }));

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqfRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO CqfRow VALUES ({r.Id},{r.A},{r.B});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Compiled_float_cast_projection_matches_linq_across_params()
    {
        using var ctx = Ctx();
        foreach (var minB in new[] { 1, 3, 5, 2, 4 }) // vary the param over the same cached plan
        {
            var expected = Rows.Where(r => r.B >= minB).Select(r => (r.Id, Value: Math.Round((double)r.A / r.B, 6)))
                .OrderBy(x => x.Id).ToList();
            var actual = (await _cqFloatRatio(ctx, minB)).Select(x => (x.Id, Value: Math.Round(x.Value, 6)))
                .OrderBy(x => x.Id).ToList();
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task Compiled_computed_projection_matches_linq_across_params()
    {
        using var ctx = Ctx();
        foreach (var k in new[] { 0, 10, 30, 5, 45 })
        {
            var expected = Rows.Where(r => r.A > k).Select(r => (double)(r.A - k)).OrderBy(v => v).ToList();
            var actual = (await _cqComputed(ctx, k)).Select(x => x.Value).OrderBy(v => v).ToList();
            Assert.Equal(expected, actual);
        }
    }
}
