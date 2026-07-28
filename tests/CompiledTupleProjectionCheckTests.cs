using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class CompiledTupleProjectionCheckTests
{
    private readonly ITestOutputHelper _out;
    public CompiledTupleProjectionCheckTests(ITestOutputHelper o) => _out = o;

    [Table("CqTupProj")]
    public sealed class Rec
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    public sealed class Proj { public int Id { get; set; } public int P { get; set; } public int Q { get; set; } }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqTupProj (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 1; i <= 12; i++) ctx.Add(new Rec { Id = i, A = i, B = i * 100 });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static readonly Rec[] Data = Enumerable.Range(1, 12).Select(i => new Rec { Id = i, A = i, B = i * 100 }).ToArray();

    // COMPILED path (tuple param, one member in Where, one in projection) vs LINQ-to-objects.
    [Fact]
    public async Task Compiled_WhereParam_ProjectionParam_vs_LinqToObjects()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int minA, int projAdd) p) =>
            c.Query<Rec>().Where(x => x.A >= p.minA).Select(x => new Proj { Id = x.Id, P = x.A + p.projAdd, Q = x.B }));

        foreach (var p in new[] { (minA: 8, projAdd: 1000), (minA: 3, projAdd: -1), (minA: 1, projAdd: 0), (minA: 10, projAdd: 500) })
        {
            var lp = p;
            var expected = Data.Where(x => x.A >= lp.minA)
                .Select(x => (x.Id, P: x.A + lp.projAdd, Q: x.B)).OrderBy(t => t.Id).ToArray();
            (int, int, int)[] actual;
            try
            {
                actual = (await compiled(ctx, p)).Select(pr => (pr.Id, pr.P, pr.Q)).OrderBy(t => t.Item1).ToArray();
            }
            catch (Exception ex)
            {
                _out.WriteLine($"p={p}: COMPILED THREW {ex.GetType().Name}: {ex.Message}");
                throw;
            }
            var exp = expected.Select(t => (t.Id, t.P, t.Q)).ToArray();
            _out.WriteLine($"p={p}: expected={string.Join(";", exp)} actual={string.Join(";", actual)}");
            Assert.Equal(exp, actual);
        }
    }
}
