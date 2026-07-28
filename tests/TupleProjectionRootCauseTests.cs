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
public sealed class TupleProjectionRootCauseTests
{
    private readonly ITestOutputHelper _out;
    public TupleProjectionRootCauseTests(ITestOutputHelper o) => _out = o;

    [Table("CqTupRC")]
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
            cmd.CommandText = "CREATE TABLE CqTupRC (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 1; i <= 6; i++) ctx.Add(new Rec { Id = i, A = i, B = i * 100 });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private void Run(string label, Func<Task> f)
    {
        try { f().GetAwaiter().GetResult(); _out.WriteLine($"{label}: OK"); }
        catch (Exception ex) { _out.WriteLine($"{label}: THREW {ex.GetType().Name}: {ex.Message.Split('\n')[0]}"); }
    }

    [Fact]
    public async Task Characterize()
    {
        using var ctx = await CtxAsync();

        // (a) tuple member in PROJECTION only, NO where
        var a = (minA: 3, projAdd: 1000);
        Run("(a) tuple-proj only, no where", async () =>
        {
            var r = await ctx.Query<Rec>().Select(x => new Proj { Id = x.Id, P = x.A + a.projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("    " + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });

        // (b) tuple member in WHERE and tuple member in PROJECTION arithmetic
        var b = (minA: 3, projAdd: 1000);
        Run("(b) tuple-where + tuple-proj arithmetic", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= b.minA).Select(x => new Proj { Id = x.Id, P = x.A + b.projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("    " + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });

        // (c) tuple member ONLY in WHERE, simple projection (no param in projection)
        var c = (minA: 3, projAdd: 1000);
        Run("(c) tuple-where only, plain projection", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= c.minA).Select(x => new Proj { Id = x.Id, P = x.A, Q = x.B }).ToListAsync();
            _out.WriteLine("    " + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });

        // (d) tuple member in PROJECTION arithmetic, DIFFERENT tuple member NOT used elsewhere, no where
        var d = (minA: 3, projAdd: 1000);
        Run("(d) tuple-proj arithmetic only (projAdd), no where, minA unused", async () =>
        {
            var r = await ctx.Query<Rec>().Select(x => new Proj { Id = x.Id, P = x.A + d.projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("    " + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });

        // (e) two scalar locals (baseline that works), where + proj arithmetic
        int e_minA = 3, e_projAdd = 1000;
        Run("(e) two scalar locals, where + proj arithmetic", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= e_minA).Select(x => new Proj { Id = x.Id, P = x.A + e_projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("    " + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });

        // (f) tuple member in WHERE and SAME tuple member in projection
        var f = (minA: 3, projAdd: 1000);
        Run("(f) tuple minA in where + same minA in proj", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= f.minA).Select(x => new Proj { Id = x.Id, P = x.A + f.minA, Q = x.B }).ToListAsync();
            _out.WriteLine("    " + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
    }
}
