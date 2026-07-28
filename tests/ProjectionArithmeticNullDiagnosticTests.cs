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
public sealed class ProjectionArithmeticNullDiagnosticTests
{
    private readonly ITestOutputHelper _out;
    public ProjectionArithmeticNullDiagnosticTests(ITestOutputHelper o) => _out = o;

    [Table("CqDiag")]
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
            cmd.CommandText = "CREATE TABLE CqDiag (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 1; i <= 6; i++) ctx.Add(new Rec { Id = i, A = i, B = i * 100 });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static void Run(ITestOutputHelper o, string label, Func<Task> f)
    {
        try { f().GetAwaiter().GetResult(); o.WriteLine($"{label}: OK"); }
        catch (Exception ex) { o.WriteLine($"{label}: THREW {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Diagnose_WhereConst_Projection_TwoScalarLocals()
    {
        using var ctx = await CtxAsync();
        int minA = 3, projAdd = 1000; // two separate scalar locals
        Run(_out, "uncompiled two-scalar-locals", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= minA)
                .Select(x => new Proj { Id = x.Id, P = x.A + projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("  rows=" + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
    }

    [Fact]
    public async Task Diagnose_WhereConst_Projection_TupleLocal()
    {
        using var ctx = await CtxAsync();
        var lp = (minA: 3, projAdd: 1000); // tuple local, two members used
        Run(_out, "uncompiled tuple-local", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= lp.minA)
                .Select(x => new Proj { Id = x.Id, P = x.A + lp.projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("  rows=" + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
    }

    [Fact]
    public async Task Diagnose_NoWhere_Projection_TwoScalarLocals()
    {
        using var ctx = await CtxAsync();
        int projAdd = 1000;
        Run(_out, "uncompiled no-where projadd-only", async () =>
        {
            var r = await ctx.Query<Rec>()
                .Select(x => new Proj { Id = x.Id, P = x.A + projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("  rows=" + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
    }

    [Fact]
    public async Task Diagnose_WhereConst_Projection_QeqB_only()
    {
        using var ctx = await CtxAsync();
        int minA = 3, projAdd = 1000;
        Run(_out, "uncompiled where+proj (P=A+add, Q=B)", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= minA)
                .Select(x => new Proj { Id = x.Id, P = x.A + projAdd, Q = x.B }).ToListAsync();
            _out.WriteLine("  rows=" + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
        Run(_out, "uncompiled where+proj (P=A+add, Q=0const)", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= minA)
                .Select(x => new Proj { Id = x.Id, P = x.A + projAdd, Q = 0 }).ToListAsync();
            _out.WriteLine("  rows=" + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
        Run(_out, "uncompiled where+proj (P=A only, Q=B)", async () =>
        {
            var r = await ctx.Query<Rec>().Where(x => x.A >= minA)
                .Select(x => new Proj { Id = x.Id, P = x.A, Q = x.B }).ToListAsync();
            _out.WriteLine("  rows=" + string.Join(";", r.Select(p => $"{p.Id}:{p.P}:{p.Q}")));
        });
    }
}
