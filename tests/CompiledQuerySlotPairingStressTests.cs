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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ADVERSARIAL AUDIT round 5: parameter slot-pairing across clause-time (Where/OrderBy/Take) and
/// Build-time (projection) registration. The compiled pipeline pairs marked slots BY NAME because
/// projection slots register after clause slots; a positional mistake would cross-bind. Asymmetric
/// argument values make any swap immediately visible against the uncompiled oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQuerySlotPairingStressTests
{
    [Table("CqSlot")]
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
            cmd.CommandText = "CREATE TABLE CqSlot (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 1; i <= 12; i++) ctx.Add(new Rec { Id = i, A = i, B = i * 100 });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    // ── 1. Projection-only param (no Where) reused ─────────────────────────────
    [Fact]
    public async Task ProjectionOnlyParam_Reused_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int add) =>
            c.Query<Rec>().Select(x => new Proj { Id = x.Id, P = x.A + add, Q = x.B }));
        foreach (var add in new[] { 1000, 0, -5, 777 })
        {
            int la = add;
            var oracle = (await ctx.Query<Rec>().Select(x => new Proj { Id = x.Id, P = x.A + la, Q = x.B }).ToListAsync())
                .Select(p => (p.Id, p.P, p.Q)).OrderBy(t => t.Id).ToArray();
            var actual = (await compiled(ctx, add)).Select(p => (p.Id, p.P, p.Q)).OrderBy(t => t.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 2. Distinct params: one in WHERE (clause) and one in PROJECTION (build) ─
    // Asymmetric values: a swap would use the projection value to filter and vice versa.
    [Fact]
    public async Task WhereParam_And_ProjectionParam_NoSwap_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int minA, int projAdd) p) =>
            c.Query<Rec>().Where(x => x.A >= p.minA).Select(x => new Proj { Id = x.Id, P = x.A + p.projAdd, Q = x.B }));
        foreach (var p in new[] { (minA: 8, projAdd: 1000), (minA: 3, projAdd: -1), (minA: 1, projAdd: 0), (minA: 10, projAdd: 500) })
        {
            var lp = p;
            var oracle = (await ctx.Query<Rec>().Where(x => x.A >= lp.minA)
                    .Select(x => new Proj { Id = x.Id, P = x.A + lp.projAdd, Q = x.B }).ToListAsync())
                .Select(pr => (pr.Id, pr.P, pr.Q)).OrderBy(t => t.Id).ToArray();
            var actual = (await compiled(ctx, p)).Select(pr => (pr.Id, pr.P, pr.Q)).OrderBy(t => t.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 3. Three params: Where + OrderBy + Projection + Take, all distinct ──────
    [Fact]
    public async Task Where_OrderBy_Projection_Take_AllParams_NoCrossBind_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int minA, int projMul, int take) p) =>
            c.Query<Rec>().Where(x => x.A >= p.minA)
                .OrderByDescending(x => x.A)
                .Select(x => new Proj { Id = x.Id, P = x.A * p.projMul, Q = x.B })
                .Take(p.take));
        foreach (var p in new[] { (minA: 2, projMul: 10, take: 3), (minA: 5, projMul: 2, take: 5), (minA: 1, projMul: 1, take: 2), (minA: 8, projMul: 100, take: 10) })
        {
            var lp = p;
            var oracle = (await ctx.Query<Rec>().Where(x => x.A >= lp.minA)
                    .OrderByDescending(x => x.A)
                    .Select(x => new Proj { Id = x.Id, P = x.A * lp.projMul, Q = x.B })
                    .Take(lp.take).ToListAsync())
                .Select(pr => (pr.Id, pr.P, pr.Q)).ToArray();
            var actual = (await compiled(ctx, p)).Select(pr => (pr.Id, pr.P, pr.Q)).ToArray();
            Assert.Equal(oracle, actual); // order-sensitive on purpose (OrderByDescending + Take)
        }
    }

    // ── 4. Same param in WHERE and PROJECTION (single scalar) reused ───────────
    [Fact]
    public async Task SameScalarParam_InWhereAndProjection_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int k) =>
            c.Query<Rec>().Where(x => x.A > k).Select(x => new Proj { Id = x.Id, P = x.A - k, Q = k }));
        foreach (var k in new[] { 5, 0, 9, 3 })
        {
            int lk = k;
            var oracle = (await ctx.Query<Rec>().Where(x => x.A > lk).Select(x => new Proj { Id = x.Id, P = x.A - lk, Q = lk }).ToListAsync())
                .Select(pr => (pr.Id, pr.P, pr.Q)).OrderBy(t => t.Id).ToArray();
            var actual = (await compiled(ctx, k)).Select(pr => (pr.Id, pr.P, pr.Q)).OrderBy(t => t.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }
}
