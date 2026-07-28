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
/// ADVERSARIAL AUDIT round 4: cross-context plan/command-pool reuse (the same compiled delegate
/// invoked against two different connections/databases must return each database's own rows, never
/// the other's cached command or data), plus Distinct/Any/Min terminal reuse across param values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryPlanReuseAudit4Tests
{
    [Table("CqAudit4")]
    public sealed class Rec
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int Score { get; set; }
        public string Tag { get; set; } = "";
    }

    private static async Task<DbContext> CtxAsync((int id, int score, string tag)[] rows)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqAudit4 (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Tag TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(new Rec { Id = r.id, Score = r.score, Tag = r.tag });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static int[] Ids(IEnumerable<Rec> rows) => rows.Select(r => r.Id).OrderBy(x => x).ToArray();

    // ── 1. Same compiled delegate against two different DBs, interleaved ───────
    [Fact]
    public async Task CrossContext_SameDelegate_TwoDatabases_NoBleed()
    {
        using var ctxA = await CtxAsync(new[] { (1, 10, "a"), (2, 20, "a"), (3, 30, "b") });
        using var ctxB = await CtxAsync(new[] { (10, 10, "x"), (11, 20, "y"), (12, 20, "z"), (13, 99, "x") });

        var compiled = Norm.CompileQuery((DbContext c, int score) => c.Query<Rec>().Where(x => x.Score == score));

        // Interleave A and B with the same and different params.
        for (int iter = 0; iter < 6; iter++)
        {
            var aExpected = Ids(await ctxA.Query<Rec>().Where(x => x.Score == 20).ToListAsync());
            var aActual = Ids(await compiled(ctxA, 20));
            Assert.Equal(aExpected, aActual);

            var bExpected = Ids(await ctxB.Query<Rec>().Where(x => x.Score == 20).ToListAsync());
            var bActual = Ids(await compiled(ctxB, 20));
            Assert.Equal(bExpected, bActual);

            var aActual10 = Ids(await compiled(ctxA, 10));
            Assert.Equal(Ids(await ctxA.Query<Rec>().Where(x => x.Score == 10).ToListAsync()), aActual10);

            var bActual99 = Ids(await compiled(ctxB, 99));
            Assert.Equal(Ids(await ctxB.Query<Rec>().Where(x => x.Score == 99).ToListAsync()), bActual99);
        }
    }

    // ── 2. Distinct projection + param reused ──────────────────────────────────
    [Fact]
    public async Task Compiled_DistinctTag_WithParam_MatchesOracle()
    {
        using var ctx = await CtxAsync(new[] { (1, 10, "a"), (2, 20, "a"), (3, 30, "b"), (4, 40, "b"), (5, 50, "c") });
        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<Rec>().Where(x => x.Score >= minScore).Select(x => x.Tag).Distinct());
        // NOTE: T must be a class for CompileQuery<,,T>; string is a class so OK.
        foreach (var minScore in new[] { 0, 25, 45, 100 })
        {
            int lm = minScore;
            var oracle = (await ctx.Query<Rec>().Where(x => x.Score >= lm).Select(x => x.Tag).Distinct().ToListAsync()).OrderBy(t => t).ToArray();
            var actual = (await compiled(ctx, minScore)).OrderBy(t => t).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 3. Terminal Any(predicate incorporating param) reused ──────────────────
    [Fact]
    public async Task TerminalAny_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync(new[] { (1, 10, "a"), (2, 20, "a"), (3, 30, "b") });
        var compiled = Norm.CompileTerminalQuery((DbContext c, int score) => c.Query<Rec>().Any(x => x.Score == score));
        foreach (var score in new[] { 10, 15, 30, 999 })
        {
            int ls = score;
            var oracle = await ctx.Query<Rec>().AnyAsync(x => x.Score == ls);
            var actual = await compiled(ctx, score);
            Assert.Equal(oracle, actual);
        }
    }

    // ── 4. Terminal Min over a filtered set reused (empty→throw parity) ─────────
    [Fact]
    public async Task TerminalMin_ReusedAcrossValues_MatchesOracleIncludingEmpty()
    {
        using var ctx = await CtxAsync(new[] { (1, 10, "a"), (2, 20, "a"), (3, 30, "b") });
        var compiled = Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<Rec>().Where(x => x.Score >= minScore).Min(x => x.Score));
        foreach (var minScore in new[] { 0, 20, 30 })
        {
            int lm = minScore;
            var oracle = await ctx.Query<Rec>().Where(x => x.Score >= lm).MinAsync(x => x.Score);
            var actual = await compiled(ctx, minScore);
            Assert.Equal(oracle, actual);
        }
        // Empty set: both compiled and uncompiled must throw the same way.
        var uncompiledThrew = false;
        try { await ctx.Query<Rec>().Where(x => x.Score >= 1000).MinAsync(x => x.Score); }
        catch (InvalidOperationException) { uncompiledThrew = true; }
        var compiledThrew = false;
        try { await compiled(ctx, 1000); }
        catch (InvalidOperationException) { compiledThrew = true; }
        Assert.Equal(uncompiledThrew, compiledThrew);
    }

    // ── 5. String param equality (case-sensitive) reused ───────────────────────
    [Fact]
    public async Task StringEquality_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync(new[] { (1, 10, "a"), (2, 20, "A"), (3, 30, "b"), (4, 40, "a") });
        var compiled = Norm.CompileQuery((DbContext c, string tag) => c.Query<Rec>().Where(x => x.Tag == tag));
        foreach (var tag in new[] { "a", "A", "b", "c" })
        {
            string lt = tag;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Tag == lt).ToListAsync());
            var actual = Ids(await compiled(ctx, tag));
            Assert.Equal(oracle, actual);
        }
    }
}
