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
/// ADVERSARIAL AUDIT round 3: the sharpest compiled-query plan-reuse corners —
/// a param that IS the IN-list (varying-length list across invocations, where a cached
/// placeholder count would be silent-wrong), the same param in two SQL positions,
/// a param feeding both Where and Take, compiled GroupBy/aggregate with a param, and
/// terminal First/Single semantics reused across empty/non-empty argument values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryPlanReuseAudit3Tests
{
    [Table("CqAudit3")]
    public sealed class Rec
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int? NVal { get; set; }
        public int Score { get; set; }
        public int GroupId { get; set; }
    }

    public sealed class GroupAgg { public int GroupId { get; set; } public int Total { get; set; } }

    private static Rec[] SeedRows() => new[]
    {
        new Rec { Id = 1, NVal = 5,    Score = 10, GroupId = 1 },
        new Rec { Id = 2, NVal = 10,   Score = 20, GroupId = 1 },
        new Rec { Id = 3, NVal = 5,    Score = 30, GroupId = 2 },
        new Rec { Id = 4, NVal = 20,   Score = 40, GroupId = 2 },
        new Rec { Id = 5, NVal = null, Score = 50, GroupId = 3 },
        new Rec { Id = 6, NVal = 10,   Score = 10, GroupId = 3 },
    };

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqAudit3 (Id INTEGER PRIMARY KEY, NVal INTEGER NULL, Score INTEGER NOT NULL, GroupId INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in SeedRows()) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static int[] Ids(IEnumerable<Rec> rows) => rows.Select(r => r.Id).OrderBy(x => x).ToArray();

    // ── 1. Param IS the IN-list; list length varies across invocations ─────────
    // If the cached plan bakes the first call's placeholder count, a differently-sized
    // list on a later call is silent-wrong. If unsupported, it must throw (fail-loud), not
    // silently return stale/wrong rows.
    [Fact]
    public async Task ParamArray_Contains_VaryingLength_MatchesOracleOrThrows()
    {
        using var ctx = await CtxAsync();
        Func<DbContext, int[], Task<List<Rec>>> compiled;
        try
        {
            compiled = Norm.CompileQuery((DbContext c, int[] ids) => c.Query<Rec>().Where(x => ids.Contains(x.Id)));
        }
        catch (NormException)
        {
            return; // fail-loud at compile time is acceptable (not silent-wrong)
        }

        var argLists = new[]
        {
            new[] { 1, 2, 3 },
            new[] { 4 },
            new[] { 2, 5 },
            new[] { 1, 2, 3, 4, 5, 6 },
            Array.Empty<int>(),
        };

        foreach (var ids in argLists)
        {
            var local = ids;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => local.Contains(x.Id)).ToListAsync());
            int[] actual;
            try
            {
                actual = Ids(await compiled(ctx, ids));
            }
            catch (NormException)
            {
                continue; // fail-loud at execute time is acceptable
            }
            Assert.Equal(oracle, actual);
        }
    }

    // ── 2. Same param in two SQL positions (OR) ────────────────────────────────
    [Fact]
    public async Task SameParam_TwoPositions_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int k) => c.Query<Rec>().Where(x => x.Score == k || x.NVal == k));
        foreach (var k in new[] { 10, 5, 20, 50, 999 })
        {
            int lk = k;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score == lk || x.NVal == lk).ToListAsync());
            var actual = Ids(await compiled(ctx, k));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 3. Same param feeds both Where and Take ────────────────────────────────
    [Fact]
    public async Task Param_InWhereAndTake_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int k) =>
            c.Query<Rec>().Where(x => x.Score >= k * 10).OrderBy(x => x.Id).Take(k));
        foreach (var k in new[] { 1, 2, 3, 0, 5 })
        {
            int lk = k;
            var oracle = (await ctx.Query<Rec>().Where(x => x.Score >= lk * 10).OrderBy(x => x.Id).Take(lk).ToListAsync()).Select(r => r.Id).ToArray();
            var actual = (await compiled(ctx, k)).Select(r => r.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 4. Compiled GroupBy + aggregate with a param filter, reused ────────────
    [Fact]
    public async Task Compiled_GroupBy_WithParamFilter_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<Rec>().Where(x => x.Score >= minScore)
                .GroupBy(x => x.GroupId)
                .Select(g => new GroupAgg { GroupId = g.Key, Total = g.Sum(x => x.Score) }));
        foreach (var minScore in new[] { 0, 20, 40, 100 })
        {
            int lm = minScore;
            var oracle = (await ctx.Query<Rec>().Where(x => x.Score >= lm)
                    .GroupBy(x => x.GroupId).Select(g => new GroupAgg { GroupId = g.Key, Total = g.Sum(x => x.Score) }).ToListAsync())
                .Select(a => (a.GroupId, a.Total)).OrderBy(t => t.GroupId).ToArray();
            var actual = (await compiled(ctx, minScore)).Select(a => (a.GroupId, a.Total)).OrderBy(t => t.GroupId).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 5. Terminal FirstOrDefault reused across empty/non-empty args ──────────
    [Fact]
    public async Task TerminalFirstOrDefault_EmptyAndNonEmpty_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileTerminalQuery((DbContext c, int score) =>
            c.Query<Rec>().Where(x => x.Score == score).OrderBy(x => x.Id).FirstOrDefault());
        foreach (var score in new[] { 10, 999, 30, 50, 12345 })
        {
            int ls = score;
            var oracle = await ctx.Query<Rec>().Where(x => x.Score == ls).OrderBy(x => x.Id).FirstOrDefaultAsync();
            var actual = await compiled(ctx, score);
            Assert.Equal(oracle?.Id, actual?.Id);
        }
    }

    // ── 6. Terminal Sum (nullable-safe) reused across a filtering param ────────
    [Fact]
    public async Task TerminalSum_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<Rec>().Where(x => x.Score >= minScore).Sum(x => x.Score));
        foreach (var minScore in new[] { 0, 20, 40, 100 })
        {
            int lm = minScore;
            var oracle = await ctx.Query<Rec>().Where(x => x.Score >= lm).SumAsync(x => x.Score);
            var actual = await compiled(ctx, minScore);
            Assert.Equal(oracle, actual);
        }
    }

    // ── 7. Param used in OrderBy direction distance AND Take (mixed positions) ─
    [Fact]
    public async Task Param_InOrderByAndTake_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int k, int take) p) =>
            c.Query<Rec>().OrderBy(x => x.Score > p.k ? x.Score - p.k : p.k - x.Score).ThenBy(x => x.Id).Take(p.take));
        foreach (var p in new[] { (25, 2), (0, 3), (50, 1), (10, 6) })
        {
            var lp = p;
            var oracle = (await ctx.Query<Rec>().OrderBy(x => x.Score > lp.Item1 ? x.Score - lp.Item1 : lp.Item1 - x.Score).ThenBy(x => x.Id).Take(lp.Item2).ToListAsync()).Select(r => r.Id).ToArray();
            var actual = (await compiled(ctx, p)).Select(r => r.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }
}
