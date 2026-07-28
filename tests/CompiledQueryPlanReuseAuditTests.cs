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
/// ADVERSARIAL AUDIT: compiled-query plan reuse across differing parameter values.
/// Each test compiles ONE delegate and invokes it with several distinct argument values,
/// asserting each result against a fresh (uncompiled) nORM query oracle with the same value.
/// A divergence = the cached plan baked/rebound the wrong value = silent data corruption.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryPlanReuseAuditTests
{
    public enum Kind { Alpha = 0, Beta = 1, Gamma = 2 }

    [Table("CqAudit")]
    public sealed class Rec
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int? NVal { get; set; }
        public string? Name { get; set; }
        public int Score { get; set; }
        public decimal Price { get; set; }
        public Kind Kind { get; set; }
        public DateTime Created { get; set; }
        public Guid Uid { get; set; }
        public bool Active { get; set; }
    }

    private static readonly Guid[] Uids =
    {
        Guid.Parse("11111111-1111-1111-1111-111111111111"),
        Guid.Parse("22222222-2222-2222-2222-222222222222"),
        Guid.Parse("33333333-3333-3333-3333-333333333333"),
        Guid.Parse("44444444-4444-4444-4444-444444444444"),
        Guid.Parse("55555555-5555-5555-5555-555555555555"),
        Guid.Parse("66666666-6666-6666-6666-666666666666"),
    };

    private static readonly DateTime[] Dates =
    {
        new(2020, 1, 1), new(2021, 6, 15), new(2022, 3, 10),
        new(2023, 12, 25), new(2019, 7, 4), new(2024, 2, 29),
    };

    private static Rec[] SeedRows() => new[]
    {
        new Rec { Id = 1, NVal = 5,    Name = "apple",   Score = 10, Price = 9.99m,  Kind = Kind.Alpha, Created = Dates[0], Uid = Uids[0], Active = true },
        new Rec { Id = 2, NVal = null, Name = "banana",  Score = 20, Price = 19.50m, Kind = Kind.Beta,  Created = Dates[1], Uid = Uids[1], Active = false },
        new Rec { Id = 3, NVal = 5,    Name = "cherry",  Score = 30, Price = 5.00m,  Kind = Kind.Gamma, Created = Dates[2], Uid = Uids[2], Active = true },
        new Rec { Id = 4, NVal = 10,   Name = "avocado", Score = 40, Price = 50.00m, Kind = Kind.Alpha, Created = Dates[3], Uid = Uids[3], Active = false },
        new Rec { Id = 5, NVal = null, Name = null,      Score = 20, Price = 100.0m, Kind = Kind.Beta,  Created = Dates[4], Uid = Uids[4], Active = true },
        new Rec { Id = 6, NVal = 15,   Name = "apricot", Score = 30, Price = 0.99m,  Kind = Kind.Gamma, Created = Dates[5], Uid = Uids[5], Active = false },
    };

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqAudit (Id INTEGER PRIMARY KEY, NVal INTEGER NULL, Name TEXT NULL, " +
                              "Score INTEGER NOT NULL, Price TEXT NOT NULL, Kind INTEGER NOT NULL, " +
                              "Created TEXT NOT NULL, Uid TEXT NOT NULL, Active INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in SeedRows()) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static int[] Ids(IEnumerable<Rec> rows) => rows.Select(r => r.Id).OrderBy(x => x).ToArray();

    // ── 1. Null transition: int? equality ──────────────────────────────────────
    [Fact]
    public async Task NullableInt_Equality_NullTransition_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int? n) => c.Query<Rec>().Where(x => x.NVal == n));
        foreach (int? n in new int?[] { 5, null, 10, null, 15, 999 })
        {
            int? local = n;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.NVal == local).ToListAsync());
            var actual = Ids(await compiled(ctx, n));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 2. Null transition: int? inequality ────────────────────────────────────
    [Fact]
    public async Task NullableInt_Inequality_NullTransition_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int? n) => c.Query<Rec>().Where(x => x.NVal != n));
        foreach (int? n in new int?[] { 5, null, 10, null })
        {
            int? local = n;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.NVal != local).ToListAsync());
            var actual = Ids(await compiled(ctx, n));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 3. Null transition: string equality ────────────────────────────────────
    [Fact]
    public async Task String_Equality_NullTransition_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, string? s) => c.Query<Rec>().Where(x => x.Name == s));
        foreach (var s in new string?[] { "apple", null, "banana", null, "zzz" })
        {
            string? local = s;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Name == local).ToListAsync());
            var actual = Ids(await compiled(ctx, s));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 4. Direct scalar Take(n) reused across n ────────────────────────────────
    [Fact]
    public async Task ScalarTake_ReusedAcrossN_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int n) => c.Query<Rec>().OrderBy(x => x.Id).Take(n));
        foreach (var n in new[] { 2, 5, 1, 3, 0, 6 })
        {
            int local = n;
            var oracle = (await ctx.Query<Rec>().OrderBy(x => x.Id).Take(local).ToListAsync()).Select(r => r.Id).ToArray();
            var actual = (await compiled(ctx, n)).Select(r => r.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 5. Skip+Take tuple reused ──────────────────────────────────────────────
    [Fact]
    public async Task SkipTake_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int skip, int take) p) =>
            c.Query<Rec>().OrderBy(x => x.Id).Skip(p.skip).Take(p.take));
        foreach (var p in new[] { (0, 2), (2, 3), (4, 5), (1, 1), (5, 10) })
        {
            var lp = p;
            var oracle = (await ctx.Query<Rec>().OrderBy(x => x.Id).Skip(lp.Item1).Take(lp.Item2).ToListAsync()).Select(r => r.Id).ToArray();
            var actual = (await compiled(ctx, p)).Select(r => r.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }

    // ── 6. DateTime param reused ───────────────────────────────────────────────
    [Fact]
    public async Task DateTime_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, DateTime d) => c.Query<Rec>().Where(x => x.Created < d));
        foreach (var d in new[] { new DateTime(2021, 1, 1), new DateTime(2025, 1, 1), new DateTime(2019, 1, 1), new DateTime(2022, 6, 1) })
        {
            var ld = d;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Created < ld).ToListAsync());
            var actual = Ids(await compiled(ctx, d));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 7. Guid param reused ───────────────────────────────────────────────────
    [Fact]
    public async Task Guid_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, Guid g) => c.Query<Rec>().Where(x => x.Uid == g));
        foreach (var g in new[] { Uids[0], Uids[3], Guid.NewGuid(), Uids[5] })
        {
            var lg = g;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Uid == lg).ToListAsync());
            var actual = Ids(await compiled(ctx, g));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 8. bool param reused ───────────────────────────────────────────────────
    [Fact]
    public async Task Bool_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, bool b) => c.Query<Rec>().Where(x => x.Active == b));
        foreach (var b in new[] { true, false, true, false })
        {
            var lb = b;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Active == lb).ToListAsync());
            var actual = Ids(await compiled(ctx, b));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 9. decimal param reused ────────────────────────────────────────────────
    [Fact]
    public async Task Decimal_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, decimal p) => c.Query<Rec>().Where(x => x.Price >= p));
        foreach (var p in new[] { 0m, 10m, 50m, 100m, 5.00m, 9.99m })
        {
            var lp = p;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Price >= lp).ToListAsync());
            var actual = Ids(await compiled(ctx, p));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 10. enum param reused (int-backed, no converter) ───────────────────────
    [Fact]
    public async Task Enum_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, Kind k) => c.Query<Rec>().Where(x => x.Kind == k));
        foreach (var k in new[] { Kind.Alpha, Kind.Beta, Kind.Gamma, Kind.Alpha })
        {
            var lk = k;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Kind == lk).ToListAsync());
            var actual = Ids(await compiled(ctx, k));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 11. Two params must not swap or stick ──────────────────────────────────
    [Fact]
    public async Task TwoParams_ScoreAndKind_NoSwap_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int score, Kind kind) p) =>
            c.Query<Rec>().Where(x => x.Score == p.score && x.Kind == p.kind));
        foreach (var p in new[] { (30, Kind.Gamma), (20, Kind.Beta), (10, Kind.Alpha), (30, Kind.Alpha), (40, Kind.Alpha) })
        {
            var lp = p;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score == lp.Item1 && x.Kind == lp.Item2).ToListAsync());
            var actual = Ids(await compiled(ctx, p));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 12. Terminal Count(predicate) reused ───────────────────────────────────
    [Fact]
    public async Task TerminalCount_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<Rec>().Count(x => x.Score >= minScore));
        foreach (var minScore in new[] { 0, 20, 30, 40, 100 })
        {
            int lm = minScore;
            var oracle = await ctx.Query<Rec>().CountAsync(x => x.Score >= lm);
            var actual = await compiled(ctx, minScore);
            Assert.Equal(oracle, actual);
        }
    }

    // ── 13. Terminal First(predicate) reused (entity result) ───────────────────
    [Fact]
    public async Task TerminalFirst_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileTerminalQuery((DbContext c, int id) =>
            c.Query<Rec>().First(x => x.Id == id));
        foreach (var id in new[] { 1, 4, 6, 2 })
        {
            int lid = id;
            var oracle = await ctx.Query<Rec>().Where(x => x.Id == lid).FirstAsync();
            var actual = await compiled(ctx, id);
            Assert.Equal(oracle.Id, actual.Id);
            Assert.Equal(oracle.Name, actual.Name);
        }
    }

    // ── 14. Interleaved invocations A/B/A must not cross-contaminate ────────────
    [Fact]
    public async Task Interleaved_Invocations_NoCrossContamination()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int score) => c.Query<Rec>().Where(x => x.Score == score));
        var a = Ids(await compiled(ctx, 20));
        var b = Ids(await compiled(ctx, 30));
        var a2 = Ids(await compiled(ctx, 20));
        var b2 = Ids(await compiled(ctx, 30));
        var oracleA = Ids(await ctx.Query<Rec>().Where(x => x.Score == 20).ToListAsync());
        var oracleB = Ids(await ctx.Query<Rec>().Where(x => x.Score == 30).ToListAsync());
        Assert.Equal(oracleA, a);
        Assert.Equal(oracleB, b);
        Assert.Equal(oracleA, a2);
        Assert.Equal(oracleB, b2);
    }

    // ── 15. Param + captured constant mixed ────────────────────────────────────
    [Fact]
    public async Task ParamPlusCapturedConstant_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        int cutoff = 15; // captured constant baked into the plan
        var compiled = Norm.CompileQuery((DbContext c, int k) =>
            c.Query<Rec>().Where(x => x.Score > cutoff && x.NVal == k));
        foreach (var k in new[] { 5, 10, 15, 5 })
        {
            int lk = k, lc = cutoff;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score > lc && x.NVal == lk).ToListAsync());
            var actual = Ids(await compiled(ctx, k));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 16. String LIKE (StartsWith) param reused ──────────────────────────────
    [Fact]
    public async Task StringStartsWith_Param_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, string prefix) =>
            c.Query<Rec>().Where(x => x.Name != null && x.Name.StartsWith(prefix)));
        foreach (var prefix in new[] { "a", "ap", "b", "c", "z" })
        {
            string lp = prefix;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Name != null && x.Name.StartsWith(lp)).ToListAsync());
            var actual = Ids(await compiled(ctx, prefix));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 17. Param in IN-list via captured-list Contains stays correct across param ─
    [Fact]
    public async Task ScalarParam_WithContainsList_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        // param is a scalar score threshold; a separate captured constant list is IN-listed.
        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<Rec>().Where(x => x.Score >= minScore && (x.Kind == Kind.Alpha || x.Kind == Kind.Gamma)));
        foreach (var minScore in new[] { 0, 10, 30, 40 })
        {
            int lm = minScore;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score >= lm && (x.Kind == Kind.Alpha || x.Kind == Kind.Gamma)).ToListAsync());
            var actual = Ids(await compiled(ctx, minScore));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 18. Param in correlated subquery / EXISTS reused ───────────────────────
    [Fact]
    public async Task Param_InCorrelatedSubquery_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        // Any() correlated on the same table with a param threshold.
        var compiled = Norm.CompileQuery((DbContext c, int threshold) =>
            c.Query<Rec>().Where(x => c.Query<Rec>().Any(y => y.Kind == x.Kind && y.Score > threshold)));
        foreach (var threshold in new[] { 5, 25, 35, 100 })
        {
            int lt = threshold;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => ctx.Query<Rec>().Any(y => y.Kind == x.Kind && y.Score > lt)).ToListAsync());
            var actual = Ids(await compiled(ctx, threshold));
            Assert.Equal(oracle, actual);
        }
    }
}
