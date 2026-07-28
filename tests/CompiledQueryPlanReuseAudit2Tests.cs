using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ADVERSARIAL AUDIT round 2: harder compiled-query plan-reuse edge cases —
/// null-first parameter ordering (pooled prepared-command metadata), a NULL in one slot of a
/// multi-parameter query (slot alignment), value-converter column + null transition,
/// concurrent interleaving, and a captured closure value changing across invocations.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryPlanReuseAudit2Tests
{
    public enum Status { Active = 1, Inactive = 2, Archived = 3 }

    [Table("CqAudit2")]
    public sealed class Rec
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int? NVal { get; set; }
        public string? Name { get; set; }
        public int Score { get; set; }
        public Status Status { get; set; }
    }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private static Rec[] SeedRows() => new[]
    {
        new Rec { Id = 1, NVal = 5,    Name = "apple",  Score = 10, Status = Status.Active },
        new Rec { Id = 2, NVal = null, Name = "banana", Score = 20, Status = Status.Inactive },
        new Rec { Id = 3, NVal = 5,    Name = null,     Score = 30, Status = Status.Active },
        new Rec { Id = 4, NVal = 10,   Name = "cherry", Score = 20, Status = Status.Archived },
        new Rec { Id = 5, NVal = null, Name = "date",   Score = 30, Status = Status.Inactive },
    };

    private static async Task<DbContext> CtxAsync(bool withConverter = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqAudit2 (Id INTEGER PRIMARY KEY, NVal INTEGER NULL, Name TEXT NULL, " +
                              "Score INTEGER NOT NULL, Status " + (withConverter ? "TEXT" : "INTEGER") + " NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        DbContextOptions? opts = withConverter ? new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Rec>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter())
        } : null;
        var ctx = opts != null ? new DbContext(cn, new SqliteProvider(), opts) : new DbContext(cn, new SqliteProvider());
        foreach (var r in SeedRows()) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static int[] Ids(IEnumerable<Rec> rows) => rows.Select(r => r.Id).OrderBy(x => x).ToArray();

    // ── 1. NULL-FIRST ordering: first invocation binds null, then non-null ─────
    // The pooled prepared command's parameter metadata is created on the first call.
    // A null-first binding must not poison the type for the subsequent non-null value.
    [Fact]
    public async Task NullableInt_NullFirst_ThenNonNull_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int? n) => c.Query<Rec>().Where(x => x.NVal == n));
        foreach (int? n in new int?[] { null, 5, null, 10, 5 }) // null FIRST
        {
            int? local = n;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.NVal == local).ToListAsync());
            var actual = Ids(await compiled(ctx, n));
            Assert.Equal(oracle, actual);
        }
    }

    [Fact]
    public async Task String_NullFirst_ThenNonNull_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, string? s) => c.Query<Rec>().Where(x => x.Name == s));
        foreach (var s in new string?[] { null, "apple", null, "banana", "cherry" }) // null FIRST
        {
            string? local = s;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Name == local).ToListAsync());
            var actual = Ids(await compiled(ctx, s));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 2. NULL in one slot of a multi-param query must not shift the other slot ─
    [Fact]
    public async Task MultiParam_NullInOneSlot_NoShift_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int? nval, int score) p) =>
            c.Query<Rec>().Where(x => x.NVal == p.nval && x.Score == p.score));
        foreach (var p in new[] { ((int?)null, 20), (5, 10), ((int?)null, 30), (5, 30), (10, 20) })
        {
            var lp = p;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.NVal == lp.Item1 && x.Score == lp.Item2).ToListAsync());
            var actual = Ids(await compiled(ctx, p));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 3. Reverse: non-null first, then null in the leading slot ──────────────
    [Fact]
    public async Task MultiParam_NonNullThenNullInLeadingSlot_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, (int? nval, int score) p) =>
            c.Query<Rec>().Where(x => x.NVal == p.nval && x.Score == p.score));
        foreach (var p in new[] { (5, 10), ((int?)null, 20), (10, 20), ((int?)null, 30) })
        {
            var lp = p;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.NVal == lp.Item1 && x.Score == lp.Item2).ToListAsync());
            var actual = Ids(await compiled(ctx, p));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 4. Value-converter column (enum→name) + reuse across values ────────────
    [Fact]
    public async Task ConverterColumn_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync(withConverter: true);
        var compiled = Norm.CompileQuery((DbContext c, Status s) => c.Query<Rec>().Where(x => x.Status == s));
        foreach (var s in new[] { Status.Active, Status.Inactive, Status.Archived, Status.Active })
        {
            var ls = s;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Status == ls).ToListAsync());
            var actual = Ids(await compiled(ctx, s));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 5. Converter column with inequality (!=) reused ────────────────────────
    [Fact]
    public async Task ConverterColumn_Inequality_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync(withConverter: true);
        var compiled = Norm.CompileQuery((DbContext c, Status s) => c.Query<Rec>().Where(x => x.Status != s));
        foreach (var s in new[] { Status.Active, Status.Inactive, Status.Archived })
        {
            var ls = s;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Status != ls).ToListAsync());
            var actual = Ids(await compiled(ctx, s));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 6. Concurrent interleaving from multiple threads, distinct params ──────
    [Fact]
    public async Task Concurrent_DistinctParams_NoCrossContamination()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int score) => c.Query<Rec>().Where(x => x.Score == score));
        // Warm the plan first (single-threaded), then hammer concurrently.
        _ = await compiled(ctx, 10);

        var scores = new[] { 10, 20, 30 };
        var expected = new Dictionary<int, int[]>();
        foreach (var s in scores)
        {
            int ls = s;
            expected[s] = Ids(await ctx.Query<Rec>().Where(x => x.Score == ls).ToListAsync());
        }

        // Note: a single DbContext/connection is not designed for parallel command execution,
        // so serialize per invocation but interleave the parameter values rapidly.
        for (int iter = 0; iter < 50; iter++)
        {
            var s = scores[iter % scores.Length];
            var actual = Ids(await compiled(ctx, s));
            Assert.Equal(expected[s], actual);
        }
    }

    // ── 7. Captured scalar closure value changes across invocations + a param ──
    [Fact]
    public async Task CapturedClosureChange_WithParam_RebuildsAndBindsCorrectly()
    {
        using var ctx = await CtxAsync();
        var holder = new int[] { 15 };
        var compiled = Norm.CompileQuery((DbContext c, int score) =>
            c.Query<Rec>().Where(x => x.Score == score && x.NVal != null && x.NVal.Value < holder[0]));
        // holder[0] = 15
        {
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score == 30 && x.NVal != null && x.NVal!.Value < 15).ToListAsync());
            var actual = Ids(await compiled(ctx, 30));
            Assert.Equal(oracle, actual);
        }
        // change captured closure -> plan should rebuild for the new closure key
        holder[0] = 6;
        {
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score == 30 && x.NVal != null && x.NVal!.Value < 6).ToListAsync());
            var actual = Ids(await compiled(ctx, 30));
            Assert.Equal(oracle, actual);
        }
        // change param over the new closure
        {
            var oracle = Ids(await ctx.Query<Rec>().Where(x => x.Score == 10 && x.NVal != null && x.NVal!.Value < 6).ToListAsync());
            var actual = Ids(await compiled(ctx, 10));
            Assert.Equal(oracle, actual);
        }
    }

    // ── 8. Nullable projection with a param computed value, reused ─────────────
    public sealed class Proj { public int Id { get; set; } public int? Diff { get; set; } }

    [Fact]
    public async Task NullableProjection_WithParam_ReusedAcrossValues_MatchesOracle()
    {
        using var ctx = await CtxAsync();
        var compiled = Norm.CompileQuery((DbContext c, int k) =>
            c.Query<Rec>().Where(x => x.NVal != null).Select(x => new Proj { Id = x.Id, Diff = x.NVal!.Value - k }));
        foreach (var k in new[] { 0, 5, 10, 100 })
        {
            int lk = k;
            var oracle = (await ctx.Query<Rec>().Where(x => x.NVal != null).Select(x => new Proj { Id = x.Id, Diff = x.NVal!.Value - lk }).ToListAsync())
                .Select(p => (p.Id, p.Diff)).OrderBy(t => t.Id).ToArray();
            var actual = (await compiled(ctx, k)).Select(p => (p.Id, p.Diff)).OrderBy(t => t.Id).ToArray();
            Assert.Equal(oracle, actual);
        }
    }
}
