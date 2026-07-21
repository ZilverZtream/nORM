using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Differential cache-staleness fuzzer. For each seed it builds a random Parent/Child dataset,
/// warms a random Cacheable query shape, applies a random write to a table that shape reads, then
/// asserts the cached context's re-run matches a fresh (uncached) context computed against the
/// same shared database. Any mismatch means a write failed to invalidate the cache entry - a
/// silent stale read. This complements the targeted correlated/multi-table/navigation invalidation
/// tests, whose shapes are fixed; the cache-tag bug history shows staleness can hide from
/// happy-path probes.
/// </summary>
[Trait("Category", "Fast")]
public class CacheStalenessFuzzTests
{
    [Table("CsfParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Val { get; set; }
    }

    [Table("CsfChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task Cacheable_reads_never_go_stale_after_a_write_to_a_table_they_read()
    {
        for (var seed = 0; seed < 120; seed++)
            await RunSeed(seed);
    }

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_CACHE_FUZZ_SWEEP to "start:count" to run that seed range through the
    /// staleness machine. Unset, this fact is a no-op so the fixed range stays the baseline.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_CACHE_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        var dop = parts.Length > 2 ? int.Parse(parts[2], System.Globalization.CultureInfo.InvariantCulture) : Environment.ProcessorCount;
        // Seeds are isolated: each uses a unique shared-cache DB (csf_{seed}_{Guid}) and the query
        // result cache is keyed by connection string, so entries can't collide across seeds. Fan
        // out across cores; optional ":dop" third field.
        var options = new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, dop) };
        await System.Threading.Tasks.Parallel.ForEachAsync(
            System.Linq.Enumerable.Range(start, count), options,
            async (s, _) => await RunSeed(s));
    }

    private static async Task RunSeed(int seed)
    {
        var rng = new Random(seed);
        var keeper = new SqliteConnection($"Data Source=file:csf_{seed}_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        try
        {
            var parentCount = rng.Next(2, 5);
            var nextChildId = 1;
            using (var cmd = keeper.CreateCommand())
            {
                var sb = new StringBuilder();
                sb.Append("CREATE TABLE CsfParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Val INTEGER NOT NULL);");
                sb.Append("CREATE TABLE CsfChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);");
                for (var p = 1; p <= parentCount; p++)
                    sb.Append($"INSERT INTO CsfParent VALUES ({p},'p{p}',{rng.Next(0, 10)});");
                for (var p = 1; p <= parentCount; p++)
                {
                    var kids = rng.Next(0, 4);
                    for (var k = 0; k < kids; k++)
                        sb.Append($"INSERT INTO CsfChild VALUES ({nextChildId++},{p},{rng.Next(0, 10)});");
                }
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
            }
            var maxChildId = nextChildId - 1;

            using var cache = new NormMemoryCacheProvider();
            DbContext MakeCached() => NewContext(keeper.ConnectionString, cache);
            DbContext MakeFresh() => NewContext(keeper.ConnectionString, null);

            var shape = rng.Next(0, 5);
            var target = rng.Next(1, parentCount + 1);

            await using var ctx = MakeCached();
            _ = RunShape(ctx, shape, target, cacheable: true);          // warm the cache

            var write = await ApplyRandomWrite(ctx, rng, parentCount, maxChildId, nextChildId);

            var cached = RunShape(ctx, shape, target, cacheable: true);  // may be stale if not invalidated

            await using var fresh = MakeFresh();
            var truth = RunShape(fresh, shape, target, cacheable: false); // ground truth

            Assert.True(
                cached.SequenceEqual(truth),
                $"seed {seed}: cache stale after {write} (shape {shape}, target {target}). " +
                $"cached=[{Fmt(cached)}] truth=[{Fmt(truth)}]");
        }
        finally
        {
            keeper.Dispose();
        }
    }

    private static DbContext NewContext(string connString, NormMemoryCacheProvider? cache)
    {
        var cn = new SqliteConnection(connString);
        cn.Open();
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static List<(int Id, int M)> RunShape(DbContext ctx, int shape, int target, bool cacheable)
    {
        var q = ((INormQueryable<Parent>)ctx.Query<Parent>()).AsNoTracking();
        switch (shape)
        {
            case 0: // reads Parent only
            {
                var proj = q.OrderBy(p => p.Id).Select(p => new { p.Id, M = p.Val });
                var rows = cacheable ? proj.Cacheable(TimeSpan.FromMinutes(5)).ToList() : proj.ToList();
                return rows.Select(r => (r.Id, r.M)).ToList();
            }
            case 1: // correlated child count for one parent (reads Parent + Child)
            {
                var proj = q.Where(p => p.Id == target)
                    .Select(p => new { p.Id, M = ctx.Query<Child>().Count(c => c.ParentId == p.Id) });
                var rows = cacheable ? proj.Cacheable(TimeSpan.FromMinutes(5)).ToList() : proj.ToList();
                return rows.Select(r => (r.Id, r.M)).ToList();
            }
            case 2: // correlated child Val sum for all parents (reads Parent + Child)
            {
                var proj = q.OrderBy(p => p.Id)
                    .Select(p => new { p.Id, M = ctx.Query<Child>().Where(c => c.ParentId == p.Id).Sum(c => c.Val) });
                var rows = cacheable ? proj.Cacheable(TimeSpan.FromMinutes(5)).ToList() : proj.ToList();
                return rows.Select(r => (r.Id, r.M)).ToList();
            }
            case 3: // explicit JOIN projection (reads Parent + Child through the join, not a subquery)
            {
                var proj = q.Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.Id, M = c.Val })
                    .OrderBy(x => x.Id).ThenBy(x => x.M);
                var rows = cacheable ? proj.Cacheable(TimeSpan.FromMinutes(5)).ToList() : proj.ToList();
                return rows.Select(r => (r.Id, r.M)).ToList();
            }
            default: // window function over Parent (ROW_NUMBER through the ranked pipeline)
            {
                var proj = q.OrderBy(p => p.Id)
                    .WithRowNumber((p, rn) => new { p.Id, Rn = rn });
                var rows = cacheable ? proj.Cacheable(TimeSpan.FromMinutes(5)).ToList() : proj.ToList();
                return rows.Select(r => (r.Id, (int)r.Rn)).ToList();
            }
        }
    }

    private static async Task<string> ApplyRandomWrite(DbContext ctx, Random rng, int parentCount, int maxChildId, int nextChildId)
    {
        var op = rng.Next(0, 5);
        switch (op)
        {
            case 0: // tracked insert child
                ctx.Add(new Child { Id = nextChildId, ParentId = rng.Next(1, parentCount + 1), Val = rng.Next(0, 10) });
                await ctx.SaveChangesAsync();
                return $"insert child {nextChildId}";
            case 1: // tracked insert parent
                ctx.Add(new Parent { Id = parentCount + 1, Name = "pn", Val = rng.Next(0, 10) });
                await ctx.SaveChangesAsync();
                return $"insert parent {parentCount + 1}";
            case 2 when maxChildId >= 1: // bulk delete a child
            {
                var id = rng.Next(1, maxChildId + 1);
                await ctx.Query<Child>().Where(c => c.Id == id).ExecuteDeleteAsync();
                return $"delete child {id}";
            }
            case 3 when maxChildId >= 1: // direct update a child's Val
            {
                var id = rng.Next(1, maxChildId + 1);
                var child = ((INormQueryable<Child>)ctx.Query<Child>()).AsNoTracking()
                    .Where(c => c.Id == id).ToList().FirstOrDefault();
                if (child != null)
                {
                    child.Val += 1 + rng.Next(0, 10);
                    await ctx.UpdateAsync(child);
                    return $"update child {id}";
                }
                goto default;
            }
            default: // bulk delete a parent
            {
                var id = rng.Next(1, parentCount + 1);
                await ctx.Query<Parent>().Where(p => p.Id == id).ExecuteDeleteAsync();
                return $"delete parent {id}";
            }
        }
    }

    private static string Fmt(List<(int Id, int M)> rows) => string.Join(",", rows.Select(r => $"{r.Id}:{r.M}"));
}
