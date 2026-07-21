using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Seeded oracle machine for temporal history integrity: random rounds of
/// adds, updates, deletes, and same-key re-adds, with a checkpoint taken from
/// the DATABASE clock and a full oracle state snapshot after every SaveChanges.
/// Every checkpoint is then replayed through AsOf and must reconstruct the
/// exact state — a missed version, an unclosed validity window, or a wrong-row
/// ValidTo close surfaces as a reconstruction mismatch at some point in time.
/// Checkpoints use the server clock so the machine also runs against live
/// providers without client/server clock-skew false positives.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalHistoryReconstructionFuzzTests
{
    [Table("ThrRow_Test")]
    internal class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public string S { get; set; } = "";
    }

    internal static async Task RunReconstructionFuzzAsync(DbContext ctx, int seed, int rounds, Func<Task<DateTime>> serverNowAsync)
    {
        var rng = new Random(seed);
        var tracked = new Dictionary<int, Row>();
        var model = new Dictionary<int, (int V, string S)>();
        var checkpoints = new List<(DateTime Ts, Dictionary<int, (int V, string S)> State)>();
        var nextId = 1;

        await Task.Delay(50);
        var beforeAll = await serverNowAsync();
        await Task.Delay(50);

        for (var round = 0; round < rounds; round++)
        {
            var mutations = rng.Next(1, 4);
            for (var m = 0; m < mutations; m++)
            {
                var op = rng.Next(3);
                if (op == 0 || model.Count == 0)
                {
                    // Add — sometimes re-using a previously deleted key.
                    var reuseCandidates = Enumerable.Range(1, nextId - 1).Where(id => !model.ContainsKey(id)).ToList();
                    int id;
                    if (reuseCandidates.Count > 0 && rng.Next(3) == 0)
                        id = reuseCandidates[rng.Next(reuseCandidates.Count)];
                    else
                        id = nextId++;
                    var row = new Row { Id = id, V = rng.Next(1000), S = "s" + rng.Next(1000) };
                    ctx.Add(row);
                    tracked[id] = row;
                    model[id] = (row.V, row.S);
                }
                else if (op == 1)
                {
                    var id = model.Keys.ElementAt(rng.Next(model.Count));
                    var row = tracked[id];
                    row.V = rng.Next(1000);
                    row.S = "s" + rng.Next(1000);
                    model[id] = (row.V, row.S);
                }
                else
                {
                    var id = model.Keys.ElementAt(rng.Next(model.Count));
                    ctx.Remove(tracked[id]);
                    tracked.Remove(id);
                    model.Remove(id);
                }
            }

            await ctx.SaveChangesAsync();
            await Task.Delay(50);
            checkpoints.Add((await serverNowAsync(), new Dictionary<int, (int, string)>(model)));
            await Task.Delay(50);
        }

        // Before any save: empty world.
        var initial = await ctx.Query<Row>().AsOf(beforeAll).ToListAsync();
        Assert.True(initial.Count == 0, $"seed={seed}: AsOf(beforeAll) returned {initial.Count} rows");

        foreach (var (ts, state) in checkpoints)
        {
            var reconstructed = (await ctx.Query<Row>().AsOf(ts).ToListAsync())
                .ToDictionary(r => r.Id, r => (r.V, r.S));
            var expectedKeys = state.Keys.OrderBy(k => k).ToList();
            var actualKeys = reconstructed.Keys.OrderBy(k => k).ToList();
            Assert.True(expectedKeys.SequenceEqual(actualKeys),
                $"seed={seed} ts={ts:O}: row-set mismatch — expected [{string.Join(",", expectedKeys)}] got [{string.Join(",", actualKeys)}]");
            foreach (var id in expectedKeys)
            {
                Assert.True(state[id] == reconstructed[id],
                    $"seed={seed} ts={ts:O} id={id}: expected {state[id]} got {reconstructed[id]}");
            }
        }
    }

    [Table("ThrDept_Test")]
    internal class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("ThrEmp_Test")]
    internal class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int DeptId { get; set; }
        public Dept? Dept { get; set; }
    }

    /// <summary>
    /// Relational reconstruction machine: a randomly mutated principal/dependent
    /// graph (dept adds/renames/childless deletes; emp adds/renames/re-parents/
    /// deletes) checkpointed on the server clock, then every checkpoint replayed
    /// through the RELATIONAL query shapes — navigation projection, navigation
    /// predicate, SelectMany flatten, correlated aggregate, Include, a set
    /// operation, and a Contains subquery — each compared against the oracle
    /// state. This is the shape coverage the single-table machine lacks: an
    /// era-mixing leak in ANY table reference of a composite statement surfaces
    /// as a value or membership mismatch at some checkpoint.
    /// </summary>
    internal static async Task RunRelationalReconstructionFuzzAsync(DbContext ctx, int seed, int rounds, Func<Task<DateTime>> serverNowAsync)
    {
        var rng = new Random(seed);
        var trackedDepts = new Dictionary<int, Dept>();
        var trackedEmps = new Dictionary<int, Emp>();
        var depts = new Dictionary<int, string>();
        var emps = new Dictionary<int, (string Name, int DeptId)>();
        var checkpoints = new List<(DateTime Ts, Dictionary<int, string> Depts, Dictionary<int, (string Name, int DeptId)> Emps)>();
        var nextDeptId = 1;
        var nextEmpId = 1;

        for (var round = 0; round < rounds; round++)
        {
            var mutations = rng.Next(1, 4);
            for (var m = 0; m < mutations; m++)
            {
                var op = rng.Next(7);
                if (op == 0 || depts.Count == 0)
                {
                    var d = new Dept { Id = nextDeptId++, Title = "t" + rng.Next(1000) };
                    ctx.Add(d);
                    trackedDepts[d.Id] = d;
                    depts[d.Id] = d.Title;
                }
                else if (op == 1)
                {
                    var id = depts.Keys.ElementAt(rng.Next(depts.Count));
                    var d = trackedDepts[id];
                    d.Title = "t" + rng.Next(1000);
                    depts[id] = d.Title;
                }
                else if (op == 2)
                {
                    // Delete only childless depts so every emp's principal always exists.
                    var childless = depts.Keys.Where(id => !emps.Values.Any(e => e.DeptId == id)).ToList();
                    if (childless.Count == 0) continue;
                    var id = childless[rng.Next(childless.Count)];
                    ctx.Remove(trackedDepts[id]);
                    trackedDepts.Remove(id);
                    depts.Remove(id);
                }
                else if (op == 3 || emps.Count == 0)
                {
                    var deptId = depts.Keys.ElementAt(rng.Next(depts.Count));
                    var e = new Emp { Id = nextEmpId++, Name = "n" + rng.Next(1000), DeptId = deptId };
                    ctx.Add(e);
                    trackedEmps[e.Id] = e;
                    emps[e.Id] = (e.Name, deptId);
                }
                else if (op == 4)
                {
                    var id = emps.Keys.ElementAt(rng.Next(emps.Count));
                    var e = trackedEmps[id];
                    e.Name = "n" + rng.Next(1000);
                    emps[id] = (e.Name, emps[id].DeptId);
                }
                else if (op == 5)
                {
                    var id = emps.Keys.ElementAt(rng.Next(emps.Count));
                    var e = trackedEmps[id];
                    var deptId = depts.Keys.ElementAt(rng.Next(depts.Count));
                    e.DeptId = deptId;
                    emps[id] = (emps[id].Name, deptId);
                }
                else
                {
                    var id = emps.Keys.ElementAt(rng.Next(emps.Count));
                    ctx.Remove(trackedEmps[id]);
                    trackedEmps.Remove(id);
                    emps.Remove(id);
                }
            }

            await ctx.SaveChangesAsync();
            await Task.Delay(50);
            checkpoints.Add((await serverNowAsync(),
                new Dictionary<int, string>(depts),
                new Dictionary<int, (string, int)>(emps)));
            await Task.Delay(50);
        }

        foreach (var (ts, deptState, empState) in checkpoints)
        {
            // Navigation projection: every emp with its principal's ERA title.
            var navProj = (await ctx.Query<Emp>().AsOf(ts)
                    .Select(e => new { e.Id, e.Name, DeptTitle = e.Dept!.Title }).ToListAsync())
                .OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.Name}:{r.DeptTitle}").ToList();
            var navProjExpected = empState.OrderBy(kv => kv.Key)
                .Select(kv => $"{kv.Key}:{kv.Value.Name}:{deptState[kv.Value.DeptId]}").ToList();
            Assert.True(navProjExpected.SequenceEqual(navProj),
                $"seed={seed} ts={ts:O} navProj: expected [{string.Join(" ", navProjExpected)}] got [{string.Join(" ", navProj)}]");

            // Navigation predicate against a title drawn from the era state.
            if (deptState.Count > 0)
            {
                var title = deptState.Values.ElementAt(rng.Next(deptState.Count));
                var navPred = (await ctx.Query<Emp>().AsOf(ts)
                        .Where(e => e.Dept!.Title == title).ToListAsync())
                    .Select(e => e.Id).OrderBy(id => id).ToList();
                var navPredExpected = empState
                    .Where(kv => deptState[kv.Value.DeptId] == title)
                    .Select(kv => kv.Key).OrderBy(id => id).ToList();
                Assert.True(navPredExpected.SequenceEqual(navPred),
                    $"seed={seed} ts={ts:O} navPred('{title}'): expected [{string.Join(",", navPredExpected)}] got [{string.Join(",", navPred)}]");
            }

            // SelectMany flatten: era membership of every principal's collection.
            var flat = (await ctx.Query<Dept>().AsOf(ts).SelectMany(d => d.Emps).ToListAsync())
                .Select(e => e.Id).OrderBy(id => id).ToList();
            var flatExpected = empState.Keys.OrderBy(id => id).ToList();
            Assert.True(flatExpected.SequenceEqual(flat),
                $"seed={seed} ts={ts:O} selectMany: expected [{string.Join(",", flatExpected)}] got [{string.Join(",", flat)}]");

            // Correlated aggregate: era counts per principal.
            var counts = (await ctx.Query<Dept>().AsOf(ts)
                    .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync())
                .OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.N}").ToList();
            var countsExpected = deptState.Keys.OrderBy(id => id)
                .Select(id => $"{id}:{empState.Values.Count(e => e.DeptId == id)}").ToList();
            Assert.True(countsExpected.SequenceEqual(counts),
                $"seed={seed} ts={ts:O} corrCount: expected [{string.Join(" ", countsExpected)}] got [{string.Join(" ", counts)}]");

            // Include: era membership AND era values of the loaded children.
            var included = await ((INormQueryable<Dept>)ctx.Query<Dept>())
                .Include(d => d.Emps).AsOf(ts).ToListAsync();
            var includedFlat = included.OrderBy(d => d.Id)
                .Select(d => d.Id + "=" + string.Join("|", d.Emps.OrderBy(e => e.Id).Select(e => $"{e.Id}:{e.Name}")))
                .ToList();
            var includedExpected = deptState.Keys.OrderBy(id => id)
                .Select(id => id + "=" + string.Join("|", empState
                    .Where(kv => kv.Value.DeptId == id).OrderBy(kv => kv.Key)
                    .Select(kv => $"{kv.Key}:{kv.Value.Name}")))
                .ToList();
            Assert.True(includedExpected.SequenceEqual(includedFlat),
                $"seed={seed} ts={ts:O} include: expected [{string.Join(" ", includedExpected)}] got [{string.Join(" ", includedFlat)}]");

            // Set operation: both arms window, so the union is exactly the era set.
            var union = (await ctx.Query<Emp>().AsOf(ts).Where(e => e.Id % 2 == 0)
                    .Union(ctx.Query<Emp>().Where(e => e.Id % 2 == 1)).ToListAsync())
                .Select(e => $"{e.Id}:{e.Name}").OrderBy(s => s, StringComparer.Ordinal).ToList();
            var unionExpected = empState.OrderBy(kv => kv.Key)
                .Select(kv => $"{kv.Key}:{kv.Value.Name}").OrderBy(s => s, StringComparer.Ordinal).ToList();
            Assert.True(unionExpected.SequenceEqual(union),
                $"seed={seed} ts={ts:O} union: expected [{string.Join(" ", unionExpected)}] got [{string.Join(" ", union)}]");

            // Contains over a mapped inner query: the inner windows with the statement.
            if (deptState.Count > 0)
            {
                var title = deptState.Values.ElementAt(rng.Next(deptState.Count));
                var contains = (await ctx.Query<Emp>().AsOf(ts)
                        .Where(e => ctx.Query<Dept>().Where(d => d.Title == title).Select(d => d.Id).Contains(e.DeptId))
                        .ToListAsync())
                    .Select(e => e.Id).OrderBy(id => id).ToList();
                var containsExpected = empState
                    .Where(kv => deptState[kv.Value.DeptId] == title)
                    .Select(kv => kv.Key).OrderBy(id => id).ToList();
                Assert.True(containsExpected.SequenceEqual(contains),
                    $"seed={seed} ts={ts:O} contains('{title}'): expected [{string.Join(",", containsExpected)}] got [{string.Join(",", contains)}]");
            }
        }
    }

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_TEMPORAL_FUZZ_SWEEP to "start:count" to run that seed range through
    /// BOTH reconstruction machines (single-table and relational). Unset, this
    /// fact is a no-op so the fixed seeds stay the baseline. Seeds here are
    /// wall-clock expensive (deliberate clock-gap delays), so sweep ranges should
    /// stay small relative to the other fuzzers.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_TEMPORAL_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], CultureInfo.InvariantCulture);
        var dop = parts.Length > 2 ? int.Parse(parts[2], CultureInfo.InvariantCulture) : Environment.ProcessorCount;
        // Seeds are isolated (own :memory: connection + per-context options; temporal config sets
        // instance fields only). Their deliberate clock-gap DELAYS make parallelism especially
        // valuable — the waits overlap across cores. Optional ":dop" third field.
        var options = new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, dop) };
        await System.Threading.Tasks.Parallel.ForEachAsync(
            System.Linq.Enumerable.Range(start, count), options,
            async (s, _) =>
            {
                await AsOf_reconstructs_every_checkpoint_state(s);
                await AsOf_reconstructs_relational_graph_at_every_checkpoint(s);
            });
    }

    [Theory]
    [InlineData(20260717)]
    [InlineData(4242)]
    [InlineData(715_301_998)]
    public async Task AsOf_reconstructs_relational_graph_at_every_checkpoint(int seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ThrDept_Test (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE ThrEmp_Test (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Dept>().HasMany(d => d.Emps).WithOne(e => e.Dept!).HasForeignKey(e => e.DeptId, d => d.Id);
            }
        };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        await RunRelationalReconstructionFuzzAsync(ctx, seed, rounds: 8, async () =>
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(
                DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None),
                DateTimeKind.Utc);
        });
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(1337)]
    [InlineData(902_211_558)]
    public async Task AsOf_reconstructs_every_checkpoint_state(int seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE ThrRow_Test (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, S TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        await RunReconstructionFuzzAsync(ctx, seed, rounds: 10, async () =>
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(
                DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None),
                DateTimeKind.Utc);
        });
    }
}
