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

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_TEMPORAL_FUZZ_SWEEP to "start:count" to run that seed range through the
    /// reconstruction machine. Unset, this fact is a no-op so the fixed seeds stay
    /// the baseline. Seeds here are wall-clock expensive (deliberate clock-gap
    /// delays), so sweep ranges should stay small relative to the other fuzzers.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_TEMPORAL_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], CultureInfo.InvariantCulture);
        for (var s = start; s < start + count; s++)
            await AsOf_reconstructs_every_checkpoint_state(s);
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
