using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Optimistic-concurrency interleaving fuzzer: two contexts over the same
/// database run seeded sequences of mutate / delete / add / save / refresh. A
/// per-row version counter is the oracle — a save that includes any row loaded
/// before another context's committed write (or a row that no longer exists)
/// must throw <see cref="DbConcurrencyException"/> and apply NOTHING (the
/// transaction rolls back atomically); a save of only fresh rows must apply
/// exactly, and the client-managed token must let the same context save again.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OccInterleavingFuzzTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("OccFuzz_Test")]
    public class OccRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
        [System.ComponentModel.DataAnnotations.Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    private sealed class CtxState
    {
        public DbContext Ctx = null!;
        public Dictionary<int, (OccRow Row, int LoadedVersion)> View = new();
        public Dictionary<int, int> PendingVals = new();
        public HashSet<int> PendingDeletes = new();
        public List<(OccRow Row, int Val)> PendingAdds = new();
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    public async Task Interleaved_saves_conflict_exactly_when_stale(int seed)
    {
        var dbName = $"occfuzz_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE OccFuzz_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL, Token BLOB)";
            cmd.ExecuteNonQuery();
        }

        DbContext OpenCtx()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider());
        }

        await RunOccMachineAsync(OpenCtx, seed, steps: 160);
    }

    /// <summary>Two-context OCC machine body, shared with the live-provider variant.</summary>
    internal static async Task RunOccMachineAsync(Func<DbContext> openCtx, int seed, int steps)
    {
        var rng = new Random(seed);
        var committedVal = new Dictionary<int, int>();
        var version = new Dictionary<int, int>();
        var nextKey = 1;
        var trace = new List<string>();
        string Tail() => "\nops:\n" + string.Join("\n", trace.TakeLast(40));

        var ctxs = new[] { new CtxState(), new CtxState() };
        ctxs[0].Ctx = openCtx();
        ctxs[1].Ctx = openCtx();

        async Task RefreshAsync(int i)
        {
            var s = ctxs[i];
            s.Ctx.ChangeTracker.Clear();
            s.View = new Dictionary<int, (OccRow, int)>();
            foreach (var row in await s.Ctx.Query<OccRow>().ToListAsync())
                s.View[row.Id] = (row, version[row.Id]);
            s.PendingVals.Clear();
            s.PendingDeletes.Clear();
            s.PendingAdds.Clear();
        }

        async Task VerifyCommittedAsync(string context)
        {
            using var verifyCtx = openCtx();
            var rows = (await verifyCtx.Query<OccRow>().ToListAsync()).OrderBy(r => r.Id).ToList();
            var expected = committedVal.OrderBy(kv => kv.Key).ToList();
            Assert.True(rows.Count == expected.Count,
                $"row count mismatch {context}: db={rows.Count} model={expected.Count}\n" +
                $"db: [{string.Join(",", rows.Select(r => r.Id))}]\nmodel: [{string.Join(",", expected.Select(kv => kv.Key))}]{Tail()}");
            for (var i = 0; i < rows.Count; i++)
            {
                Assert.True(rows[i].Id == expected[i].Key && rows[i].Val == expected[i].Value,
                    $"row mismatch {context} at Id={expected[i].Key}: db=({rows[i].Id},{rows[i].Val}) model=({expected[i].Key},{expected[i].Value}){Tail()}");
            }
        }

        // Seed a handful of rows.
        for (var k = 0; k < 6; k++)
        {
            var key = nextKey++;
            var val = rng.Next(-100, 100);
            ctxs[0].Ctx.Add(new OccRow { Id = key, Val = val });
            committedVal[key] = val;
            version[key] = 1;
        }
        await ctxs[0].Ctx.SaveChangesAsync();
        await RefreshAsync(0);
        await RefreshAsync(1);

        try
        {
            for (var step = 0; step < steps; step++)
            {
                var i = rng.Next(2);
                var s = ctxs[i];
                switch (rng.Next(10))
                {
                    case 0 or 1 or 2: // mutate a viewed row
                    {
                        var keys = s.View.Keys.Where(k => !s.PendingDeletes.Contains(k)).ToList();
                        if (keys.Count == 0) break;
                        var key = keys[rng.Next(keys.Count)];
                        var val = rng.Next(-100, 100);
                        s.View[key].Row.Val = val;
                        s.PendingVals[key] = val;
                        trace.Add($"{step}: ctx{i} mutate {key} -> {val}");
                        break;
                    }
                    case 3: // delete a viewed row
                    {
                        var keys = s.View.Keys.Where(k => !s.PendingDeletes.Contains(k)).ToList();
                        if (keys.Count == 0) break;
                        var key = keys[rng.Next(keys.Count)];
                        s.Ctx.Remove(s.View[key].Row);
                        s.PendingDeletes.Add(key);
                        s.PendingVals.Remove(key);
                        trace.Add($"{step}: ctx{i} delete {key}");
                        break;
                    }
                    case 4: // add a new row
                    {
                        var key = nextKey++;
                        var val = rng.Next(-100, 100);
                        var row = new OccRow { Id = key, Val = val };
                        s.Ctx.Add(row);
                        s.PendingAdds.Add((row, val));
                        trace.Add($"{step}: ctx{i} add {key}");
                        break;
                    }
                    case 5 or 6 or 7: // save: the version oracle decides conflict vs apply
                    {
                        var touched = s.PendingVals.Keys.Concat(s.PendingDeletes).Distinct().ToList();
                        var stale = touched.Any(k =>
                            !version.TryGetValue(k, out var v) || s.View[k].LoadedVersion != v);
                        if (touched.Count == 0 && s.PendingAdds.Count == 0)
                        {
                            await s.Ctx.SaveChangesAsync(); // empty save is a no-op
                            trace.Add($"{step}: ctx{i} empty save");
                            break;
                        }

                        if (stale)
                        {
                            trace.Add($"{step}: ctx{i} save (stale -> conflict)");
                            await Assert.ThrowsAsync<DbConcurrencyException>(() => s.Ctx.SaveChangesAsync());
                            // Atomic rollback: nothing may have applied.
                            await VerifyCommittedAsync($"seed={seed} step={step} (after conflicted save of ctx{i})");
                            await RefreshAsync(i);
                        }
                        else
                        {
                            trace.Add($"{step}: ctx{i} save ok [{string.Join(",", touched)}] +{s.PendingAdds.Count} adds");
                            await s.Ctx.SaveChangesAsync();
                            foreach (var (key, val) in s.PendingVals)
                            {
                                committedVal[key] = val;
                                version[key]++;
                                s.View[key] = (s.View[key].Row, version[key]);
                            }
                            foreach (var key in s.PendingDeletes)
                            {
                                committedVal.Remove(key);
                                version.Remove(key);
                                s.View.Remove(key);
                            }
                            foreach (var (row, val) in s.PendingAdds)
                            {
                                committedVal[row.Id] = val;
                                version[row.Id] = 1;
                                s.View[row.Id] = (row, 1);
                            }
                            s.PendingVals.Clear();
                            s.PendingDeletes.Clear();
                            s.PendingAdds.Clear();
                            await VerifyCommittedAsync($"seed={seed} step={step} (after save of ctx{i})");
                        }
                        break;
                    }
                    default: // refresh
                    {
                        trace.Add($"{step}: ctx{i} refresh");
                        await RefreshAsync(i);
                        break;
                    }
                }
            }
        }
        finally
        {
            ctxs[0].Ctx.Dispose();
            ctxs[1].Ctx.Dispose();
        }
    }
}

