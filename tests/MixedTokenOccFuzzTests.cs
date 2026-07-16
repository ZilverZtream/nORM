using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Mixed-entity optimistic-concurrency machine: THREE contexts over the same
/// database interleave mutations of a [Timestamp]-protected entity and a plain
/// (no-token) entity, saving both in the same batch. The oracle enforces the
/// distinct contracts side by side:
/// <list type="bullet">
///   <item>Token rows: a save touching any row loaded before another context's
///     committed write (or a vanished row) throws <see cref="DbConcurrencyException"/>
///     and applies NOTHING — including the plain-entity changes in the same save
///     (transaction atomicity across entity types).</item>
///   <item>Plain rows: last-writer-wins. An update or delete of a row another
///     context already deleted silently affects zero rows — no conflict, the
///     change is simply lost — and never poisons the rest of the batch.</item>
/// </list>
/// A fault injector can throw a retryable failure at the Nth write execution of
/// a save: within the retry budget the save must land exactly once (client-managed
/// tokens stamped by a rolled-back attempt must be reset, or the retry would
/// false-conflict); past the budget the save must throw, apply nothing, and the
/// recovery save must land everything exactly once.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MixedTokenOccFuzzTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("MixTok_Test")]
    public class TokenRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
        [System.ComponentModel.DataAnnotations.Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MixPlain_Test")]
    public class PlainRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    internal sealed class InjectedRetryableException : DbException
    {
        public InjectedRetryableException() : base("injected retryable failure") { }
    }

    /// <summary>
    /// Lets the first <c>skipWrites</c> write executions pass, then throws; the
    /// countdown re-arms after each trip so every retry attempt fails again at
    /// the same depth until the armed trip count is spent. Reads never trip.
    /// </summary>
    internal sealed class NthWriteFailureInterceptor : BaseDbCommandInterceptor
    {
        private int _armedSkip;
        private int _writesUntilTrip = int.MaxValue;
        private int _tripsRemaining;

        public NthWriteFailureInterceptor() : base(NullLogger.Instance) { }

        public void Arm(int skipWrites, int trips)
        {
            _armedSkip = skipWrites;
            _writesUntilTrip = skipWrites;
            _tripsRemaining = trips;
        }

        public void Disarm()
        {
            _tripsRemaining = 0;
            _writesUntilTrip = int.MaxValue;
        }

        private void TripIfArmed(DbCommand command)
        {
            if (_tripsRemaining <= 0 || !IsWrite(command))
                return;
            if (_writesUntilTrip > 0)
            {
                _writesUntilTrip--;
                return;
            }
            _tripsRemaining--;
            _writesUntilTrip = _armedSkip;
            throw new InjectedRetryableException();
        }

        public override System.Threading.Tasks.Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, System.Threading.CancellationToken ct)
        {
            TripIfArmed(command);
            return base.NonQueryExecutingAsync(command, context, ct);
        }

        public override System.Threading.Tasks.Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, System.Threading.CancellationToken ct)
        {
            TripIfArmed(command);
            return base.ScalarExecutingAsync(command, context, ct);
        }

        public override System.Threading.Tasks.Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, System.Threading.CancellationToken ct)
        {
            TripIfArmed(command);
            return base.ReaderExecutingAsync(command, context, ct);
        }

        private static bool IsWrite(DbCommand command)
            => command.CommandText.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
               || command.CommandText.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
               || command.CommandText.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase);
    }

    internal static DbContextOptions FaultOptions(NthWriteFailureInterceptor injector)
    {
        var opts = new DbContextOptions
        {
            RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = ex => ex is InjectedRetryableException,
            },
        };
        opts.CommandInterceptors.Add(injector);
        return opts;
    }

    private sealed class CtxState
    {
        public DbContext Ctx = null!;
        // OrigVal is the change-detection baseline (as-loaded, or as-accepted by
        // the last successful save): a mutation back to it leaves the entity
        // Unchanged, so no write executes and no conflict can fire for that row.
        public Dictionary<int, (TokenRow Row, int LoadedVersion, int OrigVal)> ViewTok = new();
        public Dictionary<int, (PlainRow Row, int OrigVal)> ViewPlain = new();
        public Dictionary<int, int> PendingTokVals = new();
        public HashSet<int> PendingTokDeletes = new();
        public List<(TokenRow Row, int Val)> PendingTokAdds = new();
        public Dictionary<int, int> PendingPlainVals = new();
        public HashSet<int> PendingPlainDeletes = new();
        public List<(PlainRow Row, int Val)> PendingPlainAdds = new();

        public bool HasPending =>
            PendingTokVals.Count > 0 || PendingTokDeletes.Count > 0 || PendingTokAdds.Count > 0 ||
            PendingPlainVals.Count > 0 || PendingPlainDeletes.Count > 0 || PendingPlainAdds.Count > 0;

        /// <summary>
        /// Lower bound on the write EXECUTIONS the next save issues: each
        /// non-empty (entity type × operation) group executes at least one
        /// command, so any armed skip below this count is guaranteed to trip.
        /// </summary>
        public int MinWriteExecutions =>
            (PendingTokAdds.Count > 0 ? 1 : 0) + (PendingTokVals.Count > 0 ? 1 : 0) + (PendingTokDeletes.Count > 0 ? 1 : 0) +
            (PendingPlainAdds.Count > 0 ? 1 : 0) + (PendingPlainVals.Count > 0 ? 1 : 0) + (PendingPlainDeletes.Count > 0 ? 1 : 0);
    }

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_OCC_FUZZ_SWEEP to "start:count" to run that seed range through the mixed-token
    /// interleaving machine. Unset, this fact is a no-op so the fixed seeds stay the baseline.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_OCC_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        for (var s = start; s < start + count; s++)
            await Mixed_token_and_plain_saves_conflict_and_recover_exactly(s);
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    public async Task Mixed_token_and_plain_saves_conflict_and_recover_exactly(int seed)
    {
        var dbName = $"mixocc_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MixTok_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL, Token BLOB);
                CREATE TABLE MixPlain_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        var injector = new NthWriteFailureInterceptor();
        DbContext OpenCtx()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider(), FaultOptions(injector));
        }

        await RunMixedOccMachineAsync(OpenCtx, injector, seed, steps: 200);
    }

    /// <summary>Three-context mixed-entity OCC machine body, shared with the live-provider variant.</summary>
    internal static async Task RunMixedOccMachineAsync(Func<DbContext> openCtx, NthWriteFailureInterceptor injector, int seed, int steps)
    {
        var rng = new Random(seed);
        var committedTok = new Dictionary<int, int>();
        var versionTok = new Dictionary<int, int>();
        var committedPlain = new Dictionary<int, int>();
        var nextKey = 1;
        var trace = new List<string>();
        string Tail() => "\nops:\n" + string.Join("\n", trace.TakeLast(50));

        int NewVal(int old)
        {
            // A same-value mutation would not mark the entity Modified, shrinking
            // the save's write-execution count below the machine's lower bound
            // used to place the fault; always produce a genuine change.
            int v;
            do { v = rng.Next(-100, 100); } while (v == old);
            return v;
        }

        var ctxs = new[] { new CtxState(), new CtxState(), new CtxState() };
        foreach (var s in ctxs)
            s.Ctx = openCtx();

        async Task RefreshAsync(int i)
        {
            var s = ctxs[i];
            s.Ctx.ChangeTracker.Clear();
            s.ViewTok = new Dictionary<int, (TokenRow, int, int)>();
            foreach (var row in await s.Ctx.Query<TokenRow>().ToListAsync())
                s.ViewTok[row.Id] = (row, versionTok.TryGetValue(row.Id, out var v) ? v : 0, row.Val);
            s.ViewPlain = new Dictionary<int, (PlainRow, int)>();
            foreach (var row in await s.Ctx.Query<PlainRow>().ToListAsync())
                s.ViewPlain[row.Id] = (row, row.Val);
            s.PendingTokVals.Clear();
            s.PendingTokDeletes.Clear();
            s.PendingTokAdds.Clear();
            s.PendingPlainVals.Clear();
            s.PendingPlainDeletes.Clear();
            s.PendingPlainAdds.Clear();
        }

        async Task VerifyCommittedAsync(string context)
        {
            using var verifyCtx = openCtx();
            var tokRows = (await verifyCtx.Query<TokenRow>().ToListAsync()).OrderBy(r => r.Id).ToList();
            var tokExpected = committedTok.OrderBy(kv => kv.Key).ToList();
            Assert.True(tokRows.Count == tokExpected.Count,
                $"token row count mismatch {context}: db={tokRows.Count} model={tokExpected.Count}\n" +
                $"db: [{string.Join(",", tokRows.Select(r => r.Id))}]\nmodel: [{string.Join(",", tokExpected.Select(kv => kv.Key))}]{Tail()}");
            for (var i = 0; i < tokRows.Count; i++)
            {
                Assert.True(tokRows[i].Id == tokExpected[i].Key && tokRows[i].Val == tokExpected[i].Value,
                    $"token row mismatch {context} at Id={tokExpected[i].Key}: db=({tokRows[i].Id},{tokRows[i].Val}) model=({tokExpected[i].Key},{tokExpected[i].Value}){Tail()}");
            }

            var plainRows = (await verifyCtx.Query<PlainRow>().ToListAsync()).OrderBy(r => r.Id).ToList();
            var plainExpected = committedPlain.OrderBy(kv => kv.Key).ToList();
            Assert.True(plainRows.Count == plainExpected.Count,
                $"plain row count mismatch {context}: db={plainRows.Count} model={plainExpected.Count}\n" +
                $"db: [{string.Join(",", plainRows.Select(r => r.Id))}]\nmodel: [{string.Join(",", plainExpected.Select(kv => kv.Key))}]{Tail()}");
            for (var i = 0; i < plainRows.Count; i++)
            {
                Assert.True(plainRows[i].Id == plainExpected[i].Key && plainRows[i].Val == plainExpected[i].Value,
                    $"plain row mismatch {context} at Id={plainExpected[i].Key}: db=({plainRows[i].Id},{plainRows[i].Val}) model=({plainExpected[i].Key},{plainExpected[i].Value}){Tail()}");
            }
        }

        bool IsStale(CtxState s) =>
            s.PendingTokVals.Keys.Concat(s.PendingTokDeletes).Any(k =>
                !versionTok.TryGetValue(k, out var v) || s.ViewTok[k].LoadedVersion != v);

        void ApplySaveToOracle(CtxState s)
        {
            foreach (var (key, val) in s.PendingTokVals)
            {
                committedTok[key] = val;
                versionTok[key]++;
                s.ViewTok[key] = (s.ViewTok[key].Row, versionTok[key], val);
            }
            foreach (var key in s.PendingTokDeletes)
            {
                committedTok.Remove(key);
                versionTok.Remove(key);
                s.ViewTok.Remove(key);
            }
            foreach (var (row, val) in s.PendingTokAdds)
            {
                committedTok[row.Id] = val;
                versionTok[row.Id] = 1;
                s.ViewTok[row.Id] = (row, 1, val);
            }
            foreach (var (key, val) in s.PendingPlainVals)
            {
                // Last-writer-wins has no conflict signal: an update of a row
                // another context already deleted silently affects zero rows.
                // The tracker still accepts the new value as the baseline.
                if (committedPlain.ContainsKey(key))
                    committedPlain[key] = val;
                s.ViewPlain[key] = (s.ViewPlain[key].Row, val);
            }
            foreach (var key in s.PendingPlainDeletes)
            {
                committedPlain.Remove(key);
                s.ViewPlain.Remove(key);
            }
            foreach (var (row, val) in s.PendingPlainAdds)
            {
                committedPlain[row.Id] = val;
                s.ViewPlain[row.Id] = (row, val);
            }
            s.PendingTokVals.Clear();
            s.PendingTokDeletes.Clear();
            s.PendingTokAdds.Clear();
            s.PendingPlainVals.Clear();
            s.PendingPlainDeletes.Clear();
            s.PendingPlainAdds.Clear();
        }

        async Task ConflictedSaveAsync(int i, int step, string label)
        {
            var s = ctxs[i];
            trace.Add($"{step}: ctx{i} {label} (stale -> conflict)");
            var ex = await Record.ExceptionAsync(() => s.Ctx.SaveChangesAsync());
            Assert.True(ex is DbConcurrencyException,
                $"expected DbConcurrencyException seed={seed} step={step} ctx{i}, got {ex?.GetType().Name ?? "no exception"}{Tail()}");
            // Atomic rollback: neither token nor plain changes may have applied.
            await VerifyCommittedAsync($"seed={seed} step={step} (after conflicted save of ctx{i})");
            await RefreshAsync(i);
        }

        // Seed both tables.
        for (var k = 0; k < 4; k++)
        {
            var tokKey = nextKey++;
            var tokVal = rng.Next(-100, 100);
            ctxs[0].Ctx.Add(new TokenRow { Id = tokKey, Val = tokVal });
            committedTok[tokKey] = tokVal;
            versionTok[tokKey] = 1;

            var plainKey = nextKey++;
            var plainVal = rng.Next(-100, 100);
            ctxs[0].Ctx.Add(new PlainRow { Id = plainKey, Val = plainVal });
            committedPlain[plainKey] = plainVal;
        }
        await ctxs[0].Ctx.SaveChangesAsync();
        for (var i = 0; i < ctxs.Length; i++)
            await RefreshAsync(i);

        try
        {
            for (var step = 0; step < steps; step++)
            {
                var i = rng.Next(ctxs.Length);
                var s = ctxs[i];
                switch (rng.Next(16))
                {
                    case 0 or 1: // mutate a viewed token row
                    {
                        var keys = s.ViewTok.Keys.Where(k => !s.PendingTokDeletes.Contains(k)).ToList();
                        if (keys.Count == 0) break;
                        var key = keys[rng.Next(keys.Count)];
                        var (row, _, orig) = s.ViewTok[key];
                        var val = NewVal(row.Val);
                        row.Val = val;
                        if (val == orig)
                        {
                            // Reverted to the change-detection baseline: the entity is
                            // Unchanged again, so the save writes nothing for this row.
                            s.PendingTokVals.Remove(key);
                            trace.Add($"{step}: ctx{i} revert tok {key} -> {val}");
                        }
                        else
                        {
                            s.PendingTokVals[key] = val;
                            trace.Add($"{step}: ctx{i} mutate tok {key} -> {val}");
                        }
                        break;
                    }
                    case 2 or 3: // mutate a viewed plain row (phantoms included: silently lost)
                    {
                        var keys = s.ViewPlain.Keys.Where(k => !s.PendingPlainDeletes.Contains(k)).ToList();
                        if (keys.Count == 0) break;
                        var key = keys[rng.Next(keys.Count)];
                        var (row, orig) = s.ViewPlain[key];
                        var val = NewVal(row.Val);
                        row.Val = val;
                        if (val == orig)
                        {
                            s.PendingPlainVals.Remove(key);
                            trace.Add($"{step}: ctx{i} revert plain {key} -> {val}");
                        }
                        else
                        {
                            s.PendingPlainVals[key] = val;
                            trace.Add($"{step}: ctx{i} mutate plain {key} -> {val}");
                        }
                        break;
                    }
                    case 4: // delete a viewed token row
                    {
                        var keys = s.ViewTok.Keys.Where(k => !s.PendingTokDeletes.Contains(k)).ToList();
                        if (keys.Count == 0) break;
                        var key = keys[rng.Next(keys.Count)];
                        s.Ctx.Remove(s.ViewTok[key].Row);
                        s.PendingTokDeletes.Add(key);
                        s.PendingTokVals.Remove(key);
                        trace.Add($"{step}: ctx{i} delete tok {key}");
                        break;
                    }
                    case 5: // delete a viewed plain row (phantom delete is a silent no-op)
                    {
                        var keys = s.ViewPlain.Keys.Where(k => !s.PendingPlainDeletes.Contains(k)).ToList();
                        if (keys.Count == 0) break;
                        var key = keys[rng.Next(keys.Count)];
                        s.Ctx.Remove(s.ViewPlain[key].Row);
                        s.PendingPlainDeletes.Add(key);
                        s.PendingPlainVals.Remove(key);
                        trace.Add($"{step}: ctx{i} delete plain {key}");
                        break;
                    }
                    case 6: // add a token row
                    {
                        var key = nextKey++;
                        var val = rng.Next(-100, 100);
                        var row = new TokenRow { Id = key, Val = val };
                        s.Ctx.Add(row);
                        s.PendingTokAdds.Add((row, val));
                        trace.Add($"{step}: ctx{i} add tok {key}");
                        break;
                    }
                    case 7: // add a plain row
                    {
                        var key = nextKey++;
                        var val = rng.Next(-100, 100);
                        var row = new PlainRow { Id = key, Val = val };
                        s.Ctx.Add(row);
                        s.PendingPlainAdds.Add((row, val));
                        trace.Add($"{step}: ctx{i} add plain {key}");
                        break;
                    }
                    case 8 or 9: // plain save: token staleness decides conflict vs apply
                    {
                        if (!s.HasPending)
                        {
                            await s.Ctx.SaveChangesAsync(); // empty save is a no-op
                            trace.Add($"{step}: ctx{i} empty save");
                            break;
                        }
                        if (IsStale(s))
                        {
                            await ConflictedSaveAsync(i, step, "save");
                        }
                        else
                        {
                            trace.Add($"{step}: ctx{i} save ok");
                            await s.Ctx.SaveChangesAsync();
                            ApplySaveToOracle(s);
                            await VerifyCommittedAsync($"seed={seed} step={step} (after save of ctx{i})");
                        }
                        break;
                    }
                    case 10: // save with transient failures at the Nth write, within retry budget
                    {
                        if (!s.HasPending) break;
                        if (IsStale(s))
                        {
                            // A stale save must conflict IDENTICALLY through injected
                            // transient failures: the retries consume the faults and the
                            // token conflict (never retryable) surfaces, applying nothing.
                            var staleSkip = rng.Next(s.MinWriteExecutions);
                            var staleTrips = 1 + rng.Next(2);
                            injector.Arm(staleSkip, staleTrips);
                            trace.Add($"{step}: ctx{i} stale save with {staleTrips} failures at write {staleSkip} (-> conflict)");
                            try
                            {
                                var ex = await Record.ExceptionAsync(() => s.Ctx.SaveChangesAsync());
                                Assert.True(ex is DbConcurrencyException,
                                    $"expected DbConcurrencyException from faulted stale save seed={seed} step={step} ctx{i}, got {ex?.GetType().Name ?? "no exception"}{Tail()}");
                            }
                            finally
                            {
                                injector.Disarm();
                            }
                            await VerifyCommittedAsync($"seed={seed} step={step} (after faulted stale save of ctx{i})");
                            await RefreshAsync(i);
                            break;
                        }
                        var skip = rng.Next(s.MinWriteExecutions);
                        var trips = 1 + rng.Next(2); // 1-2 failures, budget is 3
                        injector.Arm(skip, trips);
                        trace.Add($"{step}: ctx{i} save with {trips} failures at write {skip}");
                        try
                        {
                            await s.Ctx.SaveChangesAsync();
                        }
                        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                        {
                            throw new Xunit.Sdk.XunitException(
                                $"retried save threw {ex.GetType().Name} seed={seed} step={step} ctx{i}{Tail()}", ex);
                        }
                        finally
                        {
                            injector.Disarm();
                        }
                        ApplySaveToOracle(s);
                        await VerifyCommittedAsync($"seed={seed} step={step} (after retried save of ctx{i})");
                        break;
                    }
                    case 11: // save exhausting the retry budget at the Nth write, then recovery
                    {
                        if (!s.HasPending) break;
                        if (IsStale(s))
                        {
                            // Stale + budget-exhausting faults: whichever surfaces first —
                            // the token conflict (if its write executes before the armed
                            // trip) or the exhausted injected failure — nothing may apply.
                            var staleSkip = rng.Next(s.MinWriteExecutions);
                            injector.Arm(staleSkip, trips: 10);
                            trace.Add($"{step}: ctx{i} stale save exhausting retries at write {staleSkip}");
                            try
                            {
                                var ex = await Record.ExceptionAsync(() => s.Ctx.SaveChangesAsync());
                                Assert.True(ex is DbConcurrencyException or InjectedRetryableException,
                                    $"expected conflict or injected failure from exhausted stale save seed={seed} step={step} ctx{i}, got {ex?.GetType().Name ?? "no exception"}{Tail()}");
                            }
                            finally
                            {
                                injector.Disarm();
                            }
                            await VerifyCommittedAsync($"seed={seed} step={step} (after exhausted stale save of ctx{i})");
                            await RefreshAsync(i);
                            break;
                        }
                        var skip = rng.Next(s.MinWriteExecutions);
                        injector.Arm(skip, trips: 10); // > MaxRetries: the save must ultimately throw
                        trace.Add($"{step}: ctx{i} save exhausting retries at write {skip}");
                        try
                        {
                            await Assert.ThrowsAsync<InjectedRetryableException>(() => s.Ctx.SaveChangesAsync());
                        }
                        finally
                        {
                            injector.Disarm();
                        }
                        // Atomicity: nothing may have applied.
                        await VerifyCommittedAsync($"seed={seed} step={step} (after exhausted save of ctx{i})");
                        // Recovery: the same tracked state must land exactly once — a token
                        // stamped by a rolled-back attempt and not reset would false-conflict here.
                        trace.Add($"{step}: ctx{i} recovery save");
                        try
                        {
                            await s.Ctx.SaveChangesAsync();
                        }
                        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                        {
                            throw new Xunit.Sdk.XunitException(
                                $"recovery save threw {ex.GetType().Name} seed={seed} step={step} ctx{i}{Tail()}", ex);
                        }
                        ApplySaveToOracle(s);
                        await VerifyCommittedAsync($"seed={seed} step={step} (after recovery save of ctx{i})");
                        break;
                    }
                    case 12: // direct insert of a new row (auto-commit: durable immediately)
                    {
                        var key = nextKey++;
                        var val = rng.Next(-100, 100);
                        if (rng.Next(2) == 0)
                        {
                            trace.Add($"{step}: ctx{i} direct insert tok {key}");
                            await s.Ctx.InsertAsync(new TokenRow { Id = key, Val = val });
                            committedTok[key] = val;
                            versionTok[key] = 1;
                        }
                        else
                        {
                            trace.Add($"{step}: ctx{i} direct insert plain {key}");
                            await s.Ctx.InsertAsync(new PlainRow { Id = key, Val = val });
                            committedPlain[key] = val;
                        }
                        await VerifyCommittedAsync($"seed={seed} step={step} (after direct insert of ctx{i})");
                        break;
                    }
                    case 13: // direct update of a viewed row (tracked: entry must accept on success)
                    {
                        if (rng.Next(2) == 0)
                        {
                            var keys = s.ViewTok.Keys.Where(k => !s.PendingTokDeletes.Contains(k)).ToList();
                            if (keys.Count == 0) break;
                            var key = keys[rng.Next(keys.Count)];
                            var (row, loadedVersion, _) = s.ViewTok[key];
                            var val = NewVal(row.Val);
                            row.Val = val;
                            var stale = !versionTok.TryGetValue(key, out var v) || loadedVersion != v;
                            if (stale)
                            {
                                trace.Add($"{step}: ctx{i} direct update tok {key} (stale -> conflict)");
                                var ex = await Record.ExceptionAsync(() => s.Ctx.UpdateAsync(row));
                                Assert.True(ex is DbConcurrencyException,
                                    $"expected DbConcurrencyException from direct update seed={seed} step={step} ctx{i}, got {ex?.GetType().Name ?? "no exception"}{Tail()}");
                                await VerifyCommittedAsync($"seed={seed} step={step} (after conflicted direct update of ctx{i})");
                                await RefreshAsync(i);
                            }
                            else
                            {
                                trace.Add($"{step}: ctx{i} direct update tok {key} -> {val}");
                                await s.Ctx.UpdateAsync(row);
                                committedTok[key] = val;
                                versionTok[key]++;
                                s.ViewTok[key] = (row, versionTok[key], val);
                                s.PendingTokVals.Remove(key); // the entry accepted: nothing left pending
                                await VerifyCommittedAsync($"seed={seed} step={step} (after direct update of ctx{i})");
                            }
                        }
                        else
                        {
                            var keys = s.ViewPlain.Keys.Where(k => !s.PendingPlainDeletes.Contains(k)).ToList();
                            if (keys.Count == 0) break;
                            var key = keys[rng.Next(keys.Count)];
                            var (row, _) = s.ViewPlain[key];
                            var val = NewVal(row.Val);
                            row.Val = val;
                            trace.Add($"{step}: ctx{i} direct update plain {key} -> {val}");
                            await s.Ctx.UpdateAsync(row); // last-writer-wins; phantom update is silently lost
                            if (committedPlain.ContainsKey(key))
                                committedPlain[key] = val;
                            s.ViewPlain[key] = (row, val);
                            s.PendingPlainVals.Remove(key);
                            await VerifyCommittedAsync($"seed={seed} step={step} (after direct plain update of ctx{i})");
                        }
                        break;
                    }
                    case 14: // direct delete of a viewed row (tracked: entry must be evicted)
                    {
                        if (rng.Next(2) == 0)
                        {
                            var keys = s.ViewTok.Keys.Where(k => !s.PendingTokDeletes.Contains(k)).ToList();
                            if (keys.Count == 0) break;
                            var key = keys[rng.Next(keys.Count)];
                            var (row, loadedVersion, _) = s.ViewTok[key];
                            var stale = !versionTok.TryGetValue(key, out var v) || loadedVersion != v;
                            if (stale)
                            {
                                trace.Add($"{step}: ctx{i} direct delete tok {key} (stale -> conflict)");
                                var ex = await Record.ExceptionAsync(() => s.Ctx.DeleteAsync(row));
                                Assert.True(ex is DbConcurrencyException,
                                    $"expected DbConcurrencyException from direct delete seed={seed} step={step} ctx{i}, got {ex?.GetType().Name ?? "no exception"}{Tail()}");
                                await VerifyCommittedAsync($"seed={seed} step={step} (after conflicted direct delete of ctx{i})");
                                await RefreshAsync(i);
                            }
                            else
                            {
                                trace.Add($"{step}: ctx{i} direct delete tok {key}");
                                await s.Ctx.DeleteAsync(row);
                                committedTok.Remove(key);
                                versionTok.Remove(key);
                                s.ViewTok.Remove(key);
                                s.PendingTokVals.Remove(key);
                                await VerifyCommittedAsync($"seed={seed} step={step} (after direct delete of ctx{i})");
                            }
                        }
                        else
                        {
                            var keys = s.ViewPlain.Keys.Where(k => !s.PendingPlainDeletes.Contains(k)).ToList();
                            if (keys.Count == 0) break;
                            var key = keys[rng.Next(keys.Count)];
                            var (row, _) = s.ViewPlain[key];
                            trace.Add($"{step}: ctx{i} direct delete plain {key}");
                            await s.Ctx.DeleteAsync(row); // phantom delete is a silent no-op
                            committedPlain.Remove(key);
                            s.ViewPlain.Remove(key);
                            s.PendingPlainVals.Remove(key);
                            await VerifyCommittedAsync($"seed={seed} step={step} (after direct plain delete of ctx{i})");
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
            foreach (var s in ctxs)
                s.Ctx.Dispose();
        }
    }
}
