using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
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
/// Failure-injection fuzzer for the SaveChanges retry invariants: seeded
/// mixes of adds (DB-generated keys), tracked mutations, and deletes save
/// under a command interceptor that throws a retryable failure at armed write
/// executions. A save that fails fewer times than the retry budget must
/// succeed transparently with no duplicate rows, no lost rows, and correct
/// unique backfilled keys (the rolled-back attempt already stamped keys that
/// must be reset). A save that exhausts the budget must throw, leave the
/// database exactly at the prior committed state, and the very next save —
/// same tracked state, injector disarmed — must land everything exactly once.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class RetryFaultInjectionFuzzTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("RfiRow_Test")]
    public class RfiRow
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private sealed class InjectedRetryableException : DbException
    {
        public InjectedRetryableException() : base("injected retryable failure") { }
    }

    /// <summary>
    /// Throws on the first WRITE execution of an attempt, for the armed number
    /// of attempts. Each retry re-enters SaveChanges and trips again until the
    /// armed count is spent.
    /// </summary>
    private sealed class ArmableWriteFailureInterceptor : BaseDbCommandInterceptor
    {
        private int _failuresRemaining;

        public ArmableWriteFailureInterceptor() : base(NullLogger.Instance) { }

        public void Arm(int failures) => Interlocked.Exchange(ref _failuresRemaining, failures);

        private void TripIfArmed(DbCommand command)
        {
            if (!IsWrite(command))
                return;
            if (Interlocked.Decrement(ref _failuresRemaining) >= 0)
                throw new InjectedRetryableException();
            Interlocked.Increment(ref _failuresRemaining); // undo the overshoot below zero
        }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            TripIfArmed(command);
            return base.NonQueryExecutingAsync(command, context, ct);
        }

        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            TripIfArmed(command);
            return base.ScalarExecutingAsync(command, context, ct);
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            TripIfArmed(command);
            return base.ReaderExecutingAsync(command, context, ct);
        }

        private static bool IsWrite(DbCommand command)
            => command.CommandText.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
               || command.CommandText.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
               || command.CommandText.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record RowState(string Name, int Value);

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_RETRY_FUZZ_SWEEP to "start:count" to run that seed range through the
    /// fault-injection machine. Unset, this fact is a no-op so the fixed seeds stay
    /// the baseline.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_RETRY_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        for (var s = start; s < start + count; s++)
            await Saves_survive_transient_failures_without_loss_or_duplication(s);
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    public async Task Saves_survive_transient_failures_without_loss_or_duplication(int seed)
    {
        var dbName = $"rfifuzz_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE RfiRow_Test (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var interceptor = new ArmableWriteFailureInterceptor();
        var retry = new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is InjectedRetryableException,
        };

        DbContext OpenCtx(bool withFaults)
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            var opts = new DbContextOptions { RetryPolicy = retry };
            if (withFaults)
                opts.CommandInterceptors.Add(interceptor);
            return new DbContext(cn, new SqliteProvider(), opts);
        }

        var rng = new Random(seed);
        var committed = new Dictionary<int, RowState>();
        var working = new Dictionary<int, RowState>();
        var tracked = new Dictionary<int, RfiRow>();
        var pendingAdds = new List<(RfiRow Entity, RowState State)>();
        var trace = new List<string>();
        string Tail() => "\nops:\n" + string.Join("\n", trace.TakeLast(40));

        async Task VerifyAsync(Dictionary<int, RowState> expected, string context)
        {
            using var verifyCtx = OpenCtx(withFaults: false);
            var rows = (await verifyCtx.Query<RfiRow>().ToListAsync()).OrderBy(r => r.Id).ToList();
            var expectedRows = expected.OrderBy(kv => kv.Key).ToList();
            Assert.True(rows.Count == expectedRows.Count,
                $"row count mismatch {context}: db={rows.Count} model={expectedRows.Count}\n" +
                $"db: [{string.Join(",", rows.Select(r => r.Id))}]\nmodel: [{string.Join(",", expectedRows.Select(kv => kv.Key))}]{Tail()}");
            for (var i = 0; i < rows.Count; i++)
            {
                Assert.True(rows[i].Id == expectedRows[i].Key
                        && rows[i].Name == expectedRows[i].Value.Name
                        && rows[i].Value == expectedRows[i].Value.Value,
                    $"row mismatch {context} at Id={expectedRows[i].Key}: db=({rows[i].Id},\"{rows[i].Name}\",{rows[i].Value}) " +
                    $"model=({expectedRows[i].Key},\"{expectedRows[i].Value.Name}\",{expectedRows[i].Value.Value}){Tail()}");
            }
        }

        void CommitPendingToModel()
        {
            foreach (var (entity, state) in pendingAdds)
            {
                Assert.True(entity.Id > 0, $"generated key not backfilled seed={seed}: Id={entity.Id}{Tail()}");
                Assert.True(!working.ContainsKey(entity.Id), $"generated key collision seed={seed}: Id={entity.Id}{Tail()}");
                working[entity.Id] = state;
                tracked[entity.Id] = entity;
            }
            pendingAdds.Clear();
            committed = new Dictionary<int, RowState>(working);
        }

        var ctx = OpenCtx(withFaults: true);
        try
        {
            for (var step = 0; step < 140; step++)
            {
                switch (rng.Next(10))
                {
                    case 0 or 1 or 2 or 3: // add
                    {
                        var state = new RowState($"n{step}_{rng.Next(1000)}", rng.Next(-500, 500));
                        var entity = new RfiRow { Name = state.Name, Value = state.Value };
                        ctx.Add(entity);
                        pendingAdds.Add((entity, state));
                        trace.Add($"{step}: add");
                        break;
                    }
                    case 4 or 5: // mutate a tracked committed row
                    {
                        if (tracked.Count == 0) break;
                        var key = tracked.Keys.ElementAt(rng.Next(tracked.Count));
                        if (!working.ContainsKey(key)) break;
                        var state = new RowState($"m{step}_{rng.Next(1000)}", rng.Next(-500, 500));
                        tracked[key].Name = state.Name;
                        tracked[key].Value = state.Value;
                        working[key] = state;
                        trace.Add($"{step}: mutate {key}");
                        break;
                    }
                    case 6: // delete a tracked committed row
                    {
                        if (tracked.Count == 0) break;
                        var key = tracked.Keys.ElementAt(rng.Next(tracked.Count));
                        if (!working.ContainsKey(key)) break;
                        ctx.Remove(tracked[key]);
                        tracked.Remove(key);
                        working.Remove(key);
                        trace.Add($"{step}: delete {key}");
                        break;
                    }
                    case 7 or 8: // save with 0..2 injected transient failures (within budget)
                    {
                        var failures = rng.Next(3);
                        interceptor.Arm(failures);
                        trace.Add($"{step}: save with {failures} transient failures");
                        await ctx.SaveChangesAsync();
                        interceptor.Arm(0);
                        CommitPendingToModel();
                        await VerifyAsync(committed, $"seed={seed} step={step} (after retried save)");
                        break;
                    }
                    default: // save that exhausts the retry budget, then a recovery save
                    {
                        if (pendingAdds.Count == 0 && working.Count == committed.Count
                            && working.All(kv => committed.TryGetValue(kv.Key, out var c) && c == kv.Value))
                        {
                            break; // nothing pending — an exhausted save proves nothing here
                        }
                        interceptor.Arm(10); // > MaxRetries: the save must ultimately throw
                        trace.Add($"{step}: save exhausting retries");
                        await Assert.ThrowsAsync<InjectedRetryableException>(() => ctx.SaveChangesAsync());
                        interceptor.Arm(0);
                        // Atomicity: nothing may have applied.
                        await VerifyAsync(committed, $"seed={seed} step={step} (after exhausted save)");
                        // Recovery: the same tracked state must land exactly once.
                        trace.Add($"{step}: recovery save");
                        await ctx.SaveChangesAsync();
                        CommitPendingToModel();
                        await VerifyAsync(committed, $"seed={seed} step={step} (after recovery save)");
                        break;
                    }
                }
            }

            interceptor.Arm(0);
            await ctx.SaveChangesAsync();
            CommitPendingToModel();
            await VerifyAsync(committed, $"seed={seed} final");
        }
        finally
        {
            ctx.Dispose();
        }
    }
}
