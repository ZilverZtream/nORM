using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// A transient fault during a tracked <c>SaveChanges</c> must retry to an EXACTLY-ONCE outcome: the batch
/// runs inside one transaction, so a pre-commit fault — whether it fires <em>before</em> a write executes
/// or <em>after</em> the write executed but before commit — must roll the whole batch back and replay it,
/// leaving the database in precisely the intended final state. No lost insert (the DB-generated key an
/// INSERT stamped and then rolled back must be reset so the retry re-inserts rather than skipping the row),
/// no duplicate row, no half-applied batch. A fault that exhausts the retry budget, or one the policy does
/// not classify as transient, must surface and leave the database untouched (all-or-nothing).
///
/// This complements BulkNonRetryTests (which only injects <em>before</em> execution on a single row) by
/// exercising the after-execution window on multi-row mixed insert/update/delete batches and fuzzing the
/// fault position, which is where the DB-generated-key reset-on-rollback invariant actually lives.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class RetryFaultInjectionTests
{
    [Table("RfRow")]
    public sealed class RfRow
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public long Value { get; set; }
    }

    /// <summary>A DbException the configured RetryPolicy treats as transient.</summary>
    private sealed class FaultInjectionException : DbException
    {
        public FaultInjectionException() : base("injected transient write fault") { }
    }

    /// <summary>A DbException the RetryPolicy does NOT classify as transient.</summary>
    private sealed class PermanentInjectedException : DbException
    {
        public PermanentInjectedException() : base("injected non-transient write fault") { }
    }

    private static bool IsWrite(DbCommand c)
        => c.CommandText.IndexOf("INSERT", StringComparison.OrdinalIgnoreCase) >= 0
           || c.CommandText.IndexOf("UPDATE", StringComparison.OrdinalIgnoreCase) >= 0
           || c.CommandText.IndexOf("DELETE", StringComparison.OrdinalIgnoreCase) >= 0;

    /// <summary>
    /// Faults exactly once, on the Nth write command it observes, either just before the command executes
    /// (<paramref name="afterExecute"/> = false) or immediately after it executed but before the batch
    /// commits (<paramref name="afterExecute"/> = true). Writes are counted across nonquery/scalar/reader
    /// forms because a DB-generated-key INSERT surfaces as a scalar on SQLite.
    /// </summary>
    private sealed class PositionedWriteFaultInterceptor : BaseDbCommandInterceptor
    {
        private readonly int _faultAtWrite;
        private readonly bool _afterExecute;
        private readonly Func<Exception> _fault;
        private int _writeSeen;
        private bool _fired;
        private bool _armAfter;

        public int WritesObserved => _writeSeen;
        public bool Fired => _fired;

        public PositionedWriteFaultInterceptor(int faultAtWrite, bool afterExecute, Func<Exception>? fault = null)
            : base(NullLogger.Instance)
        {
            _faultAtWrite = faultAtWrite;
            _afterExecute = afterExecute;
            _fault = fault ?? (() => new FaultInjectionException());
        }

        private void OnExecuting(DbCommand c)
        {
            if (_fired || !IsWrite(c)) return;
            _writeSeen++;
            if (_writeSeen != _faultAtWrite) return;
            if (_afterExecute) _armAfter = true;               // let the command run, then trip after
            else { _fired = true; throw _fault(); }
        }

        private void OnExecuted()
        {
            if (_fired || !_armAfter) return;
            _armAfter = false;
            _fired = true;
            throw _fault();
        }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { OnExecuting(command); return Task.FromResult(InterceptionResult<int>.Continue()); }
        public override Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
        { OnExecuted(); return Task.CompletedTask; }
        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { OnExecuting(command); return Task.FromResult(InterceptionResult<object?>.Continue()); }
        public override Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
        { OnExecuted(); return Task.CompletedTask; }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { OnExecuting(command); return Task.FromResult(InterceptionResult<DbDataReader>.Continue()); }
        public override Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
        { OnExecuted(); return Task.CompletedTask; }
    }

    /// <summary>Faults on the first write of EVERY attempt (used for retry-exhaustion / non-transient cases).</summary>
    private sealed class AlwaysWriteFaultInterceptor : BaseDbCommandInterceptor
    {
        private readonly Func<Exception> _fault;
        public AlwaysWriteFaultInterceptor(Func<Exception> fault) : base(NullLogger.Instance) => _fault = fault;

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { if (IsWrite(command)) throw _fault(); return Task.FromResult(InterceptionResult<int>.Continue()); }
        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { if (IsWrite(command)) throw _fault(); return Task.FromResult(InterceptionResult<object?>.Continue()); }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { if (IsWrite(command)) throw _fault(); return Task.FromResult(InterceptionResult<DbDataReader>.Continue()); }
    }

    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE RfRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext NewCtx(SqliteConnection cn, IDbCommandInterceptor? interceptor, int maxRetries = 3)
    {
        var opts = new DbContextOptions
        {
            RetryPolicy = new RetryPolicy
            {
                MaxRetries = maxRetries,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = ex => ex is FaultInjectionException
            },
            OnModelCreating = mb => mb.Entity<RfRow>().HasKey(x => x.Id)
        };
        if (interceptor != null) opts.CommandInterceptors.Add(interceptor);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static void SeedCommitted(SqliteConnection cn, IReadOnlyDictionary<string, long> rows)
    {
        foreach (var (name, value) in rows)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO RfRow (Name, Value) VALUES ($n, $v)";
            cmd.Parameters.AddWithValue("$n", name);
            cmd.Parameters.AddWithValue("$v", value);
            cmd.ExecuteNonQuery();
        }
    }

    private static List<(string Name, long Value)> ReadAll(SqliteConnection cn)
    {
        var list = new List<(string, long)>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name, Value FROM RfRow ORDER BY Name, Value";
        using var r = cmd.ExecuteReader();
        while (r.Read()) list.Add((r.GetString(0), r.GetInt64(1)));
        return list;
    }

    private static List<(string Name, long Value)> Expected(IReadOnlyDictionary<string, long> model)
        => model.Select(kv => (kv.Key, kv.Value)).OrderBy(p => p.Key).ThenBy(p => p.Value).ToList();

    // ── Keeper battery ───────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(false)]  // fault before the batched insert executes
    [InlineData(true)]   // fault after the batched insert executed, before commit  (key-reset path)
    public async Task Transient_fault_during_insert_batch_retries_to_exactly_once(bool afterExecute)
    {
        using var cn = NewDb();
        var interceptor = new PositionedWriteFaultInterceptor(1, afterExecute);
        await using var ctx = NewCtx(cn, interceptor);

        var model = new Dictionary<string, long>();
        for (int i = 0; i < 4; i++) { var n = $"ins-{i}"; model[n] = 10 + i; ctx.Add(new RfRow { Name = n, Value = 10 + i }); }

        await ctx.SaveChangesAsync();

        Assert.True(interceptor.Fired, "the injected fault never fired — the batch issued no write command");
        Assert.Equal(Expected(model), ReadAll(cn));

        // A second save must be a no-op: the retry must not have left the tracker with stranded Added rows.
        Assert.Equal(0, await ctx.SaveChangesAsync());
        Assert.Equal(Expected(model), ReadAll(cn));
    }

    /// <summary>
    /// A batch that mixes an update, a delete, and an insert issues more than one write command; a fault on
    /// the SECOND command must roll back the first command's already-applied effect and replay the whole
    /// batch. Records the write-command count so the fault position is meaningful rather than assumed.
    /// </summary>
    [Fact]
    public async Task Transient_fault_at_second_command_of_multi_command_batch_retries_to_exactly_once()
    {
        using var cn = NewDb();
        var seed = new Dictionary<string, long> { ["seed-0"] = 100, ["seed-1"] = 101 };
        SeedCommitted(cn, seed);

        // First measure how many write commands the mixed batch issues (no fault).
        int commandCount;
        {
            var probe = new PositionedWriteFaultInterceptor(int.MaxValue, false);
            await using var probeCtx = NewCtx(cn, probe);
            var t = (await probeCtx.Query<RfRow>().ToListAsync()).ToDictionary(r => r.Name);
            t["seed-0"].Value = 200;
            probeCtx.Remove(t["seed-1"]);
            probeCtx.Add(new RfRow { Name = "new-0", Value = 300 });
            await probeCtx.SaveChangesAsync();
            commandCount = probe.WritesObserved;
        }
        Assert.True(commandCount >= 2, $"expected a multi-command batch but only {commandCount} write command(s) were issued");

        // Reset to the seeded baseline and now fault on the second write command, after it executes.
        using (var reset = cn.CreateCommand()) { reset.CommandText = "DELETE FROM RfRow"; reset.ExecuteNonQuery(); }
        SeedCommitted(cn, seed);

        var interceptor = new PositionedWriteFaultInterceptor(2, afterExecute: true);
        await using var ctx = NewCtx(cn, interceptor);
        var tracked = (await ctx.Query<RfRow>().ToListAsync()).ToDictionary(r => r.Name);
        var model = new Dictionary<string, long>(seed);
        tracked["seed-0"].Value = 200; model["seed-0"] = 200;
        ctx.Remove(tracked["seed-1"]); model.Remove("seed-1");
        ctx.Add(new RfRow { Name = "new-0", Value = 300 }); model["new-0"] = 300;

        await ctx.SaveChangesAsync();

        Assert.True(interceptor.Fired);
        Assert.Equal(Expected(model), ReadAll(cn));
        Assert.Equal(0, await ctx.SaveChangesAsync());
        Assert.Equal(Expected(model), ReadAll(cn));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Transient_fault_during_mixed_batch_retries_to_exactly_once(bool afterExecute)
    {
        using var cn = NewDb();
        var seed = new Dictionary<string, long> { ["seed-0"] = 100, ["seed-1"] = 101, ["seed-2"] = 102 };
        SeedCommitted(cn, seed);

        var interceptor = new PositionedWriteFaultInterceptor(1, afterExecute);
        await using var ctx = NewCtx(cn, interceptor);

        var tracked = (await ctx.Query<RfRow>().ToListAsync()).ToDictionary(r => r.Name);
        var model = new Dictionary<string, long>(seed);

        tracked["seed-0"].Value = 200; model["seed-0"] = 200;   // update
        ctx.Remove(tracked["seed-1"]); model.Remove("seed-1");  // delete
        ctx.Add(new RfRow { Name = "new-0", Value = 300 }); model["new-0"] = 300;  // insert

        await ctx.SaveChangesAsync();

        Assert.True(interceptor.Fired);
        Assert.Equal(Expected(model), ReadAll(cn));
        Assert.Equal(0, await ctx.SaveChangesAsync());
        Assert.Equal(Expected(model), ReadAll(cn));
    }

    [Fact]
    public async Task Retry_exhaustion_leaves_the_database_untouched()
    {
        using var cn = NewDb();
        var seed = new Dictionary<string, long> { ["seed-0"] = 1 };
        SeedCommitted(cn, seed);

        // A transient fault on every attempt exhausts the budget and surfaces; the batch never lands.
        var interceptor = new AlwaysWriteFaultInterceptor(() => new FaultInjectionException());
        await using var ctx = NewCtx(cn, interceptor, maxRetries: 3);

        ctx.Add(new RfRow { Name = "never", Value = 99 });
        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        Assert.Equal(Expected(seed), ReadAll(cn));
    }

    [Fact]
    public async Task Non_transient_fault_is_not_retried_and_leaves_the_database_untouched()
    {
        using var cn = NewDb();
        var seed = new Dictionary<string, long> { ["seed-0"] = 1 };
        SeedCommitted(cn, seed);

        // The policy does not classify PermanentInjectedException as transient, so the save fails fast.
        var interceptor = new AlwaysWriteFaultInterceptor(() => new PermanentInjectedException());
        await using var ctx = NewCtx(cn, interceptor, maxRetries: 3);

        ctx.Add(new RfRow { Name = "never", Value = 99 });
        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        Assert.Equal(Expected(seed), ReadAll(cn));
    }

    // ── Coverage-guided sweep ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Fault_position_sweep_over_mixed_batches_stays_exactly_once()
    {
        for (int trial = 0; trial < 300; trial++)
        {
            var rng = new Random(7000 + trial);
            using var cn = NewDb();

            // Seed 0..4 committed rows.
            int seedCount = rng.Next(0, 5);
            var seed = new Dictionary<string, long>();
            for (int i = 0; i < seedCount; i++) seed[$"seed-{i}"] = 100 + i;
            SeedCommitted(cn, seed);

            bool afterExecute = rng.Next(2) == 0;
            int faultAt = rng.Next(1, 3);   // 1 always fires when >=1 write; 2 fires when >=2 writes
            var interceptor = new PositionedWriteFaultInterceptor(faultAt, afterExecute);
            await using var ctx = NewCtx(cn, interceptor);

            var tracked = (await ctx.Query<RfRow>().ToListAsync()).ToDictionary(r => r.Name);
            var model = new Dictionary<string, long>(seed);

            // Mutate each seeded row: leave / update / delete.
            foreach (var name in seed.Keys.ToList())
            {
                switch (rng.Next(3))
                {
                    case 1:
                        long nv = rng.Next(1000, 2000);
                        tracked[name].Value = nv; model[name] = nv;
                        break;
                    case 2:
                        ctx.Remove(tracked[name]); model.Remove(name);
                        break;
                }
            }

            // Add 0..3 fresh rows (DB-generated keys).
            int inserts = rng.Next(0, 4);
            for (int k = 0; k < inserts; k++)
            {
                var n = $"ins-{trial}-{k}";
                long v = rng.Next(2000, 3000);
                ctx.Add(new RfRow { Name = n, Value = v }); model[n] = v;
            }

            await ctx.SaveChangesAsync();   // must transparently retry the injected fault

            var actual = ReadAll(cn);
            Assert.True(Expected(model).SequenceEqual(actual),
                $"trial {trial} (seed={seedCount}, inserts={inserts}, faultAt={faultAt}, after={afterExecute}, " +
                $"fired={interceptor.Fired}, writes={interceptor.WritesObserved}): " +
                $"expected [{string.Join(",", Expected(model))}] but DB had [{string.Join(",", actual)}]");
        }
    }
}
