using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Audit findings: X1 (GlobalFilters mutable API), I1 (sync/async deadlock),
// X2 (OCC verifier bypasses interceptors)
//
// X1 — GlobalFilters property exposed IDictionary<Type,List<>> allowing callers
//   to mutate inner lists directly, bypassing copy-on-write protection and
//   corrupting query-pipeline enumeration under concurrent access.
//   Fix: property now returns IReadOnlyDictionary<Type, IReadOnlyList<>> so
//   direct List.Add/Remove is a compile-time error without a deliberate cast.
//
// I1 — Synchronous extension methods (ExecuteNonQueryWithInterception,
//   ExecuteScalarWithInterception, ExecuteReaderWithInterception) called async
//   interceptor hooks via .GetAwaiter().GetResult(). If the interceptor
//   awaited a non-completed Task (e.g. Task.Yield()) the continuation would be
//   posted to the captured SynchronizationContext. On a single-threaded SC
//   whose thread is currently blocked in GetResult(), that continuation never
//   runs → deadlock.
//   Fix: formal sync/async split — sync paths call the new sync hooks
//   (NonQueryExecuting/ScalarExecuting/ReaderExecuting), never await.
//
// X2 — VerifyUpdateOccAsync and VerifySingleUpdateOccAsync issued
//   cmd.ExecuteScalarAsync() directly, bypassing the command-interceptor
//   pipeline. Observability tools relying on interceptors missed these queries.
//   Fix: calls now route through ExecuteScalarWithInterceptionAsync.
// ══════════════════════════════════════════════════════════════════════════════

public class AuditGlobalFiltersAndSyncInterceptorTests
{
    // ══════════════════════════════════════════════════════════════════════════
    // Shared helpers
    // ══════════════════════════════════════════════════════════════════════════

    private sealed class SilentLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => false;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? ex, Func<TState, Exception?, string> f) { }
    }

    /// <summary>
    /// Provider that reports UseAffectedRowsSemantics=true, simulating MySQL's
    /// affected-rows connection mode. Used to trigger VerifySingleUpdateOccAsync.
    /// </summary>
    private sealed class AffectedRowsProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    /// <summary>
    /// SynchronizationContext that captures posted callbacks without executing them.
    /// Simulates a single-threaded SC whose thread is currently occupied (blocked).
    /// Any code that uses .GetAwaiter().GetResult() on a Task whose continuation
    /// is scheduled here will deadlock because the continuation never runs.
    /// </summary>
    private sealed class BlockedSynchronizationContext : SynchronizationContext
    {
        private readonly List<(SendOrPostCallback, object?)> _captured = new();
        public int CapturedCount => _captured.Count;

        public override void Post(SendOrPostCallback d, object? state)
            => _captured.Add((d, state)); // enqueue but never execute

        public override void Send(SendOrPostCallback d, object? state)
            => d(state); // Send is inline by convention
    }

    /// <summary>
    /// Interceptor whose async hooks do <c>await Task.Yield()</c> without
    /// <c>ConfigureAwait(false)</c>. This causes the continuation to be
    /// posted to the current SynchronizationContext. When called from sync
    /// code via .GetAwaiter().GetResult() under a single-threaded blocked SC,
    /// that continuation never runs → deadlock.
    /// </summary>
    private sealed class YieldingAsyncInterceptor : IDbCommandInterceptor
    {
        public async Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        { await Task.Yield(); return InterceptionResult<int>.Continue(); }

        public Task NonQueryExecutedAsync(DbCommand cmd, DbContext ctx, int r, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;

        public async Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        { await Task.Yield(); return InterceptionResult<object?>.Continue(); }

        public Task ScalarExecutedAsync(DbCommand cmd, DbContext ctx, object? r, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;

        public async Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        { await Task.Yield(); return InterceptionResult<DbDataReader>.Continue(); }

        public Task ReaderExecutedAsync(DbCommand cmd, DbContext ctx, DbDataReader rdr, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand cmd, DbContext ctx, Exception ex, CancellationToken ct)
            => Task.CompletedTask;
    }

    /// <summary>
    /// Interceptor that counts calls to sync vs async reader hooks independently.
    /// </summary>
    private sealed class SyncTrackingInterceptor : BaseDbCommandInterceptor
    {
        public int SyncReaderExecutingCount;
        public int AsyncReaderExecutingCount;
        public int SyncScalarExecutingCount;
        public int AsyncScalarExecutingCount;

        public SyncTrackingInterceptor() : base(new SilentLogger()) { }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand cmd, DbContext ctx)
        {
            Interlocked.Increment(ref SyncReaderExecutingCount);
            return base.ReaderExecuting(cmd, ctx);
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        {
            Interlocked.Increment(ref AsyncReaderExecutingCount);
            return base.ReaderExecutingAsync(cmd, ctx, ct);
        }

        public override InterceptionResult<object?> ScalarExecuting(DbCommand cmd, DbContext ctx)
        {
            Interlocked.Increment(ref SyncScalarExecutingCount);
            return base.ScalarExecuting(cmd, ctx);
        }

        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        {
            Interlocked.Increment(ref AsyncScalarExecutingCount);
            return base.ScalarExecutingAsync(cmd, ctx, ct);
        }
    }

    /// <summary>
    /// Interceptor that counts all async scalar executions (for X2 OCC interception test).
    /// </summary>
    private sealed class ScalarCountingInterceptor : IDbCommandInterceptor
    {
        public int AsyncScalarCount;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
            => Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand cmd, DbContext ctx, int r, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        {
            Interlocked.Increment(ref AsyncScalarCount);
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }
        public Task ScalarExecutedAsync(DbCommand cmd, DbContext ctx, object? r, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        public Task ReaderExecutedAsync(DbCommand cmd, DbContext ctx, DbDataReader rdr, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand cmd, DbContext ctx, Exception ex, CancellationToken ct)
            => Task.CompletedTask;
    }

    // ── Entities ─────────────────────────────────────────────────────────────

    [Table("GfItem")]
    private class GfItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public bool IsActive { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("X2OccItem")]
    private class X2OccItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // X1 — GlobalFilters returns IReadOnlyDictionary / IReadOnlyList
    // ══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// X1: The GlobalFilters property must return IReadOnlyDictionary so external
    /// code cannot insert new keys or replace the filter list without using
    /// the documented AddGlobalFilter API.
    /// </summary>
    [Fact]
    public void X1_GlobalFilters_Property_IsReadOnlyDictionary()
    {
        var opts = new DbContextOptions();
        Assert.IsAssignableFrom<IReadOnlyDictionary<Type, IReadOnlyList<LambdaExpression>>>(opts.GlobalFilters);
    }

    /// <summary>
    /// X1: Individual filter lists must be IReadOnlyList so callers cannot
    /// bypass copy-on-write by calling .Add() on the inner list directly.
    /// </summary>
    [Fact]
    public void X1_GlobalFilters_Values_AreReadOnlyList()
    {
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<GfItem>(x => x.IsActive);

        var filters = opts.GlobalFilters;
        Assert.True(filters.ContainsKey(typeof(GfItem)));
        IReadOnlyList<LambdaExpression> list = filters[typeof(GfItem)];
        Assert.Single(list); // one filter registered
    }

    /// <summary>
    /// X1: GlobalFilters must not be castable to a mutable IDictionary with writable
    /// IList values. Casting the property return to IDictionary would require knowing
    /// the internal value type, which has changed to IReadOnlyList.
    /// </summary>
    [Fact]
    public void X1_GlobalFilters_CannotBeCastToMutableIDictionary()
    {
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<GfItem>(x => x.IsActive);

        // IReadOnlyDictionary<Type, IReadOnlyList<>> is NOT castable to
        // IDictionary<Type, List<>> (different value type parameter).
        Assert.False(opts.GlobalFilters is IDictionary<Type, List<LambdaExpression>>,
            "GlobalFilters must not be castable to IDictionary<Type, List<>>");
    }

    /// <summary>
    /// X1: AddGlobalFilter must continue to work correctly after the type change.
    /// Queries against the filtered entity type must respect the registered filter.
    /// </summary>
    [Fact]
    public async Task X1_GlobalFilters_AddGlobalFilter_FiltersAreApplied()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE GfItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, IsActive INTEGER NOT NULL, Name TEXT NOT NULL);
INSERT INTO GfItem (IsActive, Name) VALUES (1, 'active');
INSERT INTO GfItem (IsActive, Name) VALUES (0, 'inactive');";
        setup.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.AddGlobalFilter<GfItem>(x => x.IsActive);
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var items = await ctx.Query<GfItem>().ToListAsync();

        Assert.Single(items);
        Assert.Equal("active", items[0].Name);
    }

    /// <summary>
    /// X1: Concurrent reads while AddGlobalFilter is called on another thread
    /// must not corrupt the filter collection (copy-on-write invariant preserved).
    /// </summary>
    [Fact]
    public async Task X1_GlobalFilters_ConcurrentAddAndRead_DoesNotCorrupt()
    {
        var opts = new DbContextOptions();
        var exceptions = new ConcurrentBag<Exception>();
        var readOk = true;

        // Writer thread: repeatedly add filters
        var writer = Task.Run(() =>
        {
            for (int i = 0; i < 200; i++)
                opts.AddGlobalFilter<GfItem>(x => x.IsActive);
        });

        // Reader threads: repeatedly iterate over GlobalFilters
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < 500; i++)
            {
                try
                {
                    var filters = opts.GlobalFilters;
                    foreach (var kvp in filters)
                        foreach (var __ in kvp.Value)
                        { /* iterate */ }
                }
                catch (Exception ex)
                {
                    readOk = false;
                    exceptions.Add(ex);
                }
            }
        })).ToArray();

        await Task.WhenAll(new[] { writer }.Concat(readers).ToArray());
        Assert.True(readOk, $"Concurrent read threw: {string.Join(", ", exceptions.Select(e => e.Message))}");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // I1 — Sync/async interceptor split — no deadlock on single-threaded SC
    // ══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// I1: Synchronous ToList() must not deadlock when an interceptor's async
    /// methods use await Task.Yield() (which posts to current SynchronizationContext).
    /// The fix routes sync paths through sync hooks, never calling async methods.
    /// WaitAsync(5s) catches deadlocks that would previously block indefinitely.
    /// </summary>
    [Fact]
    public async Task I1_SyncToList_WithAsyncInterceptorAndBlockedSC_DoesNotDeadlock()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE GfItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, IsActive INTEGER NOT NULL, Name TEXT NOT NULL); INSERT INTO GfItem (IsActive, Name) VALUES (1, 'x')";
        setup.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(new YieldingAsyncInterceptor());
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Run ToList() on a background thread that has a BlockedSC installed.
        // Before the fix: sync path called .GetAwaiter().GetResult() on
        //   ReaderExecutingAsync which does Task.Yield() → posts continuation to
        //   BlockedSC → continuation never runs → GetResult() blocks forever.
        // After the fix: sync path calls ReaderExecuting (sync, no-op) → no await → completes.
        var task = Task.Run(() =>
        {
            var blocked = new BlockedSynchronizationContext();
            SynchronizationContext.SetSynchronizationContext(blocked);
            try { return ctx.Query<GfItem>().ToList(); }
            finally { SynchronizationContext.SetSynchronizationContext(null); }
        });

        // 5-second timeout catches deadlocks; successful fix completes in milliseconds.
        var items = await task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(items);
    }

    /// <summary>
    /// I1: Synchronous ToList() must invoke the sync hook (ReaderExecuting),
    /// not the async hook (ReaderExecutingAsync).
    /// </summary>
    [Fact]
    public void I1_SyncToList_InvokesSyncHooks_NotAsyncHooks()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE GfItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, IsActive INTEGER NOT NULL, Name TEXT NOT NULL); INSERT INTO GfItem (IsActive, Name) VALUES (1, 'y')";
        setup.ExecuteNonQuery();

        var tracker = new SyncTrackingInterceptor();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(tracker);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        _ = ctx.Query<GfItem>().ToList();

        Assert.True(tracker.SyncReaderExecutingCount > 0, "Expected sync ReaderExecuting to be called");
        Assert.Equal(0, tracker.AsyncReaderExecutingCount);
    }

    /// <summary>
    /// I1: Asynchronous ToListAsync() must invoke the async hook (ReaderExecutingAsync),
    /// not the sync hook (ReaderExecuting).
    /// </summary>
    [Fact]
    public async Task I1_AsyncToListAsync_InvokesAsyncHooks_NotSyncHooks()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE GfItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, IsActive INTEGER NOT NULL, Name TEXT NOT NULL); INSERT INTO GfItem (IsActive, Name) VALUES (1, 'z')";
        setup.ExecuteNonQuery();

        var tracker = new SyncTrackingInterceptor();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(tracker);
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        _ = await ctx.Query<GfItem>().ToListAsync();

        Assert.True(tracker.AsyncReaderExecutingCount > 0, "Expected async ReaderExecutingAsync to be called");
        Assert.Equal(0, tracker.SyncReaderExecutingCount);
    }

    /// <summary>
    /// I1: A custom interceptor that overrides only sync hooks (not async) must
    /// be invoked on synchronous query paths.
    /// </summary>
    [Fact]
    public void I1_SyncHookOverride_IsInvoked_FromToList()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE GfItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, IsActive INTEGER NOT NULL, Name TEXT NOT NULL)";
        setup.ExecuteNonQuery();

        // Custom interceptor that only overrides the sync reader hook
        var callCount = 0;
        var customInterceptor = new SuppressingReaderInterceptor(() => Interlocked.Increment(ref callCount));
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(customInterceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        _ = ctx.Query<GfItem>().ToList();

        Assert.True(callCount > 0, "Custom sync hook must be called from ToList()");
    }

    /// <summary>Helper interceptor that calls an action on sync reader execution.</summary>
    private sealed class SuppressingReaderInterceptor : IDbCommandInterceptor
    {
        private readonly Action _onExecuting;
        public SuppressingReaderInterceptor(Action onExecuting) => _onExecuting = onExecuting;

        // Override sync hook — this is what sync paths call
        public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand cmd, DbContext ctx)
        { _onExecuting(); return InterceptionResult<DbDataReader>.Continue(); }

        // Async hooks — not called from sync paths
        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand c, DbContext ctx, int r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand c, DbContext ctx, object? r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        public Task ReaderExecutedAsync(DbCommand c, DbContext ctx, DbDataReader rdr, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand c, DbContext ctx, Exception ex, CancellationToken ct) => Task.CompletedTask;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // X2 — OCC verification routed through command interceptor pipeline
    // ══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// X2: VerifySingleUpdateOccAsync (triggered by 0-row-affected UPDATE with
    /// affected-row semantics) must route through the async scalar interceptor
    /// pipeline. Before the fix, cmd.ExecuteScalarAsync() was called directly
    /// and interceptors never saw the verification query.
    /// </summary>
    [Fact]
    public async Task X2_OccVerifier_VerifySingleUpdate_ScalarRoutesThroughInterceptor()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE X2OccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Token BLOB);
INSERT INTO X2OccItem VALUES (1, 'original', X'010203')";
        setup.ExecuteNonQuery();

        var counter = new ScalarCountingInterceptor();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(counter);
        await using var ctx = new DbContext(cn, new AffectedRowsProvider(), opts);

        // UPDATE with stale token → 0 rows affected on affected-row-semantics provider
        // → triggers VerifySingleUpdateOccAsync (SELECT COUNT to confirm conflict)
        // → SELECT COUNT routes through ExecuteScalarWithInterceptionAsync
        // → interceptor ScalarExecutingAsync must be called
        var staleToken = new byte[] { 9, 9, 9 };
        await Assert.ThrowsAsync<DbConcurrencyException>(
            () => ctx.UpdateAsync(new X2OccItem { Id = 1, Name = "new", Token = staleToken }));

        Assert.True(counter.AsyncScalarCount > 0,
            "OCC verification SELECT COUNT must route through the scalar interceptor pipeline");
    }

    /// <summary>
    /// X2: VerifyUpdateOccAsync (batch path, triggered when SaveChangesAsync UPDATE
    /// returns fewer rows than the batch size on affected-row-semantics providers)
    /// must also route through the scalar interceptor pipeline.
    /// </summary>
    [Fact]
    public async Task X2_OccVerifier_VerifyBatchUpdate_ScalarRoutesThroughInterceptor()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE X2OccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Token BLOB);
INSERT INTO X2OccItem VALUES (1, 'original', X'010203')";
        setup.ExecuteNonQuery();

        var counter = new ScalarCountingInterceptor();
        var opts = new DbContextOptions
        {
            // RequireMatchedRowOccSemantics=false allows affected-row-semantics providers to proceed
            // to the SELECT-then-verify path instead of throwing NormConfigurationException.
            RequireMatchedRowOccSemantics = false
        };
        opts.CommandInterceptors.Add(counter);
        await using var ctx = new DbContext(cn, new AffectedRowsProvider(), opts);

        // Attach entity with correct token then externally change the token in DB
        // so the batch UPDATE returns 0 rows (OCC conflict via affected-row semantics)
        var item = new X2OccItem { Id = 1, Name = "original", Token = new byte[] { 1, 2, 3 } };
        ctx.Attach(item);

        // Change DB token externally (simulate competing writer)
        using var ext = cn.CreateCommand();
        ext.CommandText = "UPDATE X2OccItem SET Token = X'AABBCC' WHERE Id = 1";
        ext.ExecuteNonQuery();

        item.Name = "modified";
        ctx.Update(item);

        // SaveChangesAsync triggers batch UPDATE → 0 rows affected → VerifyUpdateOccAsync
        // → SELECT COUNT routes through interceptor
        await Assert.ThrowsAsync<DbConcurrencyException>(
            () => ctx.SaveChangesAsync());

        Assert.True(counter.AsyncScalarCount > 0,
            "Batch OCC verification SELECT COUNT must route through the scalar interceptor pipeline");
    }
}
