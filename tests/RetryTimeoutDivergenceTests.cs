using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// A1/X1 — Retry timeout divergence (Gate 3.8→4.0)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that both the query path (RetryingExecutionStrategy) and the save path
/// (DbContext.IsRetryableException) agree: TimeoutException must NOT be retried.
///
/// A1/X1 root cause: RetryingExecutionStrategy previously caught TimeoutException in
/// its catch guard, so a timed-out query could be replayed (risking duplicate side effects).
/// The save path's IsRetryableException already excluded TimeoutException.
/// Fix: remove TimeoutException from the RetryingExecutionStrategy catch clause so both
/// paths consistently skip timeout retries.
/// </summary>
public class RetryTimeoutDivergenceTests
{
    private static DbContext BuildContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    // ── Query path: TimeoutException is NOT retried ───────────────────────────

    [Fact]
    public async Task RetryStrategy_TimeoutException_PropagatesImmediately()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 5,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = _ => true // would retry everything — but TimeoutException is not caught
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        int callCount = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                callCount++;
                return Task.FromException<int>(new TimeoutException("simulated command timeout"));
            }, CancellationToken.None));

        // TimeoutException must escape without being caught by the retry loop.
        Assert.IsType<TimeoutException>(ex);
        Assert.Equal(1, callCount); // called exactly once — no retries
    }

    [Fact]
    public async Task RetryStrategy_TimeoutException_NotWrappedInNormException()
    {
        // TimeoutException should propagate as-is, not wrapped, because the catch clause
        // never fires for it.
        using var ctx = BuildContext();
        var strategy = new RetryingExecutionStrategy(ctx, new RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.FromMilliseconds(1) });

        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) => Task.FromException<int>(new TimeoutException("timeout")), CancellationToken.None));

        Assert.IsType<TimeoutException>(ex);
        Assert.IsNotType<NormException>(ex);
    }

    // ── Query path: DbException IS retried ────────────────────────────────────

    [Fact]
    public async Task RetryStrategy_DbExceptionWithRetryablePolicy_IsRetried()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is DbException dbEx && dbEx.HResult == -2147467259
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        int callCount = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                callCount++;
                // Return a faulted task matching ShouldRetry
                var sqliteEx = new Microsoft.Data.Sqlite.SqliteException("simulated transient", 5);
                return Task.FromException<int>(sqliteEx);
            }, CancellationToken.None));

        // Should have been called MaxRetries+1 times (initial + 2 retries)
        Assert.True(callCount >= 2, $"Expected at least 2 calls (with retry), got {callCount}");
        Assert.IsType<NormException>(ex); // wrapped after exhausting retries
    }

    [Fact]
    public async Task RetryStrategy_IOException_IsRetried()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is System.IO.IOException
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        int callCount = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                callCount++;
                return Task.FromException<int>(new System.IO.IOException("network pipe broken"));
            }, CancellationToken.None));

        Assert.True(callCount >= 2);
        Assert.IsType<NormException>(ex);
    }

    [Fact]
    public async Task RetryStrategy_SocketException_IsRetried()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is System.Net.Sockets.SocketException
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        int callCount = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                callCount++;
                return Task.FromException<int>(new System.Net.Sockets.SocketException());
            }, CancellationToken.None));

        Assert.True(callCount >= 2);
    }

    // ── Query path vs save path policy parity ────────────────────────────────

    [Fact]
    public async Task RetryStrategy_TimeoutException_NotCaught_SameBehaviorAsSavePath()
    {
        // The save path (DbContext.WriteWithTransactionAsync retry logic) uses
        // IsRetryableException which returns false for TimeoutException.
        // This test confirms the query path (RetryingExecutionStrategy) also does not
        // retry TimeoutException — both paths now have identical timeout behavior.
        using var ctx = BuildContext();
        var allRetryPolicy = new RetryPolicy
        {
            MaxRetries = 10,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = _ => true // would retry literally everything if the catch fires
        };
        var strategy = new RetryingExecutionStrategy(ctx, allRetryPolicy);

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                attempts++;
                if (attempts == 1) throw new TimeoutException("operation timed out");
                return Task.FromResult(42); // would succeed on retry if retried
            }, CancellationToken.None));

        // TimeoutException must NOT be retried regardless of policy
        Assert.IsType<TimeoutException>(ex);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task RetryStrategy_MaxRetriesExhausted_ThrowsNormException()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is System.IO.IOException
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        int callCount = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                callCount++;
                return Task.FromException<int>(new System.IO.IOException("persistent failure"));
            }, CancellationToken.None));

        Assert.Equal(3, callCount); // initial + 2 retries
        Assert.IsType<NormException>(ex);
    }

    [Fact]
    public async Task RetryStrategy_SucceedsOnSecondAttempt_ReturnsResult()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is System.IO.IOException
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        int callCount = 0;
        var result = await strategy.ExecuteAsync((_, _) =>
        {
            if (++callCount == 1) throw new System.IO.IOException("first attempt fails");
            return Task.FromResult(callCount);
        }, CancellationToken.None);

        Assert.Equal(2, callCount);
        Assert.Equal(2, result);
    }

    // ── Cancellation token is respected ───────────────────────────────────────

    [Fact]
    public async Task RetryStrategy_CancelledToken_StopsRetrying()
    {
        using var ctx = BuildContext();
        var policy = new RetryPolicy
        {
            MaxRetries = 100,
            BaseDelay = TimeSpan.FromMilliseconds(500), // long enough to allow cancellation
            ShouldRetry = ex => ex is System.IO.IOException
        };
        var strategy = new RetryingExecutionStrategy(ctx, policy);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        int callCount = 0;

        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                callCount++;
                return Task.FromException<int>(new System.IO.IOException("transient"));
            }, cts.Token));

        // Either cancelled during delay or after first call
        Assert.True(ex is OperationCanceledException or NormException);
        Assert.True(callCount < 10); // definitely not 100 retries
    }
}
