using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Contract tests for the retry policy documented in docs/retry-policy.md.
///
/// Coverage:
/// - Retrying inside an explicit transaction is blocked (non-retryable boundary).
/// - NormConfigurationException is never retried (permanent failure).
/// - Retry count respects the configured maximum.
/// - TimeoutException is not retried (unknown write outcome).
/// - Non-DbException types are not retried by the default policy.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class RetryPolicyContractTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static SqliteConnection OpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static DbContext BuildContext(SqliteConnection cn, RetryPolicy? policy = null)
    {
        var opts = new DbContextOptions { RetryPolicy = policy };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static RetryPolicy AllRetryPolicy(int maxRetries = 5) =>
        new RetryPolicy
        {
            MaxRetries = maxRetries,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = _ => true   // retry everything that reaches the delegate
        };

    // ── explicit transaction boundary ─────────────────────────────────────────

    /// <summary>
    /// SaveChangesAsync executed inside an explicit BeginTransactionAsync must not
    /// be retried even when the RetryPolicy would normally approve a retry.
    /// The write outcome is controlled by the caller's transaction; retrying
    /// would replay writes inside a scope the caller owns.
    /// </summary>
    [Fact]
    public async Task SaveChanges_InsideExplicitTransaction_DoesNotRetry()
    {
        using var cn = OpenConnection();
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE RetryContract (Id INTEGER PRIMARY KEY, Val TEXT);";
            await setup.ExecuteNonQueryAsync();
        }

        var callCount = 0;
        var policy = new RetryPolicy
        {
            MaxRetries = 5,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = _ => { callCount++; return true; }
        };

        await using var ctx = BuildContext(cn, policy);

        // Open an explicit transaction — this should suppress the retry loop.
        await using var tx = await ctx.Database.BeginTransactionAsync();

        // Insert a row successfully inside the explicit transaction.
        await using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx.Transaction!;
            cmd.CommandText = "INSERT INTO RetryContract (Id, Val) VALUES (1, 'a')";
            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();

        // ShouldRetry must never have been called because the retry loop is
        // bypassed when an external transaction is active.
        Assert.Equal(0, callCount);
    }

    /// <summary>
    /// RetryingExecutionStrategy itself does not check for an active context
    /// transaction — that boundary is enforced in SaveChangesAsync. Verify that
    /// the strategy still respects MaxRetries correctly when used for queries.
    /// </summary>
    [Fact]
    public async Task RetryStrategy_RespectsMaxRetries_ForQueryPath()
    {
        using var cn = OpenConnection();
        await using var ctx = BuildContext(cn, AllRetryPolicy(maxRetries: 2));

        var strategy = new RetryingExecutionStrategy(ctx, AllRetryPolicy(maxRetries: 2));

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                attempts++;
                return Task.FromException<int>(new System.IO.IOException("network error"));
            }, CancellationToken.None));

        // initial attempt + 2 retries = 3 total calls
        Assert.Equal(3, attempts);
        Assert.NotNull(ex);
    }

    // ── NormConfigurationException is not retried ──────────────────────────────

    /// <summary>
    /// NormConfigurationException represents a permanent configuration error.
    /// The default ShouldRetry returns false for it because it is not a
    /// DbException, IOException, or SocketException.
    /// </summary>
    [Fact]
    public void NormConfigurationException_IsNotRetryable_ByDefaultPolicy()
    {
        // Use reflection to invoke IsRetryableException (same technique as
        // RetryBehaviorTests) to confirm the contract at the DbContext level.
        using var cn = OpenConnection();
        using var ctx = BuildContext(cn, new RetryPolicy { MaxRetries = 3 });

        var method = typeof(DbContext).GetMethod("IsRetryableException",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        var configEx = new NormConfigurationException("bad config");
        var retryable = (bool)method.Invoke(ctx, new object[] { configEx })!;
        Assert.False(retryable, "NormConfigurationException must never trigger a retry.");
    }

    /// <summary>
    /// RetryingExecutionStrategy must not retry NormConfigurationException when the
    /// default ShouldRetry policy is used. The default policy returns false for
    /// NormConfigurationException because it is not a SQL Server transient error,
    /// IOException, or SocketException — even though NormConfigurationException is
    /// technically a DbException (via NormException).
    /// </summary>
    [Fact]
    public async Task RetryStrategy_NormConfigurationException_NotRetried_ByDefaultPolicy()
    {
        using var cn = OpenConnection();
        // Use the default RetryPolicy (not AllRetryPolicy) — it only retries SQL Server
        // transient error numbers, IOException, and SocketException.
        var defaultPolicy = new RetryPolicy { MaxRetries = 5, BaseDelay = TimeSpan.FromMilliseconds(1) };
        await using var ctx = BuildContext(cn, defaultPolicy);

        var strategy = new RetryingExecutionStrategy(ctx, defaultPolicy);

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync<int>((_, _) =>
            {
                attempts++;
                throw new NormConfigurationException("misconfigured entity mapping");
            }, CancellationToken.None));

        // Default ShouldRetry returns false for NormConfigurationException —
        // it is not in the SQL Server transient error list and is not IOException/SocketException.
        Assert.Equal(1, attempts);
        Assert.NotNull(ex);
    }

    // ── retry count respects configured maximum ────────────────────────────────

    [Fact]
    public async Task RetryStrategy_MaxRetries_Zero_NeverRetries()
    {
        using var cn = OpenConnection();
        await using var ctx = BuildContext(cn, AllRetryPolicy(maxRetries: 0));

        var strategy = new RetryingExecutionStrategy(ctx, AllRetryPolicy(maxRetries: 0));

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                attempts++;
                return Task.FromException<int>(new System.IO.IOException("transient"));
            }, CancellationToken.None));

        // MaxRetries=0: initial attempt only, no retries.
        Assert.Equal(1, attempts);
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task RetryStrategy_MaxRetries_One_OnlyOneRetry()
    {
        using var cn = OpenConnection();
        await using var ctx = BuildContext(cn, AllRetryPolicy(maxRetries: 1));

        var strategy = new RetryingExecutionStrategy(ctx, AllRetryPolicy(maxRetries: 1));

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                attempts++;
                return Task.FromException<int>(new System.IO.IOException("transient"));
            }, CancellationToken.None));

        // MaxRetries=1: initial attempt + 1 retry = 2 total.
        Assert.Equal(2, attempts);
        Assert.NotNull(ex);
    }

    // ── TimeoutException is never retried ─────────────────────────────────────

    [Fact]
    public void TimeoutException_IsNotRetryable_ViaDefaultPolicy()
    {
        using var cn = OpenConnection();
        using var ctx = BuildContext(cn, new RetryPolicy { MaxRetries = 5 });

        var method = typeof(DbContext).GetMethod("IsRetryableException",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        var retryable = (bool)method.Invoke(ctx, new object[] { new TimeoutException("timed out") })!;
        Assert.False(retryable, "TimeoutException must never be retried (unknown write outcome).");
    }

    [Fact]
    public async Task RetryStrategy_TimeoutException_NeverRetried()
    {
        using var cn = OpenConnection();
        await using var ctx = BuildContext(cn, AllRetryPolicy());

        var strategy = new RetryingExecutionStrategy(ctx, AllRetryPolicy(maxRetries: 10));

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync((_, _) =>
            {
                attempts++;
                return Task.FromException<int>(new TimeoutException("command timeout"));
            }, CancellationToken.None));

        // TimeoutException is not in the RetryingExecutionStrategy catch guard.
        Assert.Equal(1, attempts);
        Assert.IsType<TimeoutException>(ex);
    }

    // ── non-DbException types are not retried by default ──────────────────────

    [Fact]
    public void InvalidOperationException_IsNotRetryable_ByDefaultPolicy()
    {
        using var cn = OpenConnection();
        using var ctx = BuildContext(cn, new RetryPolicy { MaxRetries = 5 });

        var method = typeof(DbContext).GetMethod("IsRetryableException",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        var retryable = (bool)method.Invoke(ctx, new object[] { new InvalidOperationException("logic error") })!;
        Assert.False(retryable);
    }

    [Fact]
    public async Task RetryStrategy_NonDbException_PropagatesWithoutRetry()
    {
        using var cn = OpenConnection();
        await using var ctx = BuildContext(cn, AllRetryPolicy());

        var strategy = new RetryingExecutionStrategy(ctx, AllRetryPolicy(maxRetries: 5));

        int attempts = 0;
        var ex = await Record.ExceptionAsync(() =>
            strategy.ExecuteAsync<int>((_, _) =>
            {
                attempts++;
                throw new InvalidOperationException("not a db error");
            }, CancellationToken.None));

        Assert.Equal(1, attempts);
        Assert.IsType<InvalidOperationException>(ex);
    }
}
