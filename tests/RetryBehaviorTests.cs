using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// T1: Verifies that <see cref="TimeoutException"/> does NOT trigger retry by default
/// (to avoid duplicate writes), while a <see cref="DbException"/> with a custom
/// <c>ShouldRetry</c> returning true DOES trigger retry.
/// </summary>
public class RetryBehaviorTests
{
    // Helper: check the private IsRetryableException method via reflection
    private static bool IsRetryableException(DbContext ctx, Exception ex)
    {
        var method = typeof(DbContext).GetMethod("IsRetryableException",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        return (bool)method.Invoke(ctx, new object[] { ex })!;
    }

    private static DbContext CreateContext(RetryPolicy? policy = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var opts = new DbContextOptions { RetryPolicy = policy };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void TimeoutException_IsNotRetryable_ByDefault()
    {
        // T1: Even with a policy configured, a bare TimeoutException must not be retried
        // because we cannot know whether the write was applied.
        using var ctx = CreateContext(new RetryPolicy { MaxRetries = 3 });
        var ex = new TimeoutException("command timed out");

        // IsRetryableException only returns true for DbException where ShouldRetry returns true
        Assert.False(IsRetryableException(ctx, ex));
    }

    [Fact]
    public void TimeoutException_IsNotRetryable_WithNoPolicy()
    {
        using var ctx = CreateContext(policy: null);
        var ex = new TimeoutException("command timed out");
        Assert.False(IsRetryableException(ctx, ex));
    }

    [Fact]
    public void DbException_WithShouldRetryTrue_IsRetryable()
    {
        // A custom policy that marks all DbExceptions as retryable
        var policy = new RetryPolicy
        {
            MaxRetries = 3,
            ShouldRetry = _ => true  // always retry
        };
        using var ctx = CreateContext(policy);

        // Use a real (but benign) SqliteException as a DbException stand-in
        DbException dbEx;
        try
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT * FROM NonExistentTable";
            cmd.ExecuteNonQuery(); // throws SqliteException (a DbException)
            throw new InvalidOperationException("Should not reach here");
        }
        catch (DbException e)
        {
            dbEx = e;
        }

        Assert.True(IsRetryableException(ctx, dbEx));
    }

    [Fact]
    public void DbException_WithShouldRetryFalse_IsNotRetryable()
    {
        var policy = new RetryPolicy
        {
            MaxRetries = 3,
            ShouldRetry = _ => false  // never retry
        };
        using var ctx = CreateContext(policy);

        DbException dbEx;
        try
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT * FROM NonExistentTable2";
            cmd.ExecuteNonQuery();
            throw new InvalidOperationException("Should not reach here");
        }
        catch (DbException e)
        {
            dbEx = e;
        }

        Assert.False(IsRetryableException(ctx, dbEx));
    }
}
