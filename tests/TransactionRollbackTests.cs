using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TX-1: Verifies that <see cref="TransactionManager.RollbackAsync"/> always uses
/// <see cref="CancellationToken.None"/> internally, so that a canceled caller token
/// cannot abort a rollback and leave the transaction in an ambiguous state.
/// </summary>
public class TransactionRollbackTests
{
    private static Type TransactionManagerType =>
        typeof(DbContext).Assembly.GetType("nORM.Core.TransactionManager", throwOnError: true)!;

    private static async Task<object> CreateTransactionManagerAsync(DbContext ctx, CancellationToken ct)
    {
        var createMethod = TransactionManagerType.GetMethod("CreateAsync",
            BindingFlags.Public | BindingFlags.Static)!;
        // CreateAsync returns Task<TransactionManager>; we need to await it as Task<object>
        var task = (Task)createMethod.Invoke(null, new object[] { ctx, ct })!;
        await task.ConfigureAwait(false);
        // Use reflection to get the Result property
        return task.GetType().GetProperty("Result")!.GetValue(task)!;
    }

    private static async ValueTask InvokeRollbackAsync(object tm, CancellationToken ct)
    {
        var rollbackMethod = TransactionManagerType.GetMethod("RollbackAsync",
            BindingFlags.Public | BindingFlags.Instance)!;
        var result = rollbackMethod.Invoke(tm, new object[] { ct });
        if (result is ValueTask vt)
            await vt;
    }

    private static async ValueTask InvokeDisposeAsync(object tm)
    {
        var disposeMethod = TransactionManagerType.GetMethod("DisposeAsync",
            BindingFlags.Public | BindingFlags.Instance)!;
        var result = disposeMethod.Invoke(tm, null);
        if (result is ValueTask vt)
            await vt;
    }

    [Fact]
    public async Task RollbackAsync_CompletesEvenWhenCallerTokenIsCanceled()
    {
        // TX-1: Simulate: operation fails, caller token is canceled before rollback.
        // Rollback must still complete (not throw OperationCanceledException).
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        await using var ctx = new DbContext(cn, new SqliteProvider());

        using var cts = new CancellationTokenSource();

        // Create transaction manager with a valid (non-canceled) token
        var tm = await CreateTransactionManagerAsync(ctx, CancellationToken.None);

        // Now cancel the token AFTER the transaction is started — simulates mid-operation cancellation
        cts.Cancel();

        // RollbackAsync must not throw OperationCanceledException even with a canceled token,
        // because TX-1 fix makes it always use CancellationToken.None internally.
        var exception = await Record.ExceptionAsync(async () =>
        {
            await InvokeRollbackAsync(tm, cts.Token);
        });

        Assert.Null(exception); // Rollback completed without throwing

        await InvokeDisposeAsync(tm);
    }

    [Fact]
    public async Task RollbackAsync_WithPreCanceledToken_DoesNotThrow()
    {
        // TX-1: Even if the token is already canceled when RollbackAsync is called,
        // rollback must complete because it uses CancellationToken.None internally.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        await using var ctx = new DbContext(cn, new SqliteProvider());

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel

        // Create TransactionManager with a fresh token (to allow BeginTransactionAsync to succeed)
        var tm = await CreateTransactionManagerAsync(ctx, CancellationToken.None);

        var exception = await Record.ExceptionAsync(async () =>
        {
            // Pass the pre-canceled token to RollbackAsync — must not throw
            await InvokeRollbackAsync(tm, cts.Token);
        });

        Assert.Null(exception);

        await InvokeDisposeAsync(tm);
    }
}
