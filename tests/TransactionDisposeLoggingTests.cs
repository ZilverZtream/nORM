using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using Xunit;

#nullable enable

namespace nORM.Tests;

//<summary>
//Verifies that unexpected exceptions thrown during DbTransaction disposal are
//routed to the ILogger as warnings rather than being silently swallowed (Debug.WriteLine)
//or rethrown (violates .NET Dispose contract).
//</summary>
public class TransactionDisposeLoggingTests
{
 // ── Fake ILogger ─────────────────────────────────────────────────────────

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, Exception? Ex, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add((logLevel, exception, formatter(state, exception)));
        }
    }

 // ── Throwing DbTransaction ───────────────────────────────────────────────

 //<summary>
 //A DbTransaction whose Dispose and DisposeAsync throw a generic Exception —
 //simulating an unexpected error from a misbehaving provider.
 //</summary>
    private sealed class ThrowingTransaction : DbTransaction
    {
        protected override DbConnection? DbConnection => null;
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
        public override void Commit() { }
        public override void Rollback() { }

        protected override void Dispose(bool disposing)
            => throw new Exception("simulated unexpected dispose error");

        public override ValueTask DisposeAsync()
            => throw new Exception("simulated unexpected async dispose error");
    }

 // ── TransactionManager factory via reflection ────────────────────────────

 //<summary>
 //Constructs a TransactionManager that owns the given transaction and has the given logger,
 //by invoking the private constructor via reflection.
 //Signature: TransactionManager(DbTransaction?, bool ownsTransaction, CancellationTokenSource?, CancellationToken, ILogger?)
 //</summary>
    private static TransactionManager CreateManager(DbTransaction tx, ILogger logger)
    {
        var ctor = typeof(TransactionManager).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { typeof(DbTransaction), typeof(bool), typeof(CancellationTokenSource), typeof(CancellationToken), typeof(ILogger) },
            null)!;

        return (TransactionManager)ctor.Invoke(new object?[]
        {
            tx,
            true,            // ownsTransaction = true so Dispose path is taken
            null,            // no CTS
            CancellationToken.None,
            logger
        });
    }

 // ── Tests ────────────────────────────────────────────────────────────────

 //<summary>
 //When Transaction.DisposeAsync() throws unexpectedly, TransactionManager.DisposeAsync
 //must NOT rethrow and must call ILogger.LogWarning with the exception.
 //</summary>
    [Fact]
    public async Task TransactionDisposeAsync_UnexpectedException_LogsWarning()
    {
        var logger = new CapturingLogger();
        var manager = CreateManager(new ThrowingTransaction(), logger);

 // DisposeAsync must not throw even though the underlying DisposeAsync throws.
        var ex = await Record.ExceptionAsync(() => manager.DisposeAsync().AsTask());
        Assert.Null(ex);

 // The warning must have been logged.
        Assert.Contains(logger.Entries, e =>
            e.Level == LogLevel.Warning &&
            e.Ex is not null &&
            e.Message.Contains("indeterminate", StringComparison.OrdinalIgnoreCase));
    }

 //<summary>
 //When Transaction.Dispose() throws unexpectedly, TransactionManager.Dispose
 //must NOT rethrow and must call ILogger.LogWarning with the exception.
 //</summary>
    [Fact]
    public async Task TransactionDispose_UnexpectedException_LogsWarning()
    {
        var logger = new CapturingLogger();
        var manager = CreateManager(new ThrowingTransaction(), logger);

 // Sync Dispose must not throw.
        var ex = Record.Exception(() => manager.Dispose());
        Assert.Null(ex);

 // The warning must have been logged.
        Assert.Contains(logger.Entries, e =>
            e.Level == LogLevel.Warning &&
            e.Ex is not null &&
            e.Message.Contains("indeterminate", StringComparison.OrdinalIgnoreCase));

        await Task.CompletedTask; // satisfy async signature
    }
}
