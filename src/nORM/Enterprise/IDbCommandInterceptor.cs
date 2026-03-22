using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Query;

#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Defines hooks that are invoked before and after execution of <see cref="DbCommand"/> instances.
    /// Implementations may inspect or modify the command, short-circuit execution, or react to failures.
    /// </summary>
    /// <remarks>
    /// Two contract tiers are provided:
    /// <list type="bullet">
    ///   <item><term>Async (…Async methods)</term><description>Invoked from all async execution paths.
    ///     Implementations may freely use <c>await</c>.</description></item>
    ///   <item><term>Sync (non-Async methods)</term><description>Invoked from synchronous execution
    ///     paths (e.g. <c>ToList()</c>). Default implementations are no-ops. Override these when
    ///     synchronous interception is needed; do <em>not</em> block on async work from these
    ///     implementations as doing so can deadlock single-threaded
    ///     <see cref="System.Threading.SynchronizationContext"/> environments.</description></item>
    /// </list>
    /// </remarks>
    public interface IDbCommandInterceptor
    {
        // ── Async hooks ──────────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Called before a command that does not return rows is executed.
        /// Returning a suppressed result prevents command execution and returns the provided value instead.
        /// </summary>
        Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Called after a command that does not return rows has executed.
        /// </summary>
        Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Called before a command that returns a scalar value is executed.
        /// Returning a suppressed result prevents command execution and returns the provided value instead.
        /// </summary>
        Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Called after a command that returns a scalar value has executed.
        /// </summary>
        Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Called before a command that returns a reader is executed.
        /// Returning a suppressed reader prevents command execution and returns the provided reader instead.
        /// </summary>
        Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Called after a command that returns a reader has executed.
        /// </summary>
        Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Called when execution of a command results in an exception.
        /// </summary>
        Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken);

        // ── Sync hooks ───────────────────────────────────────────────────────────────────────────
        // These are invoked from synchronous execution paths. Default implementations are no-ops
        // so existing implementations continue to compile without modification.

        /// <summary>
        /// Called synchronously before a command that does not return rows is executed.
        /// Return <see cref="InterceptionResult{T}.SuppressWithResult"/> to short-circuit execution.
        /// The default implementation is a no-op.
        /// </summary>
        InterceptionResult<int> NonQueryExecuting(DbCommand command, DbContext context)
            => InterceptionResult<int>.Continue();

        /// <summary>
        /// Called synchronously after a command that does not return rows has executed.
        /// The default implementation is a no-op.
        /// </summary>
        void NonQueryExecuted(DbCommand command, DbContext context, int result, TimeSpan duration) { }

        /// <summary>
        /// Called synchronously before a command that returns a scalar value is executed.
        /// Return <see cref="InterceptionResult{T}.SuppressWithResult"/> to short-circuit execution.
        /// The default implementation is a no-op.
        /// </summary>
        InterceptionResult<object?> ScalarExecuting(DbCommand command, DbContext context)
            => InterceptionResult<object?>.Continue();

        /// <summary>
        /// Called synchronously after a command that returns a scalar value has executed.
        /// The default implementation is a no-op.
        /// </summary>
        void ScalarExecuted(DbCommand command, DbContext context, object? result, TimeSpan duration) { }

        /// <summary>
        /// Called synchronously before a command that returns a reader is executed.
        /// Return <see cref="InterceptionResult{T}.SuppressWithResult"/> to short-circuit execution.
        /// The default implementation is a no-op.
        /// </summary>
        InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
            => InterceptionResult<DbDataReader>.Continue();

        /// <summary>
        /// Called synchronously after a command that returns a reader has executed.
        /// The default implementation is a no-op.
        /// </summary>
        void ReaderExecuted(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration) { }

        /// <summary>
        /// Called synchronously when execution of a command results in an exception.
        /// The default implementation is a no-op.
        /// </summary>
        void CommandFailed(DbCommand command, DbContext context, Exception exception) { }
    }

    /// <summary>
    /// Base implementation of <see cref="IDbCommandInterceptor"/> that logs command execution.
    /// </summary>
    public abstract class BaseDbCommandInterceptor : IDbCommandInterceptor
    {
        /// <summary>
        /// Logger used to emit diagnostic messages for command execution.
        /// </summary>
        protected ILogger Logger { get; }

        /// <summary>
        /// Initializes a new instance of the interceptor using the provided logger.
        /// </summary>
        /// <param name="logger">Logger used to emit diagnostic messages.</param>
        protected BaseDbCommandInterceptor(ILogger logger)
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc />
        public virtual Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            // I1: use the same SQL redaction policy applied by QueryExecutor to prevent
            // credential / literal data leakage through log sinks.
            Logger.LogInformation("Executing non-query: {CommandText}", QueryExecutor.RedactSqlForLogging(command.CommandText));
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        /// <inheritdoc />
        public virtual Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
        {
            Logger.LogInformation("Executed non-query in {Duration}ms, affected {Result} rows", duration.TotalMilliseconds, result);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            // I1: redact SQL before logging.
            Logger.LogInformation("Executing scalar: {CommandText}", QueryExecutor.RedactSqlForLogging(command.CommandText));
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        /// <inheritdoc />
        public virtual Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
        {
            Logger.LogInformation("Executed scalar in {Duration}ms, result {Result}", duration.TotalMilliseconds, result);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            // I1: redact SQL before logging.
            Logger.LogInformation("Executing reader: {CommandText}", QueryExecutor.RedactSqlForLogging(command.CommandText));
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        /// <inheritdoc />
        public virtual Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
        {
            Logger.LogInformation("Executed reader in {Duration}ms", duration.TotalMilliseconds);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
        {
            // I1: redact SQL before logging to prevent credential leakage in error logs.
            Logger.LogError(exception, "Command failed: {CommandText}", QueryExecutor.RedactSqlForLogging(command.CommandText));
            return Task.CompletedTask;
        }

        // ── Sync hook overrides (no-ops in base; override in subclasses that need sync interception) ──

        /// <inheritdoc />
        public virtual InterceptionResult<int> NonQueryExecuting(DbCommand command, DbContext context)
            => InterceptionResult<int>.Continue();

        /// <inheritdoc />
        public virtual void NonQueryExecuted(DbCommand command, DbContext context, int result, TimeSpan duration) { }

        /// <inheritdoc />
        public virtual InterceptionResult<object?> ScalarExecuting(DbCommand command, DbContext context)
            => InterceptionResult<object?>.Continue();

        /// <inheritdoc />
        public virtual void ScalarExecuted(DbCommand command, DbContext context, object? result, TimeSpan duration) { }

        /// <inheritdoc />
        public virtual InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
            => InterceptionResult<DbDataReader>.Continue();

        /// <inheritdoc />
        public virtual void ReaderExecuted(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration) { }

        /// <inheritdoc />
        public virtual void CommandFailed(DbCommand command, DbContext context, Exception exception) { }
    }
}
