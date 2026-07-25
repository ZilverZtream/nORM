using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;

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
    /// <para>
    /// <strong>IMPORTANT — implement BOTH families for any command-observing control.</strong> A
    /// direct implementation of this interface that overrides only the async hooks (the natural choice
    /// for an async application) will silently miss every command that runs on a synchronous path — and
    /// several execution paths are synchronous by design: SQLite sets <c>PrefersSyncExecution</c>, and the
    /// small-query fast paths, compiled-query, and query-plan paths execute synchronously on every
    /// provider. A consumer audit log, tenant-predicate guard, or other security control implemented on
    /// the async members alone therefore <em>fails open</em> — it passes the majority of traffic
    /// unobserved with no error. Implement the sync member of every hook you implement, or derive from
    /// <see cref="BaseDbCommandInterceptor"/>, whose sync and async hooks share one logging core and so
    /// behave identically on both paths. See <c>docs/interceptors.md</c>.
    /// </para>
    /// </remarks>
    // IDbCommandInterceptor cannot carry [RequiresDynamicCode]/[RequiresUnreferencedCode] at the
    // interface level; those attributes only apply to classes, constructors, and methods. The
    // implementing types (BaseDbCommandInterceptor and its subclasses) carry the dynamic-code
    // marker, and the AOT baseline at eng/aot-baseline.txt accepts the residual interface-method
    // diagnostics as a known-dynamic surface.
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

        // ── Shared redacted-logging core ─────────────────────────────────────────────────────────
        // The async and sync hook families delegate to ONE logging core so a BaseDbCommandInterceptor
        // subclass observes every command IDENTICALLY on both execution paths. This is a security
        // property, not just DRY: SQLite and the small-query fast paths run synchronously
        // (PrefersSyncExecution), so if the base logged only on the async path, a consumer audit log or
        // policy guard derived from it would silently miss the majority of commands on those providers —
        // failing open. The message templates are byte-identical to the historical async ones so existing
        // structured-log consumers are unaffected. (The INTERFACE default sync hooks stay no-ops: a default
        // implementation cannot safely run a direct implementor's async logic without blocking on the Task,
        // which the interface remarks warn deadlocks single-threaded SynchronizationContexts.)
        // I1: redact SQL before logging to prevent credential / literal data leakage through log sinks.

        private void LogNonQueryExecuting(DbCommand command)
            => Logger.LogInformation("Executing non-query: {CommandText}", SqlRedaction.RedactForLogging(command.CommandText));
        private void LogNonQueryExecuted(int result, TimeSpan duration)
            => Logger.LogInformation("Executed non-query in {Duration}ms, affected {Result} rows", duration.TotalMilliseconds, result);
        private void LogScalarExecuting(DbCommand command)
            => Logger.LogInformation("Executing scalar: {CommandText}", SqlRedaction.RedactForLogging(command.CommandText));
        private void LogScalarExecuted(TimeSpan duration)
            => Logger.LogInformation("Executed scalar in {Duration}ms", duration.TotalMilliseconds);
        private void LogReaderExecuting(DbCommand command)
            => Logger.LogInformation("Executing reader: {CommandText}", SqlRedaction.RedactForLogging(command.CommandText));
        private void LogReaderExecuted(TimeSpan duration)
            => Logger.LogInformation("Executed reader in {Duration}ms", duration.TotalMilliseconds);
        private void LogCommandFailed(DbCommand command, Exception exception)
            => Logger.LogError(exception, "Command failed: {CommandText}", SqlRedaction.RedactForLogging(command.CommandText));

        // ── Async hooks ──────────────────────────────────────────────────────────────────────────

        /// <inheritdoc />
        public virtual Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            LogNonQueryExecuting(command);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        /// <inheritdoc />
        public virtual Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
        {
            LogNonQueryExecuted(result, duration);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            LogScalarExecuting(command);
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        /// <inheritdoc />
        public virtual Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
        {
            LogScalarExecuted(duration);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            LogReaderExecuting(command);
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        /// <inheritdoc />
        public virtual Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
        {
            LogReaderExecuted(duration);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
        {
            LogCommandFailed(command, exception);
            return Task.CompletedTask;
        }

        // ── Sync hooks ───────────────────────────────────────────────────────────────────────────
        // Delegate to the same logging core as the async hooks above, so the built-in interceptor
        // behaves identically on the synchronous execution paths (SQLite / fast paths). A subclass that
        // adds behavior should override BOTH the sync and async member for a hook to stay path-agnostic.

        /// <inheritdoc />
        public virtual InterceptionResult<int> NonQueryExecuting(DbCommand command, DbContext context)
        {
            LogNonQueryExecuting(command);
            return InterceptionResult<int>.Continue();
        }

        /// <inheritdoc />
        public virtual void NonQueryExecuted(DbCommand command, DbContext context, int result, TimeSpan duration)
            => LogNonQueryExecuted(result, duration);

        /// <inheritdoc />
        public virtual InterceptionResult<object?> ScalarExecuting(DbCommand command, DbContext context)
        {
            LogScalarExecuting(command);
            return InterceptionResult<object?>.Continue();
        }

        /// <inheritdoc />
        public virtual void ScalarExecuted(DbCommand command, DbContext context, object? result, TimeSpan duration)
            => LogScalarExecuted(duration);

        /// <inheritdoc />
        public virtual InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            LogReaderExecuting(command);
            return InterceptionResult<DbDataReader>.Continue();
        }

        /// <inheritdoc />
        public virtual void ReaderExecuted(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration)
            => LogReaderExecuted(duration);

        /// <inheritdoc />
        public virtual void CommandFailed(DbCommand command, DbContext context, Exception exception)
            => LogCommandFailed(command, exception);
    }
}
