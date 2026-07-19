using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Enterprise;

#nullable enable

namespace nORM.Internal
{
    internal static class CommandInterceptorExtensions
    {
        private static readonly ConditionalWeakTable<DbConnection, SemaphoreSlim> s_serializedConnectionGates = new();

        private static SemaphoreSlim CreateGate(DbConnection _) => new(1, 1);

        private static System.Collections.Generic.IList<IDbCommandInterceptor> GetCommandInterceptors(DbContext ctx)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count > 0 && ctx.IsStrictProviderMobility)
                ctx.ThrowIfStrictProviderMobilityEscapeHatch("command interceptors");
            return interceptors;
        }

        internal static SemaphoreSlim? GetSerializedConnectionGate(DbCommand command, DbContext ctx)
        {
            var connection = command.Connection;
            return connection != null ? GetSerializedConnectionGate(connection, ctx) : null;
        }

        internal static SemaphoreSlim? GetSerializedConnectionGate(DbConnection connection, DbContext ctx)
            => ctx.RawProvider.PrefersSyncExecution
                ? s_serializedConnectionGates.GetValue(connection, CreateGate)
                : null;

        /// <summary>
        /// Disposes a command while holding the connection's serialization gate. Command disposal finalizes
        /// the provider's prepared statement, which touches the same underlying connection handle that
        /// <c>ExecuteScalar</c>/<c>ExecuteReader</c>/<c>ExecuteNonQuery</c> use. On a sync-serialized
        /// connection (e.g. SQLite, which is not safe for concurrent use of one connection) an ungated
        /// dispose races a concurrent execution on another thread and can NRE or corrupt state; gating it
        /// keeps the whole per-connection command lifecycle serialized. Deadlock-safe: the execution block
        /// releases the gate before the disposing finally runs, so this never re-acquires a gate it holds.
        /// </summary>
        private static void DisposeCommandSerialized(DbCommand command, SemaphoreSlim gate)
        {
            gate.Wait();
            try { command.Dispose(); }
            finally { gate.Release(); }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteNonQueryAsync()"/> while invoking any registered
        /// command interceptors before and after execution.
        /// </summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected.</returns>
        public static Task<int> ExecuteNonQueryWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = GetCommandInterceptors(ctx);
            var gate = GetSerializedConnectionGate(command, ctx);
            if (interceptors.Count == 0)
            {
                // Return the task directly — avoids async state machine allocation
                if (gate != null)
                    return ExecuteNonQuerySerializedAsync(command, gate, ct);
                return command.ExecuteNonQueryAsync(ct);
            }
            return ExecuteNonQueryWithInterceptionSlowAsync(command, ctx, interceptors, ct, gate);
        }

        private static async Task<int> ExecuteNonQuerySerializedAsync(DbCommand command, SemaphoreSlim gate, CancellationToken ct)
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        }

        private static async Task<int> ExecuteNonQueryWithInterceptionSlowAsync(DbCommand command, DbContext ctx, System.Collections.Generic.IList<IDbCommandInterceptor> interceptors, CancellationToken ct, SemaphoreSlim? gate)
        {

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.NonQueryExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.NonQueryExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            var gateHeld = false;
            try
            {
                if (gate != null)
                {
                    await gate.WaitAsync(ct).ConfigureAwait(false);
                    gateHeld = true;
                }
                var result = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                if (gateHeld)
                {
                    gate!.Release();
                    gateHeld = false;
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.NonQueryExecutedAsync(command, ctx, result, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return result;
            }
            catch (Exception ex)
            {
                if (gateHeld)
                {
                    gate!.Release();
                    gateHeld = false;
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                if (gateHeld)
                    gate!.Release();
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteNonQuery"/> while invoking registered command interceptors
        /// before and after execution. Interceptors are notified via their synchronous hooks
        /// (<see cref="IDbCommandInterceptor.NonQueryExecuting"/>) to avoid deadlocking
        /// single-threaded <see cref="System.Threading.SynchronizationContext"/> environments.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <returns>The number of rows affected.</returns>
        public static int ExecuteNonQueryWithInterception(this DbCommand command, DbContext ctx)
        {
            var interceptors = GetCommandInterceptors(ctx);
            var gate = GetSerializedConnectionGate(command, ctx);
            if (interceptors.Count == 0)
            {
                if (gate == null)
                    return command.ExecuteNonQuery();

                gate.Wait();
                try
                {
                    return command.ExecuteNonQuery();
                }
                finally
                {
                    gate.Release();
                }
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.NonQueryExecuting(command, ctx);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.NonQueryExecuted(command, ctx, interception.Result, TimeSpan.Zero);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            var gateHeld = false;
            try
            {
                if (gate != null)
                {
                    gate.Wait();
                    gateHeld = true;
                }
                var result = command.ExecuteNonQuery();
                if (gateHeld)
                {
                    gate!.Release();
                    gateHeld = false;
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                    interceptor.NonQueryExecuted(command, ctx, result, sw.Elapsed);
                return result;
            }
            catch (Exception ex)
            {
                if (gateHeld)
                {
                    gate!.Release();
                    gateHeld = false;
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                    interceptor.CommandFailed(command, ctx, ex);
                throw;
            }
            finally
            {
                if (gateHeld)
                    gate!.Release();
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteScalarAsync()"/> while invoking registered
        /// command interceptors surrounding the call.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The scalar result returned by the command.</returns>
        public static Task<object?> ExecuteScalarWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = GetCommandInterceptors(ctx);
            var gate = GetSerializedConnectionGate(command, ctx);
            if (interceptors.Count == 0)
            {
                // Return the task directly — avoids async state machine allocation
                if (gate != null)
                    return ExecuteScalarSerializedAsync(command, gate, ct);
                return command.ExecuteScalarAsync(ct);
            }
            return ExecuteScalarWithInterceptionSlowAsync(command, ctx, interceptors, ct, gate);
        }

        private static async Task<object?> ExecuteScalarSerializedAsync(DbCommand command, SemaphoreSlim gate, CancellationToken ct)
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                return await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        }

        private static async Task<object?> ExecuteScalarWithInterceptionSlowAsync(DbCommand command, DbContext ctx, System.Collections.Generic.IList<IDbCommandInterceptor> interceptors, CancellationToken ct, SemaphoreSlim? gate)
        {

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ScalarExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ScalarExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            var gateHeld = false;
            try
            {
                if (gate != null)
                {
                    await gate.WaitAsync(ct).ConfigureAwait(false);
                    gateHeld = true;
                }
                var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
                if (gateHeld)
                {
                    gate!.Release();
                    gateHeld = false;
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ScalarExecutedAsync(command, ctx, result, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return result;
            }
            catch (Exception ex)
            {
                if (gateHeld)
                {
                    gate!.Release();
                    gateHeld = false;
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                if (gateHeld)
                    gate!.Release();
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteScalar"/> while invoking registered command interceptors
        /// before and after execution. Interceptors are notified via their synchronous hooks
        /// (<see cref="IDbCommandInterceptor.ScalarExecuting"/>) to avoid deadlocking
        /// single-threaded <see cref="System.Threading.SynchronizationContext"/> environments.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <returns>The scalar result returned by the command.</returns>
        public static object? ExecuteScalarWithInterception(this DbCommand command, DbContext ctx)
            => ExecuteScalarWithInterceptionSerialized(command, ctx);

        internal static object? ExecuteScalarWithInterceptionSerialized(this DbCommand command, DbContext ctx)
        {
            var gate = GetSerializedConnectionGate(command, ctx);
            if (gate == null)
                return ExecuteScalarWithInterceptionCore(command, ctx);

            return ExecuteScalarWithInterceptionCoreSerialized(command, ctx, gate, disposeCommand: false);
        }

        internal static object? ExecuteScalarWithInterceptionSerializedAndDispose(this DbCommand command, DbContext ctx)
        {
            var gate = GetSerializedConnectionGate(command, ctx);
            if (gate == null)
            {
                try { return ExecuteScalarWithInterceptionCore(command, ctx); }
                finally { command.Dispose(); }
            }

            return ExecuteScalarWithInterceptionCoreSerialized(command, ctx, gate, disposeCommand: true);
        }

        private static object? ExecuteScalarWithInterceptionCoreSerialized(DbCommand command, DbContext ctx, SemaphoreSlim gate, bool disposeCommand)
        {
            try
            {
                var interceptors = GetCommandInterceptors(ctx);
                if (interceptors.Count == 0)
                {
                    gate.Wait();
                    try
                    {
                        return command.ExecuteScalar();
                    }
                    finally
                    {
                        gate.Release();
                    }
                }

                foreach (var interceptor in interceptors)
                {
                    var interception = interceptor.ScalarExecuting(command, ctx);
                    if (interception.IsSuppressed)
                    {
                        foreach (var i in interceptors)
                            i.ScalarExecuted(command, ctx, interception.Result, TimeSpan.Zero);
                        return interception.Result;
                    }
                }

                var sw = Stopwatch.StartNew();
                try
                {
                    gate.Wait();
                    object? result;
                    try
                    {
                        result = command.ExecuteScalar();
                    }
                    finally
                    {
                        gate.Release();
                    }

                    sw.Stop();
                    foreach (var interceptor in interceptors)
                        interceptor.ScalarExecuted(command, ctx, result, sw.Elapsed);
                    return result;
                }
                catch (Exception ex)
                {
                    sw.Stop();
                    foreach (var interceptor in interceptors)
                        interceptor.CommandFailed(command, ctx, ex);
                    throw;
                }
            }
            finally
            {
                if (disposeCommand)
                    DisposeCommandSerialized(command, gate);
            }
        }

        private static object? ExecuteScalarWithInterceptionCore(DbCommand command, DbContext ctx)
        {
            var interceptors = GetCommandInterceptors(ctx);
            if (interceptors.Count == 0)
            {
                return command.ExecuteScalar();
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.ScalarExecuting(command, ctx);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.ScalarExecuted(command, ctx, interception.Result, TimeSpan.Zero);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = command.ExecuteScalar();
                sw.Stop();
                foreach (var interceptor in interceptors)
                    interceptor.ScalarExecuted(command, ctx, result, sw.Elapsed);
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                    interceptor.CommandFailed(command, ctx, ex);
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
        /// while invoking registered command interceptors around the execution.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <param name="behavior">Reader behavior flags.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The resulting <see cref="DbDataReader"/>.</returns>
        public static Task<DbDataReader> ExecuteReaderWithInterceptionAsync(this DbCommand command, DbContext ctx, CommandBehavior behavior, CancellationToken ct)
        {
            var interceptors = GetCommandInterceptors(ctx);
            var gate = GetSerializedConnectionGate(command, ctx);
            if (interceptors.Count == 0)
            {
                // Return the task directly — avoids async state machine allocation
                if (gate != null)
                    return ExecuteReaderSerializedAsync(command, gate, behavior, ct);
                return command.ExecuteReaderAsync(behavior, ct);
            }
            return ExecuteReaderWithInterceptionSlowAsync(command, ctx, interceptors, behavior, ct, gate);
        }

        private static async Task<DbDataReader> ExecuteReaderSerializedAsync(DbCommand command, SemaphoreSlim gate, CommandBehavior behavior, CancellationToken ct)
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var reader = await command.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);
                return new SerializedDbDataReader(reader, gate);
            }
            catch
            {
                gate.Release();
                throw;
            }
        }

        private static async Task<DbDataReader> ExecuteReaderWithInterceptionSlowAsync(DbCommand command, DbContext ctx, System.Collections.Generic.IList<IDbCommandInterceptor> interceptors, CommandBehavior behavior, CancellationToken ct, SemaphoreSlim? gate)
        {

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ReaderExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ReaderExecutedAsync(command, ctx, interception.Result!, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                if (gate != null)
                    await gate.WaitAsync(ct).ConfigureAwait(false);
                var reader = await command.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ReaderExecutedAsync(command, ctx, reader, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return gate != null ? new SerializedDbDataReader(reader, gate) : reader;
            }
            catch (Exception ex)
            {
                gate?.Release();
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteReader(CommandBehavior)"/> while wrapping the
        /// call with the currently registered command interceptors. Interceptors are notified
        /// via their synchronous hooks (<see cref="IDbCommandInterceptor.ReaderExecuting"/>) to
        /// avoid deadlocking single-threaded <see cref="System.Threading.SynchronizationContext"/>
        /// environments.
        /// </summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="ctx">The <see cref="DbContext"/> associated with the command.</param>
        /// <param name="behavior">Behavior flags that influence reader execution.</param>
        /// <returns>The <see cref="DbDataReader"/> returned by the command execution.</returns>
        public static DbDataReader ExecuteReaderWithInterception(this DbCommand command, DbContext ctx, CommandBehavior behavior)
            => ExecuteReaderWithInterceptionCore(command, ctx, behavior, disposeCommandWithReader: false);

        internal static DbDataReader ExecuteReaderWithInterceptionAndCommandDispose(this DbCommand command, DbContext ctx, CommandBehavior behavior)
            => ExecuteReaderWithInterceptionCore(command, ctx, behavior, disposeCommandWithReader: true);

        private static DbDataReader ExecuteReaderWithInterceptionCore(DbCommand command, DbContext ctx, CommandBehavior behavior, bool disposeCommandWithReader)
        {
            var interceptors = GetCommandInterceptors(ctx);
            var gate = GetSerializedConnectionGate(command, ctx);
            if (interceptors.Count == 0)
            {
                return ExecuteReaderSerializedIfNeeded(command, gate, behavior, disposeCommandWithReader);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.ReaderExecuting(command, ctx);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.ReaderExecuted(command, ctx, interception.Result!, TimeSpan.Zero);
                    return disposeCommandWithReader
                        ? new SerializedDbDataReader(interception.Result!, gate, command, holdGateUntilDispose: true)
                        : interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                gate?.Wait();
                var reader = command.ExecuteReader(behavior);
                sw.Stop();
                foreach (var interceptor in interceptors)
                    interceptor.ReaderExecuted(command, ctx, reader, sw.Elapsed);
                return gate != null || disposeCommandWithReader
                    ? new SerializedDbDataReader(reader, gate, disposeCommandWithReader ? command : null, disposeCommandWithReader)
                    : reader;
            }
            catch (Exception ex)
            {
                try
                {
                    if (disposeCommandWithReader)
                        command.Dispose();
                }
                finally
                {
                    gate?.Release();
                }
                sw.Stop();
                foreach (var interceptor in interceptors)
                    interceptor.CommandFailed(command, ctx, ex);
                throw;
            }
        }

        private static DbDataReader ExecuteReaderSerializedIfNeeded(DbCommand command, SemaphoreSlim? gate, CommandBehavior behavior, bool disposeCommandWithReader)
        {
            if (gate == null)
            {
                var reader = command.ExecuteReader(behavior);
                return disposeCommandWithReader
                    ? new SerializedDbDataReader(reader, null, command, holdGateUntilDispose: true)
                    : reader;
            }

            gate.Wait();
            try
            {
                return new SerializedDbDataReader(
                    command.ExecuteReader(behavior),
                    gate,
                    disposeCommandWithReader ? command : null,
                    disposeCommandWithReader);
            }
            catch
            {
                try
                {
                    if (disposeCommandWithReader)
                        command.Dispose();
                }
                finally
                {
                    gate.Release();
                }
                throw;
            }
        }

        private sealed class SerializedDbDataReader : DbDataReader
        {
            private readonly DbDataReader _inner;
            private readonly SemaphoreSlim? _gate;
            private readonly DbCommand? _commandToDispose;
            private readonly bool _holdGateUntilDispose;
            private int _released;

            internal SerializedDbDataReader(DbDataReader inner, SemaphoreSlim? gate, DbCommand? commandToDispose = null, bool holdGateUntilDispose = false)
            {
                _inner = inner;
                _gate = gate;
                _commandToDispose = commandToDispose;
                _holdGateUntilDispose = holdGateUntilDispose;
            }

            private void ReleaseGate()
            {
                if (Interlocked.Exchange(ref _released, 1) == 0)
                {
                    try
                    {
                        _commandToDispose?.Dispose();
                    }
                    finally
                    {
                        _gate?.Release();
                    }
                }
            }

            private void ReleaseGateAfterExhaustion()
            {
                if (!_holdGateUntilDispose)
                    ReleaseGate();
            }

            public override void Close()
            {
                try { _inner.Close(); }
                finally { ReleaseGate(); }
            }

            protected override void Dispose(bool disposing)
            {
                try
                {
                    if (disposing)
                        _inner.Dispose();
                }
                finally
                {
                    ReleaseGate();
                    base.Dispose(disposing);
                }
            }

            public override async ValueTask DisposeAsync()
            {
                try { await _inner.DisposeAsync().ConfigureAwait(false); }
                finally { ReleaseGate(); }
            }

            public override object this[int ordinal] => _inner[ordinal];
            public override object this[string name] => _inner[name];
            public override int Depth => _inner.Depth;
            public override int FieldCount => _inner.FieldCount;
            public override bool HasRows => _inner.HasRows;
            public override bool IsClosed => _inner.IsClosed;
            public override int RecordsAffected => _inner.RecordsAffected;
            public override int VisibleFieldCount => _inner.VisibleFieldCount;
            public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
            public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);
            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
                => _inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
            public override char GetChar(int ordinal) => _inner.GetChar(ordinal);
            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
                => _inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
            public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
            public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
            public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal);
            public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
            [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields | System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
            public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);
            public override T GetFieldValue<T>(int ordinal) => _inner.GetFieldValue<T>(ordinal);
            public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
                => _inner.GetFieldValueAsync<T>(ordinal, cancellationToken);
            public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
            public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);
            public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
            public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
            public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
            public override string GetName(int ordinal) => _inner.GetName(ordinal);
            public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
            public override DataTable? GetSchemaTable() => _inner.GetSchemaTable();
            public override string GetString(int ordinal) => _inner.GetString(ordinal);
            public override object GetValue(int ordinal) => _inner.GetValue(ordinal);
            public override int GetValues(object[] values) => _inner.GetValues(values);
            public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);
            public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
                => _inner.IsDBNullAsync(ordinal, cancellationToken);
            public override bool NextResult()
            {
                var hasNext = _inner.NextResult();
                if (!hasNext)
                    ReleaseGateAfterExhaustion();
                return hasNext;
            }

            public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
            {
                var hasNext = await _inner.NextResultAsync(cancellationToken).ConfigureAwait(false);
                if (!hasNext)
                    ReleaseGateAfterExhaustion();
                return hasNext;
            }

            public override bool Read()
            {
                var hasRow = _inner.Read();
                if (!hasRow)
                    ReleaseGateAfterExhaustion();
                return hasRow;
            }

            public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
            {
                var hasRow = await _inner.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (!hasRow)
                    ReleaseGateAfterExhaustion();
                return hasRow;
            }
            public override IEnumerator GetEnumerator() => _inner.GetEnumerator();
        }
    }
}
