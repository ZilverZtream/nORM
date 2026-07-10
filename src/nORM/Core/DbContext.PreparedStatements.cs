using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Creates a prepared INSERT statement for the specified entity type. This allows
        /// the cost of SQL generation and command preparation to be paid only once, with
        /// subsequent executions only updating parameter values. Ideal for batch insert
        /// scenarios where the same operation is repeated many times.
        /// </summary>
        /// <typeparam name="T">The entity type to prepare the insert for.</typeparam>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <param name="hydrateGeneratedKeys">
        /// When <c>false</c>, uses a plain <c>INSERT</c> shape and skips generated-key backfill.
        /// </param>
        /// <returns>A <see cref="PreparedOperation{T}"/> that can be executed multiple times.</returns>
        /// <remarks>
        /// Example usage:
        /// <code>
        /// await using var preparedInsert = await context.PrepareInsertAsync&lt;User&gt;();
        /// for (int i = 0; i &lt; users.Length; i++)
        /// {
        ///     await preparedInsert.ExecuteAsync(users[i]);
        /// }
        /// </code>
        /// </remarks>
        public async Task<PreparedOperation<T>> PrepareInsertAsync<T>(
            CancellationToken ct = default,
            bool hydrateGeneratedKeys = true) where T : class
        {
            ThrowIfDisposed();
            var mapping = GetMapping(typeof(T));
            if (CurrentTransaction == null && Transaction.Current != null)
            {
                await using var ambientScope = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
            }

            var transaction = CurrentTransaction;
            var preparedInsert = await CreatePreparedInsertCommandAsync(
                mapping, transaction, hydrateGeneratedKeys, ct).ConfigureAwait(false);
            return new PreparedOperation<T>(preparedInsert);
        }

        private async Task<PreparedInsertCommand> GetOrCreatePreparedInsertCommandAsync(
            TableMapping mapping,
            DbTransaction? transaction,
            bool hydrateGeneratedKeys,
            CancellationToken ct)
        {
            var key = (mapping.Type, hydrateGeneratedKeys);

            if (_preparedInsertCache.TryGetValue(key, out var cached))
            {
                if (ReferenceEquals(cached.BoundTransaction, transaction))
                    return cached;

                _preparedInsertCache.TryRemove(key, out _);
                await cached.DisposeAsync().ConfigureAwait(false);
            }

            var created = await CreatePreparedInsertCommandAsync(
                mapping, transaction, hydrateGeneratedKeys, ct).ConfigureAwait(false);

            _preparedInsertCache[key] = created;

            return created;
        }

        private async Task<PreparedInsertCommand> CreatePreparedInsertCommandAsync(
            TableMapping mapping,
            DbTransaction? transaction,
            bool hydrateGeneratedKeys,
            CancellationToken ct)
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var cmd = RawConnection.CreateCommand();
            try
            {
                if (transaction != null)
                    cmd.Transaction = transaction;

                var hydrateFromCommand = hydrateGeneratedKeys &&
                    _p.SupportsCommandGeneratedKeyRetrieval &&
                    HasDbGeneratedKey(mapping.KeyColumns);
                cmd.CommandText = _p.BuildInsert(mapping, hydrateGeneratedKeys && !hydrateFromCommand);
                cmd.CommandTimeout = ToSecondsClamped(Options.TimeoutConfiguration.BaseTimeout);

                foreach (var col in _p.GetInsertColumns(mapping))
                {
                    cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, null, GetParameterKnownType(col, null));
                    ApplyPreparedInsertSizeHint(cmd.Parameters[cmd.Parameters.Count - 1], col);
                }

                try
                {
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);
                }
                catch (NotSupportedException)
                {
                    // Some providers expose command reuse but not explicit preparation.
                }

                return new PreparedInsertCommand(cmd, mapping, this, hydrateGeneratedKeys, hydrateFromCommand, transaction);
            }
            catch
            {
                await cmd.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        private static void ApplyPreparedInsertSizeHint(DbParameter parameter, Column column)
        {
            if (parameter.Size != 0)
                return;

            var isSqlServerParameter = parameter.GetType().FullName?.Contains("SqlParameter", StringComparison.Ordinal) == true;
            var type = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
            if (type == typeof(string) || type == typeof(char))
            {
                if (isSqlServerParameter)
                    parameter.Size = ParameterOptimizer.MaxInlineStringSize;
            }
            else if (type == typeof(byte[]))
                parameter.Size = -1;
            else if (parameter.DbType == DbType.Object)
            {
                if (isSqlServerParameter)
                    parameter.Size = ParameterOptimizer.MaxInlineStringSize;
            }
            else if (isSqlServerParameter)
                parameter.Size = 1;
        }

        private List<PreparedInsertCommand> DrainPreparedInsertCache()
        {
            var commands = _preparedInsertCache.Values.ToList();
            _preparedInsertCache.Clear();
            return commands;
        }

        private void DisposePreparedInsertCache()
        {
            foreach (var command in DrainPreparedInsertCache())
                command.Dispose();
        }

        private async Task DisposePreparedInsertCacheAsync()
        {
            foreach (var command in DrainPreparedInsertCache())
                await command.DisposeAsync().ConfigureAwait(false);
        }

        private List<FastPathPreparedCommand> DrainFastPathPreparedCommandCache()
        {
            var commands = _fastPathPreparedCommandCache.Values.ToList();
            _fastPathPreparedCommandCache.Clear();
            return commands;
        }

        private void DisposeFastPathPreparedCommandCache()
        {
            foreach (var command in DrainFastPathPreparedCommandCache())
                command.Dispose();
        }

        private async Task DisposeFastPathPreparedCommandCacheAsync()
        {
            foreach (var command in DrainFastPathPreparedCommandCache())
                await command.DisposeAsync().ConfigureAwait(false);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal sealed class PreparedInsertCommand : IDisposable, IAsyncDisposable
        {
            private readonly DbCommand _command;
            private readonly TableMapping _mapping;
            private readonly DbContext _context;
            private readonly (DbParameter Parameter, Mapping.Column Column)[] _bindings;
            private readonly bool _hydrateGeneratedKeys;
            private readonly bool _hydrateGeneratedKeysFromCommand;
            private volatile bool _disposed;

            internal PreparedInsertCommand(
                DbCommand command,
                TableMapping mapping,
                DbContext context,
                bool hydrateGeneratedKeys,
                bool hydrateGeneratedKeysFromCommand,
                DbTransaction? boundTransaction)
            {
                _command = command ?? throw new ArgumentNullException(nameof(command));
                _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
                _context = context ?? throw new ArgumentNullException(nameof(context));
                BoundTransaction = boundTransaction;
                _hydrateGeneratedKeys = hydrateGeneratedKeys && HasDbGeneratedKey(_mapping.KeyColumns);
                _hydrateGeneratedKeysFromCommand = hydrateGeneratedKeysFromCommand && _hydrateGeneratedKeys;

                var insertCols = _context.RawProvider.GetInsertColumns(_mapping);
                _bindings = new (DbParameter, Mapping.Column)[insertCols.Length];
                var prefix = _context.RawProvider.ParamPrefix;

                for (int i = 0; i < insertCols.Length; i++)
                {
                    var col = insertCols[i];
                    var paramName = prefix + col.PropName;
                    if (!_command.Parameters.Contains(paramName))
                        throw new InvalidOperationException(
                            $"Prepared INSERT command is missing expected parameter '{paramName}' " +
                            $"for column '{col.EscCol}' on table '{mapping.TableName}'.");
                    _bindings[i] = (_command.Parameters[paramName], col);
                }
            }

            internal DbTransaction? BoundTransaction { get; }

            internal Task<int> ExecuteAsync(object entity, CancellationToken ct = default)
            {
                if (_disposed)
                    throw new ObjectDisposedException(nameof(PreparedInsertCommand));
                if (entity == null)
                    throw new ArgumentNullException(nameof(entity));

                var bindings = _bindings;
                for (int i = 0; i < bindings.Length; i++)
                {
                    var (param, col) = bindings[i];
                    var rawValue = col.Getter(entity);
                    var value = col.Converter != null ? col.Converter.ConvertToProvider(rawValue) : rawValue;
                    AssignPreparedValue(param, value);
                }

                if (_hydrateGeneratedKeys)
                {
                    return ExecuteWithHydrateAsync(entity, ct);
                }

                if (_context.RawProvider.PrefersSyncFastPathExecution
                    && _context.Options.CommandInterceptors.Count == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(_command.ExecuteNonQuery());
                }

                return _command.ExecuteNonQueryWithInterceptionAsync(_context, ct);
            }

            private static void AssignPreparedValue(DbParameter parameter, object? value)
            {
                if (value is null or DBNull)
                {
                    parameter.Value = DBNull.Value;
                    return;
                }

                var type = value.GetType();
                if (type.IsEnum)
                {
                    parameter.Value = Convert.ChangeType(value, Enum.GetUnderlyingType(type));
                    return;
                }

                if (value is string text)
                {
                    if (parameter.Size >= 0 && text.Length > parameter.Size)
                        parameter.Size = text.Length;
                    parameter.Value = text;
                    return;
                }

                if (value is Guid guid && parameter is Microsoft.Data.Sqlite.SqliteParameter)
                {
                    parameter.DbType = DbType.String;
                    parameter.Size = 36;
                    parameter.Value = guid.ToString("D");
                    return;
                }

                if (value is byte[] bytes)
                {
                    if (parameter.Size >= 0 && bytes.Length > parameter.Size)
                        parameter.Size = -1;
                    parameter.Value = bytes;
                    return;
                }

                parameter.Value = value switch
                {
                    TimeOnly time => time.ToTimeSpan(),
                    char ch => ch.ToString(),
                    _ => value
                };
            }

            private async Task<int> ExecuteWithHydrateAsync(object entity, CancellationToken ct)
            {
                if (_hydrateGeneratedKeysFromCommand)
                {
                    if (_context.RawProvider.PrefersSyncFastPathExecution
                        && _context.Options.CommandInterceptors.Count == 0)
                    {
                        ct.ThrowIfCancellationRequested();
                        var recordsAffectedSync = _command.ExecuteNonQuery();
                        var commandGeneratedIdSync = _context.RawProvider.GetCommandGeneratedKey(_command, _mapping);
                        if (commandGeneratedIdSync != null && commandGeneratedIdSync != DBNull.Value)
                            _mapping.SetPrimaryKey(entity, commandGeneratedIdSync);
                        return recordsAffectedSync;
                    }

                    var recordsAffected = await _command.ExecuteNonQueryWithInterceptionAsync(_context, ct).ConfigureAwait(false);
                    var commandGeneratedId = _context.RawProvider.GetCommandGeneratedKey(_command, _mapping);
                    if (commandGeneratedId != null && commandGeneratedId != DBNull.Value)
                        _mapping.SetPrimaryKey(entity, commandGeneratedId);
                    return recordsAffected;
                }

                if (_context.RawProvider.PrefersSyncFastPathExecution
                    && _context.Options.CommandInterceptors.Count == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var newIdSync = _command.ExecuteScalar();
                    if (newIdSync != null && newIdSync != DBNull.Value)
                        _mapping.SetPrimaryKey(entity, newIdSync);
                    return 1;
                }

                var newId = await _command.ExecuteScalarWithInterceptionAsync(_context, ct).ConfigureAwait(false);
                if (newId != null && newId != DBNull.Value)
                    _mapping.SetPrimaryKey(entity, newId);
                return 1;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;

                _command.Dispose();
                _disposed = true;
            }

            public async ValueTask DisposeAsync()
            {
                if (_disposed)
                    return;

                await _command.DisposeAsync().ConfigureAwait(false);
                _disposed = true;
            }
        }

        internal sealed class FastPathPreparedCommand : IDisposable, IAsyncDisposable
        {
            internal FastPathPreparedCommand(DbCommand command)
            {
                Command = command ?? throw new ArgumentNullException(nameof(command));
            }

            internal DbCommand Command { get; }
            internal SemaphoreSlim Gate { get; } = new(1, 1);

            public void Dispose()
            {
                Gate.Dispose();
                Command.Dispose();
            }

            public async ValueTask DisposeAsync()
            {
                Gate.Dispose();
                await Command.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Represents a prepared database operation that can be executed multiple times
    /// with different parameter values. The SQL and command structure are prepared
    /// once and reused, providing significant performance benefits for repeated
    /// operations like batch inserts.
    /// </summary>
    /// <typeparam name="T">The entity type this operation works with.</typeparam>
    public sealed class PreparedOperation<T> : IAsyncDisposable where T : class
    {
        private readonly DbContext.PreparedInsertCommand _command;

        /// <summary>
        /// Initializes a new instance of the <see cref="PreparedOperation{T}"/> class.
        /// </summary>
        /// <param name="command">The prepared database command.</param>
        internal PreparedOperation(DbContext.PreparedInsertCommand command)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
        }

        /// <summary>
        /// Executes the prepared operation for the specified entity. Parameter values
        /// are updated from the entity properties and the command is executed.
        /// </summary>
        /// <param name="entity">The entity to insert.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of rows affected.</returns>
        public Task<int> ExecuteAsync(T entity, CancellationToken ct = default)
            => _command.ExecuteAsync(entity, ct);

        /// <summary>
        /// Releases all resources used by this prepared operation.
        /// </summary>
        public ValueTask DisposeAsync()
            => _command.DisposeAsync();
    }
}
