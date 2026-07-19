using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using nORM.Versioning;
#nullable enable
namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Creates a new <see cref="DbConnection"/> using the provided connection
        /// string and database provider. If creation fails, the method ensures that
        /// any partially created connection is disposed and rethrows a sanitized
        /// <see cref="ArgumentException"/> that masks sensitive connection details.
        /// </summary>
        /// <param name="connectionString">Raw database connection string.</param>
        /// <param name="provider">The provider responsible for creating the connection.</param>
        /// <returns>A ready-to-use <see cref="DbConnection"/> instance.</returns>
        /// <exception cref="ArgumentException">Thrown when the connection string is invalid.</exception>
        private static DbConnection CreateConnectionSafe(string connectionString, DatabaseProvider provider)
        {
            DbConnection? connection = null;
            bool success = false;
            try
            {
                connection = DbConnectionFactory.Create(connectionString, provider);
                success = true;
                return connection;
            }
            catch (DbException ex)
            {
                // Database-specific errors (e.g., connection failures, invalid server names)
                var safeConnStr = NormValidator.MaskSensitiveConnectionStringData(connectionString);
                throw new ArgumentException($"Invalid connection string format: {safeConnStr}", nameof(connectionString), ex);
            }
            catch (ArgumentException ex)
            {
                // Argument validation errors (e.g., malformed connection string)
                var safeConnStr = NormValidator.MaskSensitiveConnectionStringData(connectionString);
                throw new ArgumentException($"Invalid connection string format: {safeConnStr}", nameof(connectionString), ex);
            }
            finally
            {
                // Always dispose connection on failure to prevent handle leaks,
                // including from TypeLoadException, FileNotFoundException, and similar exceptions.
                if (!success)
                {
                    connection?.Dispose();
                }
            }
        }
        // Cached completed task to avoid allocation on the hot path
        private Task<DbConnection>? _ensureConnectionCompletedTask;

        internal Task<DbConnection> EnsureConnectionAsync(CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested)
                return Task.FromCanceled<DbConnection>(ct);

            // Fast path - connection already open, provider initialized, and temporal init either
            // not needed or already complete. Returns a cached Task to avoid async state machine allocation.
            if (_cn.State == ConnectionState.Open && _providerInitialized &&
                NativeTenantSessionIsCurrent() &&
                (!Options.IsTemporalVersioningEnabled || _temporalInitComplete))
                return _ensureConnectionCompletedTask ??= Task.FromResult(_cn);
            return EnsureConnectionSlowAsync(ct);
        }

        private async Task<DbConnection> EnsureConnectionSlowAsync(CancellationToken ct)
        {
            if (_cn.State != ConnectionState.Open)
            {
                await _cn.OpenAsync(ct).ConfigureAwait(false);
                _nativeTenantSessionAppliedKey = null;
            }
            if (!_providerInitialized)
            {
                await _providerInitLock.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    if (!_providerInitialized)
                    {
                        var connectionGate = CommandInterceptorExtensions.GetSerializedConnectionGate(_cn, this);
                        if (connectionGate != null)
                            await connectionGate.WaitAsync(ct).ConfigureAwait(false);
                        try
                        {
                            await _p.InitializeConnectionAsync(_cn, ct).ConfigureAwait(false);
                        }
                        finally
                        {
                            connectionGate?.Release();
                        }
                        _providerInitialized = true;
                    }
                }
                finally
                {
                    _providerInitLock.Release();
                }
            }
            await ApplyNativeTenantSessionContextAsync(ct).ConfigureAwait(false);
            if (Options.IsTemporalVersioningEnabled && !_temporalInitComplete)
            {
                // A1 fix: double-checked lock so the caller's CancellationToken is forwarded to
                // TemporalManager.InitializeAsync. If already complete (fast-path race), skip.
                await _temporalInitLock.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    if (!_temporalInitComplete)
                    {
                        // Pass _cn directly to avoid re-entering EnsureConnectionAsync
                        // from within the bootstrap (which would deadlock on _temporalInitLock).
                        await TemporalManager.InitializeAsync(this, _cn, ct).ConfigureAwait(false);
                        _temporalInitComplete = true;
                    }
                }
                finally
                {
                    _temporalInitLock.Release();
                }
            }
            // Cache for future fast-path returns
            _ensureConnectionCompletedTask = Task.FromResult(_cn);
            return _cn;
        }
        internal DbConnection EnsureConnection()
        {
            if (_cn.State != ConnectionState.Open)
            {
                _cn.Open();
                _nativeTenantSessionAppliedKey = null;
            }
            if (!_providerInitialized)
            {
                // Use regular lock instead of SemaphoreSlim.Wait() to avoid deadlock
                // in synchronous contexts (ASP.NET, UI threads with sync context).
                lock (_providerInitSyncLock)
                {
                    if (!_providerInitialized)
                    {
                        var connectionGate = CommandInterceptorExtensions.GetSerializedConnectionGate(_cn, this);
                        connectionGate?.Wait();
                        try
                        {
                            _p.InitializeConnection(_cn);
                        }
                        finally
                        {
                            connectionGate?.Release();
                        }
                        _providerInitialized = true;
                    }
                }
            }
            // A1/X1: Parity with EnsureConnectionSlowAsync - run temporal bootstrap on
            // sync entry paths too.
            //
            // Sync-over-async (.GetAwaiter().GetResult()) is safe here because:
            // 1. All async code in TemporalManager.InitializeAsync uses ConfigureAwait(false),
            //    so there is no captured SynchronizationContext to deadlock against.
            // 2. .NET 8 console/server apps have no SynchronizationContext by default.
            // 3. ASP.NET Core uses a thread-pool SynchronizationContext that does not marshal
            //    back to a specific thread, so blocking is safe.
            ApplyNativeTenantSessionContext();
            if (Options.IsTemporalVersioningEnabled && !_temporalInitComplete)
            {
                _temporalInitLock.Wait();
                try
                {
                    if (!_temporalInitComplete)
                    {
                        TemporalManager.InitializeAsync(this, _cn, CancellationToken.None)
                            .GetAwaiter().GetResult();
                        _temporalInitComplete = true;
                    }
                }
                finally { _temporalInitLock.Release(); }
            }
            return _cn;
        }

        private bool NativeTenantSessionIsCurrent()
        {
            if (Options.NativeTenantSecurityMode == NativeTenantSecurityMode.Disabled)
                return true;

            if (_nativeTenantSessionAppliedKey == null || Options.TenantProvider == null)
                return false;

            object? tenantId;
            try
            {
                tenantId = Options.TenantProvider.GetCurrentTenantId();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                return false;
            }
            if (tenantId == null)
                return false;

            return string.Equals(
                _nativeTenantSessionAppliedKey,
                BuildNativeTenantSessionCacheKey(tenantId),
                StringComparison.Ordinal);
        }

        private async Task ApplyNativeTenantSessionContextAsync(CancellationToken ct)
        {
            if (Options.NativeTenantSecurityMode == NativeTenantSecurityMode.Disabled)
                return;

            if (Options.NativeTenantSecurityMode != NativeTenantSecurityMode.SessionContext)
                throw new NormUnsupportedFeatureException(
                    $"Native tenant security mode '{Options.NativeTenantSecurityMode}' is not supported.");

            if (!_p.SupportsNativeTenantSessionContext)
                throw new NormUnsupportedFeatureException(
                    $"{_p.GetType().Name} does not support provider-native tenant session context.");

            var tenantId = GetRequiredTenantId("native tenant session context");
            var cacheKey = BuildNativeTenantSessionCacheKey(tenantId);
            if (string.Equals(_nativeTenantSessionAppliedKey, cacheKey, StringComparison.Ordinal))
                return;

            await using var cmd = CreateCommand();
            var tenantParam = _p.ParamPrefix + "__nativeTenant";
            cmd.CommandText = _p.GetSetNativeTenantSessionContextSql(Options.NativeTenantSessionKey, tenantParam);
            cmd.AddOptimizedParam(tenantParam, tenantId, tenantId.GetType());
            await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
            _nativeTenantSessionAppliedKey = cacheKey;
            _ensureConnectionCompletedTask = null;
        }

        private void ApplyNativeTenantSessionContext()
        {
            if (Options.NativeTenantSecurityMode == NativeTenantSecurityMode.Disabled)
                return;

            if (Options.NativeTenantSecurityMode != NativeTenantSecurityMode.SessionContext)
                throw new NormUnsupportedFeatureException(
                    $"Native tenant security mode '{Options.NativeTenantSecurityMode}' is not supported.");

            if (!_p.SupportsNativeTenantSessionContext)
                throw new NormUnsupportedFeatureException(
                    $"{_p.GetType().Name} does not support provider-native tenant session context.");

            var tenantId = GetRequiredTenantId("native tenant session context");
            var cacheKey = BuildNativeTenantSessionCacheKey(tenantId);
            if (string.Equals(_nativeTenantSessionAppliedKey, cacheKey, StringComparison.Ordinal))
                return;

            using var cmd = CreateCommand();
            var tenantParam = _p.ParamPrefix + "__nativeTenant";
            cmd.CommandText = _p.GetSetNativeTenantSessionContextSql(Options.NativeTenantSessionKey, tenantParam);
            cmd.AddOptimizedParam(tenantParam, tenantId, tenantId.GetType());
            cmd.ExecuteScalarWithInterception(this);
            _nativeTenantSessionAppliedKey = cacheKey;
            _ensureConnectionCompletedTask = null;
        }

        private string BuildNativeTenantSessionCacheKey(object tenantId)
            => Options.NativeTenantSessionKey + "\u001f" + Convert.ToString(tenantId, CultureInfo.InvariantCulture);

        /// <summary>
        /// Computes an adaptive timeout for the given database operation. When a
        /// <see cref="nORM.Query.QueryComplexityMetrics"/> value is supplied (from the
        /// expression-tree translation pass) it is used directly and no SQL string
        /// scanning is performed.  The SQL string fallback is retained for raw-SQL
        /// code paths (stored procedures, navigation loaders, etc.) where no expression
        /// tree exists.
        /// </summary>
        internal TimeSpan GetAdaptiveTimeout(
            AdaptiveTimeoutManager.OperationType operationType,
            string? sql = null,
            int recordCount = 1,
            nORM.Query.QueryComplexityMetrics? treeMetrics = null)
        {
            int baseComplexity;

            // PREFERRED PATH: use expression-tree metrics computed during translation.
            // This is exact, cheap, and immune to false positives from quoted identifiers
            // or column names that happen to contain SQL keywords (e.g. group_by_column).
            if (treeMetrics.HasValue)
            {
                baseComplexity = treeMetrics.Value.ToComplexityScore();
            }
            else if (!string.IsNullOrEmpty(sql))
            {
                // FALLBACK PATH (raw SQL, stored procedures, navigation eager-loaders):
                // scan the SQL string as before.  This path is used when no expression
                // tree was walked (e.g. GetAdaptiveTimeout called with a raw SQL string).
                //
                // Skip detailed analysis for extremely large SQL (>100KB) to avoid severe
                // slowdown from scanning multi-megabyte strings with Contains/IndexOf.
                if (sql.Length > SqlComplexityAnalysisMaxLength)
                {
                    baseComplexity = LargeSqlBaselineComplexity + Math.Min(MaxComplexityScore - LargeSqlBaselineComplexity, sql.Length / LargeSqlLengthComplexityDivisor);
                    Options.Logger?.LogDebug(
                        "Skipping detailed complexity analysis for large SQL ({Length} chars). Using length-based estimate: {Complexity}",
                        sql.Length, baseComplexity);
                }
                else
                {
                    baseComplexity = 1 + (sql.Length / SqlLengthComplexityDivisor);

                    int joinCount = CountOccurrences(sql, "JOIN", StringComparison.OrdinalIgnoreCase);
                    if (joinCount > 0) baseComplexity += 2 * joinCount;

                    if (sql.Contains("UNION", StringComparison.OrdinalIgnoreCase)) baseComplexity += 3;
                    if (sql.Contains("INTERSECT", StringComparison.OrdinalIgnoreCase)) baseComplexity += 3;
                    if (sql.Contains("EXCEPT", StringComparison.OrdinalIgnoreCase)) baseComplexity += 3;

                    if (sql.Contains("GROUP BY", StringComparison.OrdinalIgnoreCase)) baseComplexity += 2;
                    if (sql.Contains("HAVING", StringComparison.OrdinalIgnoreCase)) baseComplexity += 1;
                    if (sql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase)) baseComplexity += 2;

                    int subqueryCount = CountOccurrences(sql, "SELECT", StringComparison.OrdinalIgnoreCase) - 1;
                    if (subqueryCount > 0) baseComplexity += 2 * subqueryCount;

                    if (sql.Contains("DISTINCT", StringComparison.OrdinalIgnoreCase)) baseComplexity += 1;
                    if (sql.Contains("CROSS JOIN", StringComparison.OrdinalIgnoreCase)) baseComplexity += 3;
                    if (sql.Contains("LEFT JOIN", StringComparison.OrdinalIgnoreCase) || sql.Contains("RIGHT JOIN", StringComparison.OrdinalIgnoreCase)) baseComplexity += 1;
                    if (sql.Contains("OUTER JOIN", StringComparison.OrdinalIgnoreCase)) baseComplexity += 1;

                    if (sql.Contains("OVER(", StringComparison.OrdinalIgnoreCase) || sql.Contains("OVER (", StringComparison.OrdinalIgnoreCase)) baseComplexity += 2;
                    if (sql.Contains("WITH", StringComparison.OrdinalIgnoreCase) && sql.Contains("AS(", StringComparison.OrdinalIgnoreCase)) baseComplexity += 2;
                }

                baseComplexity = Math.Min(baseComplexity, MaxComplexityScore);
            }
            else
            {
                // No metrics and no SQL: use baseline complexity of 1.
                baseComplexity = 1;
            }

            return _timeoutManager.GetTimeoutForOperation(operationType, recordCount, baseComplexity);
        }

        /// <summary>
        /// Overload used when a query plan with pre-computed complexity metrics is available.
        /// The expression-tree metrics are used directly; no SQL string scanning is performed.
        /// </summary>
        internal TimeSpan GetAdaptiveTimeout(
            AdaptiveTimeoutManager.OperationType operationType,
            nORM.Query.QueryComplexityMetrics treeMetrics,
            int recordCount = 1)
        {
            return GetAdaptiveTimeout(operationType, sql: null, recordCount, treeMetrics);
        }

        /// <summary>
        /// Counts the number of occurrences of a substring in a string.
        /// </summary>
        private static int CountOccurrences(string text, string pattern, StringComparison comparison = StringComparison.Ordinal)
        {
            if (string.IsNullOrEmpty(pattern)) return 0;

            int count = 0;
            int index = 0;

            while ((index = text.IndexOf(pattern, index, comparison)) != -1)
            {
                count++;
                index += pattern.Length;
            }

            return count;
        }

        /// <summary>
        /// Gets the raw <see cref="DbConnection"/> associated with this context.
        /// Consumers should avoid closing or disposing the connection as it is
        /// managed by the context.
        /// </summary>
        public DbConnection Connection
        {
            get
            {
                ThrowIfStrictProviderMobilityEscapeHatch(nameof(Connection));
                return _cn;
            }
        }

        internal DbConnection RawConnection => _cn;

        /// <summary>
        /// Gets the <see cref="DatabaseProvider"/> that supplies provider-specific
        /// behavior such as SQL generation and bulk operations.
        /// </summary>
        public DatabaseProvider Provider
        {
            get
            {
                ThrowIfStrictProviderMobilityEscapeHatch(nameof(Provider));
                return _p;
            }
        }

        internal DatabaseProvider RawProvider => _p;

        internal bool IsStrictProviderMobility
            => _strictProviderMobility || Options.ProviderMobilityMode == ProviderMobilityMode.Strict;

        // Safe atomic accessors using Interlocked (avoids CS0420 warnings)
        internal DbTransaction? CurrentTransaction
        {
            get => Interlocked.CompareExchange(ref _currentTransaction, null, null);
            set
            {
                Interlocked.Exchange(ref _currentTransaction, value);
                if (value == null)
                    Volatile.Write(ref _currentContextTransaction, null);
            }
        }

        internal DbContextTransaction? CurrentContextTransaction
            => Volatile.Read(ref _currentContextTransaction);

        internal void SetCurrentTransaction(DbTransaction transaction, DbContextTransaction contextTransaction)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            ArgumentNullException.ThrowIfNull(contextTransaction);

            Interlocked.Exchange(ref _currentTransaction, transaction);
            Volatile.Write(ref _currentContextTransaction, contextTransaction);
            // Snapshot Added-entity keys so a full rollback of this caller-owned transaction can reset
            // any key stamped while it was active (see DbContext.Transactions.cs).
            CaptureTransactionKeySnapshot();
        }

        internal void ClearTransaction(DbTransaction transaction)
        {
            if (ReferenceEquals(Interlocked.CompareExchange(ref _currentTransaction, null, null), transaction))
            {
                Interlocked.Exchange(ref _currentTransaction, null);
                Volatile.Write(ref _currentContextTransaction, null);
                // Key snapshots are scoped to the transaction that owned them.
                _savepointKeySnapshots?.Clear();
                _transactionKeySnapshot = null;
            }
        }

        /// <summary>
        /// Returns a cached NormQueryProvider for this context, avoiding 4 heap allocations per query.
        /// </summary>
        internal Query.NormQueryProvider GetQueryProvider()
        {
            return _cachedQueryProvider ??= new Query.NormQueryProvider(this);
        }

        /// <summary>
        /// Creates a <see cref="DbCommand"/> for the current connection and automatically
        /// binds the active transaction so that every command participates in the ongoing
        /// unit-of-work. Prefer this over <c>RawConnection.CreateCommand()</c> inside nORM
        /// code so that transaction binding is never accidentally omitted.
        /// </summary>
        internal DbCommand CreateCommand()
        {
            var cmd = RawConnection.CreateCommand();
            var tx = CurrentTransaction;
            if (tx != null)
                cmd.Transaction = tx;
            return cmd;
        }

        internal FastPathPreparedCommand GetOrCreateFastPathPreparedCommand(
            string cacheKey,
            string sql,
            int commandTimeout,
            Action<DbCommand> initializeParameters)
        {
            return _fastPathPreparedCommandCache.GetOrAdd(cacheKey, static (_, state) =>
            {
                var cmd = state.Context.CreateCommand();
                try
                {
                    cmd.CommandText = state.Sql;
                    cmd.CommandTimeout = state.CommandTimeout;
                    state.InitializeParameters(cmd);
                    try
                    {
                        cmd.Prepare();
                    }
                    catch (NotSupportedException)
                    {
                    }
                    catch (InvalidOperationException)
                    {
                    }
                    catch (DbException)
                    {
                    }

                    return new FastPathPreparedCommand(cmd);
                }
                catch
                {
                    cmd.Dispose();
                    throw;
                }
            }, (Context: this, Sql: sql, CommandTimeout: commandTimeout, InitializeParameters: initializeParameters));
        }

        /// <summary>
        /// Overload that threads caller <paramref name="parameterState"/> to a STATIC
        /// <paramref name="initializeParameters"/> so the hot path allocates no per-call closure. The
        /// initializer runs once, on the cache miss, before <c>Prepare()</c>.
        /// </summary>
        internal FastPathPreparedCommand GetOrCreateFastPathPreparedCommand<TState>(
            string cacheKey,
            string sql,
            int commandTimeout,
            TState parameterState,
            Action<DbCommand, TState> initializeParameters)
        {
            return _fastPathPreparedCommandCache.GetOrAdd(cacheKey, static (_, s) =>
            {
                var cmd = s.Context.CreateCommand();
                try
                {
                    cmd.CommandText = s.Sql;
                    cmd.CommandTimeout = s.CommandTimeout;
                    s.InitializeParameters(cmd, s.ParameterState);
                    try
                    {
                        cmd.Prepare();
                    }
                    catch (NotSupportedException)
                    {
                    }
                    catch (InvalidOperationException)
                    {
                    }
                    catch (DbException)
                    {
                    }

                    return new FastPathPreparedCommand(cmd);
                }
                catch
                {
                    cmd.Dispose();
                    throw;
                }
            }, (Context: this, Sql: sql, CommandTimeout: commandTimeout, ParameterState: parameterState, InitializeParameters: initializeParameters));
        }

        /// <summary>
        /// X1: Creates a <see cref="DbCommand"/> that is fully lifecycle-aware:
        /// opens the connection if closed, binds the active transaction, and participates in
        /// command interception. Generated <c>[CompileTimeQuery]</c> methods must use this
        /// instead of <c>ctx.RawConnection.CreateCommand()</c> to avoid bypassing connection
        /// initialisation, transaction scope, and interceptor pipelines.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A ready-to-use <see cref="DbCommand"/> bound to the current context.</returns>
        public async Task<DbCommand> CreateCompiledQueryCommandAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(CreateCompiledQueryCommandAsync));
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            return CreateCommand();
        }

        /// <summary>
        /// X1: Executes a pre-built command through the interception pipeline and materializes
        /// all rows into a <see cref="List{T}"/>. Generated <c>[CompileTimeQuery]</c> methods
        /// must use this instead of calling <c>cmd.ExecuteReaderAsync</c> directly so that
        /// command interceptors (logging, tracing, auditing) are invoked.
        /// </summary>
        /// <typeparam name="T">Entity type returned by each row.</typeparam>
        /// <param name="cmd">The prepared command with <c>CommandText</c> and parameters already set.</param>
        /// <param name="materializer">Delegate that converts one reader row into an entity.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A list of materialized entities.</returns>
        public async Task<List<T>> ExecuteCompiledQueryListAsync<T>(
            DbCommand cmd,
            Func<DbDataReader, CancellationToken, Task<T>> materializer,
            CancellationToken ct = default)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteCompiledQueryListAsync));
            var list = new List<T>();
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                list.Add(await materializer(reader, ct).ConfigureAwait(false));
            return list;
        }

        /// <summary>
        /// SG1/SG2: Returns the correct materializer for <typeparamref name="T"/> for use in
        /// [CompileTimeQuery]-generated methods. When the entity is registered in this context,
        /// delegates to <see cref="Query.MaterializerFactory"/> which applies the same eligibility
        /// guards as the regular LINQ path (fluent renames, value converters, owned navigations),
        /// falling back to the runtime reflection materializer when any unsafe condition is present.
        /// When the entity is not registered, uses the compiled materializer directly (no runtime
        /// config can override it). Throws a descriptive exception if no materializer exists at all.
        /// </summary>
        public Func<System.Data.Common.DbDataReader, CancellationToken, Task<T>> GetCompiledQueryMaterializer<T>(string tableName)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(GetCompiledQueryMaterializer));
            if (IsMapped(typeof(T)))
            {
                // MaterializerFactory.CreateMaterializer<T> applies all guards internally:
                // it uses the compiled materializer only when safe (no fluent renames, no converters,
                // no owned navigations), and falls back to a runtime materializer otherwise.
                var mapping = GetMapping(typeof(T));
                return _compiledQueryMaterializerFactory.CreateMaterializer<T>(mapping);
            }
            // Entity not registered in this context - no runtime fluent config can override the
            // compiled materializer, so use it directly without guards.
            if (SourceGeneration.CompiledMaterializerStore.TryGet(typeof(T), tableName, out var compiledUntyped))
            {
                return async (reader, ct) => (T)(await compiledUntyped(reader, ct).ConfigureAwait(false));
            }
            throw new InvalidOperationException(
                $"No materializer found for {typeof(T).Name} (table '{tableName}'). " +
                "Apply [GenerateMaterializer] to the entity type, or register it with the DbContext.");
        }

        // Singleton MaterializerFactory for compiled-query materialization. All state is static,
        // so a single instance per DbContext is sufficient.
        private static readonly Query.MaterializerFactory _compiledQueryMaterializerFactory = new();

        /// <summary>
        /// Checks whether the database connection is healthy by executing a lightweight query.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if the connection is healthy; otherwise <c>false</c>.</returns>
        public async Task<bool> IsHealthyAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            try
            {
                return await _executionStrategy.ExecuteAsync(async (ctx, token) =>
                {
                    await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                    await using var cmd = CommandPool.Get(ctx.RawConnection, "SELECT 1");
                    cmd.CommandTimeout = ToSecondsClamped(TimeSpan.FromSeconds(5));
                    var result = await cmd.ExecuteScalarWithInterceptionAsync(ctx, token).ConfigureAwait(false);
                    return result is 1 or 1L;
                }, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Rethrow cancellation to respect cancellation tokens; swallowing it
                // breaks proper async cancellation patterns.
                throw;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                // Log exceptions instead of silently swallowing them; silent failures
                // make debugging connection issues nearly impossible.
                // Fatal exceptions (OutOfMemoryException, StackOverflowException) are
                // excluded so they propagate and terminate the process as intended.
                Options.Logger?.LogWarning(ex, "Health check failed: {Message}", ex.Message);
                return false;
            }
        }
    }
}
