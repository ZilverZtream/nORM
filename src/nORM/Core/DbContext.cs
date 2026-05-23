using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Linq;
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
    /// <summary>
    /// Represents the primary entry point for interacting with a database using nORM.
    /// The <see cref="DbContext"/> manages the underlying <see cref="DbConnection"/>,
    /// maintains metadata such as <see cref="TableMapping"/> instances and coordinates
    /// change tracking, transactions and provider specific behavior. Instances are
    /// intended to be short lived and not thread safe.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("DbContext uses Expression-based query translation and reflection-built materializers; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("DbContext reflects over entity types to build mappings; trimming may remove the required members.")]
    public partial class DbContext : IDisposable, IAsyncDisposable
    {
        /// <summary>Maximum SQL length (in chars) before falling back to length-based complexity estimation.</summary>
        private const int SqlComplexityAnalysisMaxLength = 102_400; // 100 KB
        /// <summary>Divisor for base complexity from SQL string length in the fallback path.</summary>
        private const int SqlLengthComplexityDivisor = 100;
        /// <summary>Divisor for length-based complexity when SQL exceeds <see cref="SqlComplexityAnalysisMaxLength"/>.</summary>
        private const int LargeSqlLengthComplexityDivisor = 10_000;
        /// <summary>Baseline complexity floor for extremely large SQL strings (>100 KB).</summary>
        private const int LargeSqlBaselineComplexity = 20;
        /// <summary>Maximum complexity score cap to prevent timeout inflation from false positives.</summary>
        private const int MaxComplexityScore = 50;
        /// <summary>SQL Server error number indicating a deadlock victim (used by <see cref="UseDeadlockResilientSaveChanges"/>).</summary>
        private const int SqlServerDeadlockErrorNumber = 1205;
        /// <summary>Number of parameters reserved for internal use (e.g. tenant, timestamp) when computing batch sizes.</summary>
        private const int ParameterBudgetReserve = 10;
        /// <summary>Jitter range (�) applied to retry backoff delays to prevent thundering herd.</summary>
        private const double RetryJitterRange = 0.2;
        /// <summary>Maximum seconds to wait for the cleanup timer to drain during dispose.</summary>
        private const int CleanupTimerDrainTimeoutSeconds = 5;

        private readonly DbConnection _cn;
        // When false, Dispose/DisposeAsync must NOT close or dispose the connection
        // because it was passed in by the caller who retains ownership.
        private readonly bool _ownsConnection;
        private readonly DatabaseProvider _p;
        private readonly ConcurrentDictionary<Type, TableMapping> _m = new();
        /// <summary>Per-context fast-path SQL template cache. Keyed by entity type; stores provider+model-specific SELECT templates.</summary>
        internal readonly ConcurrentDictionary<Type, string> FastPathSqlCache = new();
        /// <summary>Pre-compiled regex for identifier validation. Matches word characters and spaces only.</summary>
        private static readonly System.Text.RegularExpressions.Regex s_safeIdentifierRegex =
            new(@"^[\w\s]+$", System.Text.RegularExpressions.RegexOptions.Compiled);
        /// <summary>
        /// SP1: Strict output-parameter name validator. Allows only letters, digits, and
        /// underscores (no spaces, no dots). Spaces/dots accepted by IsSafeIdentifier would
        /// produce invalid ADO.NET parameter names when prefixed with the provider's ParamPrefix.
        /// </summary>
        private static readonly System.Text.RegularExpressions.Regex s_safeOutputParamNameRegex =
            new(@"^[A-Za-z_][A-Za-z0-9_]*$", System.Text.RegularExpressions.RegexOptions.Compiled);

        private static bool IsSafeOutputParamName(string name)
            => !string.IsNullOrEmpty(name) && s_safeOutputParamNameRegex.IsMatch(name);
        /// <summary>Cached PropertyInfo for DbException.Number (provider-specific); avoids repeated reflection.</summary>
        private static readonly ConcurrentDictionary<Type, PropertyInfo?> s_numberPropertyCache = new();
        /// <summary>Cached MethodInfo for NormQueryable.Query&lt;T&gt;() generic method definition; avoids repeated reflection.</summary>
        private static readonly Lazy<MethodInfo> s_queryMethodInfo = new(() =>
            typeof(NormQueryable).GetMethods()
                .SingleOrDefault(m => m.Name == nameof(NormQueryable.Query) && m.IsGenericMethodDefinition)
                ?? throw new InvalidOperationException(
                    $"Could not find generic method '{nameof(NormQueryable.Query)}' on type '{nameof(NormQueryable)}'. " +
                    "The NormQueryable API surface may have changed."));
        /// <summary>Cached MethodInfo for NavigationPropertyExtensions.EnableLazyLoading&lt;T&gt;(); avoids per-call GetMethod reflection.</summary>
        private static readonly MethodInfo s_enableLazyLoadingMethod =
            typeof(NavigationPropertyExtensions).GetMethod(nameof(NavigationPropertyExtensions.EnableLazyLoading))!;
        /// <summary>
        /// Cached NormQueryProvider for this context. Avoids creating NormQueryProvider,
        /// IncludeProcessor, QueryExecutor, and BulkCudBuilder (4 heap allocations) on every Query&lt;T&gt;() call.
        /// </summary>
        private Query.NormQueryProvider? _cachedQueryProvider;
        /// <summary>Cached mapping hash for plan cache fingerprinting. Computed lazily once per context.
        /// Volatile to ensure visibility across threads when read after _mappingHashComputed is true.</summary>
        private volatile int _mappingHashCached;
        private volatile bool _mappingHashComputed;
        private readonly IExecutionStrategy _executionStrategy;
        private readonly AdaptiveTimeoutManager _timeoutManager;
        private readonly ModelBuilder _modelBuilder;
        private readonly DynamicEntityTypeGenerator _typeGenerator = new();
        // Static cache shared across all DbContext instances. Dynamic type generation is expensive
        // (uses Reflection.Emit), so sharing avoids recreating types per context.
        // Bounded LRU cache prevents unbounded memory growth as new dynamic types are generated.
        private static readonly ConcurrentLruCache<string, Lazy<Task<Type>>> _dynamicTypeCache = new(maxSize: 1000);
        private readonly LinkedList<WeakReference<IDisposable>> _disposables = new();
        private readonly object _disposablesLock = new();
        private readonly Timer _cleanupTimer;
        private bool _providerInitialized;
        private readonly SemaphoreSlim _providerInitLock = new(1, 1);
        private readonly object _providerInitSyncLock = new object(); // For synchronous initialization to avoid deadlock
        // A1 fix: replaced Lazy<Task> with an explicit double-checked lock so that the
        // CancellationToken passed to EnsureConnectionAsync is forwarded to TemporalManager.
        // Lazy<Task> fixes the factory at creation time with no way to accept a later ct.
        private readonly SemaphoreSlim _temporalInitLock = new(1, 1);
        private volatile bool _temporalInitComplete;
        private DbTransaction? _currentTransaction; // Access via Interlocked.* only
        // ConcurrentDictionary eliminates lock contention on the insert fast path.
        private readonly ConcurrentDictionary<(Type EntityType, bool HydrateGeneratedKeys), PreparedInsertCommand> _preparedInsertCache = new();
        private readonly ConcurrentDictionary<string, FastPathPreparedCommand> _fastPathPreparedCommandCache = new();
        private volatile bool _disposed;

        /// <summary>
        /// Gets the configuration options that control the behavior of this context
        /// instance including logging, retry policies and mapping conventions.
        /// </summary>
        public DbContextOptions Options { get; }

        /// <summary>
        /// Provides access to the tracking graph responsible for detecting changes
        /// on entities and orchestrating persistence operations.
        /// </summary>
        public ChangeTracker ChangeTracker { get; }

        /// <summary>
        /// Exposes database specific operations such as transaction management and
        /// command execution helpers.
        /// </summary>
        public DatabaseFacade Database { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbContext"/> class using an
        /// existing <see cref="DbConnection"/>. The context takes ownership of the
        /// connection and will not dispose it until the context itself is disposed.
        /// </summary>
        /// <param name="cn">The open or closed database connection.</param>
        /// <param name="p">The <see cref="DatabaseProvider"/> responsible for generating provider specific SQL.</param>
        /// <param name="options">Optional configuration controlling context behavior.</param>
        public DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options = null)
            : this(cn, p, options, ownsConnection: true) { }

        // Internal constructor that allows callers (e.g. migration runners) to pass an
        // externally-owned connection. When ownsConnection is false, Dispose/DisposeAsync will NOT
        // close or dispose the connection � the caller retains full ownership.
        internal DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options, bool ownsConnection)
        {
            _cn = cn ?? throw new ArgumentNullException(nameof(cn));
            _ownsConnection = ownsConnection;
            _p = p ?? throw new ArgumentNullException(nameof(p));
            // IMPORTANT: Options is treated as effectively immutable after construction.
            // Mutating Options properties after the context is created leads to undefined behavior
            // because cached plans, prepared commands, and internal state depend on the initial values.
            Options = options ?? new DbContextOptions();
            Options.Validate();
            // Only validate TenantColumnName when multi-tenancy is actually configured.
            // Without a TenantProvider, the column name is unused and may safely be null.
            if (Options.TenantProvider != null && string.IsNullOrWhiteSpace(Options.TenantColumnName))
                throw new ArgumentException("TenantColumnName cannot be null or empty when TenantProvider is configured.");
            if (Options.CacheExpiration <= TimeSpan.Zero)
                throw new ArgumentException("CacheExpiration must be positive.");
            // Avoid LINQ .Any() allocation on the constructor hot path; use index-based loop.
            for (int i = 0; i < Options.CommandInterceptors.Count; i++)
            {
                if (Options.CommandInterceptors[i] == null)
                    throw new ArgumentException("CommandInterceptors cannot contain null entries.");
            }
            for (int i = 0; i < Options.SaveChangesInterceptors.Count; i++)
            {
                if (Options.SaveChangesInterceptors[i] == null)
                    throw new ArgumentException("SaveChangesInterceptors cannot contain null entries.");
            }
            ChangeTracker = new ChangeTracker(Options);
            _modelBuilder = new ModelBuilder();
            Options.OnModelCreating?.Invoke(_modelBuilder);
            Database = new DatabaseFacade(this);
            _executionStrategy = Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(this, Options.RetryPolicy)
                : new DefaultExecutionStrategy(this);
            _timeoutManager = new AdaptiveTimeoutManager(Options.TimeoutConfiguration,
                Options.Logger ?? NullLogger.Instance);
            // _temporalInitLock initialized via field declarations above;
            // initialization happens lazily inside EnsureConnectionSlowAsync when ct is available.
            _cleanupTimer = new Timer(_ => CleanupDisposables(), null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }
        /// <summary>
        /// Initializes a new <see cref="DbContext"/> by creating and owning a
        /// <see cref="DbConnection"/> using the supplied connection string and
        /// provider. This overload is convenient for scenarios where a connection
        /// has not yet been instantiated.
        /// </summary>
        /// <param name="connectionString">The database connection string.</param>
        /// <param name="p">The provider used to create the connection.</param>
        /// <param name="options">Optional configuration for the context.</param>
        public DbContext(string connectionString, DatabaseProvider p, DbContextOptions? options = null)
            : this(CreateConnectionSafe(connectionString, p), p, options)
        {
        }
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

            // Fast path � connection already open, provider initialized, and temporal init either
            // not needed or already complete. Returns a cached Task to avoid async state machine allocation.
            if (_cn.State == ConnectionState.Open && _providerInitialized &&
                (!Options.IsTemporalVersioningEnabled || _temporalInitComplete))
                return _ensureConnectionCompletedTask ??= Task.FromResult(_cn);
            return EnsureConnectionSlowAsync(ct);
        }

        private async Task<DbConnection> EnsureConnectionSlowAsync(CancellationToken ct)
        {
            if (_cn.State != ConnectionState.Open)
                await _cn.OpenAsync(ct).ConfigureAwait(false);
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
                _cn.Open();
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
            // A1/X1: Parity with EnsureConnectionSlowAsync � run temporal bootstrap on
            // sync entry paths too.
            //
            // Sync-over-async (.GetAwaiter().GetResult()) is safe here because:
            // 1. All async code in TemporalManager.InitializeAsync uses ConfigureAwait(false),
            //    so there is no captured SynchronizationContext to deadlock against.
            // 2. .NET 8 console/server apps have no SynchronizationContext by default.
            // 3. ASP.NET Core uses a thread-pool SynchronizationContext that does not marshal
            //    back to a specific thread, so blocking is safe.
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
        public DbConnection Connection => _cn;

        /// <summary>
        /// Gets the <see cref="DatabaseProvider"/> that supplies provider-specific
        /// behavior such as SQL generation and bulk operations.
        /// </summary>
        public DatabaseProvider Provider => _p;

        // Safe atomic accessors using Interlocked (avoids CS0420 warnings)
        internal DbTransaction? CurrentTransaction
        {
            get => Interlocked.CompareExchange(ref _currentTransaction, null, null);
            set => Interlocked.Exchange(ref _currentTransaction, value);
        }
        internal void ClearTransaction(DbTransaction transaction)
        {
            if (ReferenceEquals(Interlocked.CompareExchange(ref _currentTransaction, null, null), transaction))
                Interlocked.Exchange(ref _currentTransaction, null);
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
        /// unit-of-work. Prefer this over <c>Connection.CreateCommand()</c> inside nORM
        /// code so that transaction binding is never accidentally omitted.
        /// </summary>
        internal DbCommand CreateCommand()
        {
            var cmd = Connection.CreateCommand();
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
        /// X1: Creates a <see cref="DbCommand"/> that is fully lifecycle-aware:
        /// opens the connection if closed, binds the active transaction, and participates in
        /// command interception. Generated <c>[CompileTimeQuery]</c> methods must use this
        /// instead of <c>ctx.Connection.CreateCommand()</c> to avoid bypassing connection
        /// initialisation, transaction scope, and interceptor pipelines.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A ready-to-use <see cref="DbCommand"/> bound to the current context.</returns>
        public async Task<DbCommand> CreateCompiledQueryCommandAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
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
            if (IsMapped(typeof(T)))
            {
                // MaterializerFactory.CreateMaterializer<T> applies all guards internally:
                // it uses the compiled materializer only when safe (no fluent renames, no converters,
                // no owned navigations), and falls back to a runtime materializer otherwise.
                var mapping = GetMapping(typeof(T));
                return _compiledQueryMaterializerFactory.CreateMaterializer<T>(mapping);
            }
            // Entity not registered in this context � no runtime fluent config can override the
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
                    await using var cmd = CommandPool.Get(ctx.Connection, "SELECT 1");
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

        /// <summary>
        /// Retrieves or creates a <see cref="TableMapping"/> for the specified entity
        /// type. Mappings are cached for reuse to avoid repeated reflection.
        /// </summary>
        /// <param name="t">The entity CLR type to map.</param>
        /// <returns>The mapping associated with the given type.</returns>
        internal TableMapping GetMapping(Type t) => _m.GetOrAdd(t, static (k, args) =>
            new TableMapping(k, args.p, args.ctx, args.modelBuilder.GetConfiguration(k)), (p: _p, ctx: this, modelBuilder: _modelBuilder));

        /// <summary>
        /// Enumerates mappings for all entity types that were configured via the
        /// <see cref="ModelBuilder"/>. Useful for scenarios that need to inspect
        /// or pre-generate metadata for every known entity.
        /// </summary>
        /// <returns>An enumerable sequence of <see cref="TableMapping"/> objects.</returns>
        internal IEnumerable<TableMapping> GetAllMappings()
        {
            foreach (var type in _modelBuilder.GetConfiguredEntityTypes())
                yield return GetMapping(type);
        }

        /// <summary>
        /// Returns a stable hash of all entity type ? table name mappings.
        /// Computed once and cached for the lifetime of the context.
        /// </summary>
        internal int GetMappingHash()
        {
            if (_mappingHashComputed) return _mappingHashCached;
            int h = 0;
            foreach (var mapping in GetAllMappings())
            {
                // Q1/C1/Cache1 fix: include full mapping shape � not just Type+TableName.
                // Column names, converter fingerprint, shadow fingerprint, tenant column,
                // and timestamp column all affect SQL generation and materialization.
                // Without these, divergent fluent configurations sharing the same CLR type
                // and table name can poison the global static plan/fast-path caches.
                h = HashCode.Combine(h, mapping.TableName, mapping.Type);
                h = HashCode.Combine(h, mapping.ConverterFingerprint, mapping.ShadowFingerprint);
                h = HashCode.Combine(h, mapping.TenantColumn?.Name, mapping.TimestampColumn?.Name);
                foreach (var col in mapping.Columns)
                    h = HashCode.Combine(h, col.Name, col.PropName);
            }
            _mappingHashCached = h;
            _mappingHashComputed = true;
            return h;
        }

        /// <summary>
        /// Tracks types that were used as query roots via ctx.Query&lt;T&gt;().
        /// These are "real" entity types, as opposed to DTO projection result types.
        /// Thread-safe via ConcurrentDictionary (used as a set).
        /// </summary>
        private readonly ConcurrentDictionary<Type, byte> _entityQueryRoots = new();

        /// <summary>
        /// Registers a type as a query-root entity (called by Query&lt;T&gt; extension).
        /// Ensures that directly-queried entities are considered "mapped" by IsMapped.
        /// </summary>
        internal void RegisterEntityType(Type t) => _entityQueryRoots.TryAdd(t, 0);

        /// <summary>
        /// Returns true when the type is a known entity root � either explicitly
        /// registered with the ModelBuilder OR used as a direct query root via Query&lt;T&gt;.
        /// DTO projection results (from .Select(x => new Dto {...})) are never query roots
        /// and will NOT be tracked, preventing ChangeTracker pollution.
        /// </summary>
        internal bool IsMapped(Type t) =>
            _modelBuilder.GetConfiguredEntityTypes().Contains(t) ||
            _entityQueryRoots.ContainsKey(t);

        /// <summary>
        /// Validates that an identifier is safe for use in SQL queries by stripping optional
        /// delimiters and checking the inner content against a strict allowlist. Prevents SQL
        /// injection via crafted identifiers like "[foo]; DROP TABLE Users--". Allows:
        /// - Alphanumeric characters, underscores, dots, spaces (word chars only)
        /// - Quoted identifiers: "name", `name`, [name] with safe inner content
        /// - Schema-qualified identifiers: dbo.Table, [dbo].[Table], "schema"."table"
        /// </summary>
        internal static bool IsSafeIdentifier(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;

            var trimmed = name.Trim();

            // Fast reject: statement-break characters are never valid in identifiers
            if (trimmed.Contains(';') || trimmed.Contains("--") || trimmed.Contains("/*") ||
                trimmed.Contains("*/") || trimmed.Contains('\''))
                return false;

            // Split on '.' to handle schema-qualified names like dbo.Table or [dbo].[Table]
            // Each part is validated independently.
            var parts = trimmed.Split('.');
            foreach (var part in parts)
            {
                if (!IsSafeIdentifierPart(part.Trim()))
                    return false;
            }
            return true;
        }

        private static readonly HashSet<string> _sensitiveConnStringKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            "password", "pwd", "user password", "access token", "accesstoken", "token", "secret"
        };

        /// <summary>
        /// Normalizes a connection string for use as a static cache key.
        /// Uses DbConnectionStringBuilder for robust parsing (handles quoted semicolons, escaped
        /// chars, reordered keys). Strips sensitive keys so credentials never appear in cache key
        /// memory (coredump / diagnostic exposure risk). Keys are sorted case-insensitively so
        /// identical strings with different orderings map to the same cache key.
        /// </summary>
        private static string NormalizeConnectionString(string? cs)
        {
            if (string.IsNullOrEmpty(cs)) return string.Empty;
            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = cs };
                return string.Join(";",
                    builder.Keys.Cast<string>()
                        .Where(k => !_sensitiveConnStringKeys.Contains(k))
                        .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                        .Select(k => $"{k}={builder[k]}"));
            }
            catch (ArgumentException)
            {
                // Malformed connection string: fall back to opaque hash so we never
                // return credentials in the key.
                return Convert.ToHexString(
                    System.Security.Cryptography.SHA256.HashData(
                        System.Text.Encoding.UTF8.GetBytes(cs)));
            }
        }

        /// <summary>
        /// Validates a single identifier part (possibly delimited). A part is either:
        /// - A plain word (letters, digits, underscore, space)
        /// - A delimited identifier: [inner], "inner", or `inner` where inner contains only word chars
        /// </summary>
        private static bool IsSafeIdentifierPart(string part)
        {
            if (part.Length == 0)
                return false;

            string inner;

            // Strip single pair of delimiters
            if ((part.StartsWith("[") && part.EndsWith("]")) ||
                (part.StartsWith("\"") && part.EndsWith("\"")) ||
                (part.StartsWith("`") && part.EndsWith("`")))
            {
                if (part.Length < 3)
                    return false; // empty delimiter pair
                inner = part[1..^1];
            }
            else
            {
                inner = part;
            }

            if (inner.Length == 0)
                return false;

            // Inner content must be strictly word characters and spaces only
            // (no brackets, quotes, semicolons, hyphens, or other special chars).
            // Uses pre-compiled static regex to avoid per-call regex compilation overhead.
            return s_safeIdentifierRegex.IsMatch(inner);
        }

        /// <summary>
        /// Creates an untyped <see cref="IQueryable"/> for the specified table name.
        /// This API is useful when working with tables or views that do not have a
        /// corresponding CLR type at compile time. A dynamic entity type is generated
        /// on-demand and cached so subsequent calls incur minimal overhead.
        /// </summary>
        /// <param name="tableName">Name of the table to query.</param>
        /// <returns>An <see cref="IQueryable"/> that can be composed with LINQ operators.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or empty.</exception>
        /// <exception cref="NormUsageException">Thrown when the provided name contains invalid characters.</exception>
        [RequiresDynamicCode("Query(string) generates a CLR entity type at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
        [RequiresUnreferencedCode("Query(string) reflects database schema into a runtime-generated type and is not trim-safe.")]
        public IQueryable Query(string tableName)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            if (!IsSafeIdentifier(tableName))
                throw new NormUsageException("Invalid table name.");
            // Gate C fix: Use normalized full connection string instead of 32-bit hash to
            // eliminate key collision risk. GetHashCode() is a 32-bit projection � two
            // distinct connection strings can share the same hash, causing schema aliasing
            // where queries against different databases return stale type definitions.
            // Normalizing (sort key=value pairs, lowercase) also ensures that identical
            // connection strings written in different key orders map to the same cache entry.
            //
            // Schema-signature fix: Include a hash of the live column name+type pairs in the
            // cache key. After a schema migration (new column added, column type changed), the
            // same provider|connstring|table triple would return the stale generated CLR type.
            // Including the schema signature means any schema change produces a new cache entry.
            // Old entries become unreachable and are eligible for LRU eviction.
            EnsureConnection();
            var schemaSignature = _typeGenerator.ComputeSchemaSignature(_cn, tableName);
            var cacheKey = $"{Provider.GetType().FullName}|{NormalizeConnectionString(_cn.ConnectionString)}|{tableName}|{schemaSignature}";
            var lazyTask = _dynamicTypeCache.GetOrAdd(cacheKey,
                _ => new Lazy<Task<Type>>(() => Task.FromResult(_typeGenerator.GenerateEntityType(Connection, tableName))));

            // Query(string) is synchronous. Generate the cached type on the current thread
            // instead of bouncing through Task.Run, which can starve the thread pool when many
            // callers hit the same dynamic root concurrently.
            var entityType = lazyTask.Value.GetAwaiter().GetResult();

            var generic = s_queryMethodInfo.Value.MakeGenericMethod(entityType);
            return (IQueryable)generic.Invoke(null, new object[] { this })!;
        }

        #region Transaction Savepoints
        /// <summary>
        /// Creates a savepoint within the provided transaction. Savepoints allow portions of a
        /// transaction to be rolled back without affecting the entire transaction scope.
        /// </summary>
        /// <param name="transaction">The active database transaction.</param>
        /// <param name="name">Name of the savepoint to create.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the savepoint has been created.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or empty.</exception>
        public Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            ThrowIfDisposed();
            if (transaction == null)
                throw new NormUsageException("No active transaction.");
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));
            return _p.CreateSavepointAsync(transaction, name, ct);
        }

        /// <summary>
        /// Rolls back the specified transaction to a previously created savepoint.
        /// </summary>
        /// <param name="transaction">The active database transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the transaction has been rolled back to the savepoint.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or empty.</exception>
        public Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            ThrowIfDisposed();
            if (transaction == null)
                throw new NormUsageException("No active transaction.");
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));
            return _p.RollbackToSavepointAsync(transaction, name, ct);
        }
        #endregion

        private void ValidateTenantContext<T>(T entity, TableMapping map, WriteOperation operation) where T : class
        {
            if (Options.TenantProvider == null) return;
            var tenantCol = map.TenantColumn;
            if (tenantCol == null) return;
            var tenantId = Options.TenantProvider.GetCurrentTenantId();
            if (tenantId == null)
                throw new NormConfigurationException("Tenant context required but not available");
            var entityTenant = tenantCol.Getter(entity);
            // Auto-injecting tenant ID is dangerous � developers might intend null for global records.
            // Requiring explicit tenant ID setting prevents accidental data leakage.
            if (entityTenant == null)
            {
                throw new NormConfigurationException($"Tenant ID is required for {operation} operation but was null. " +
                    "Explicitly set the tenant ID on the entity before saving. Auto-injection has been disabled for security.");
            }
            // X1 fix: use coercion-aware comparison matching the query filter path
            else if (!TenantIdsEqual(entityTenant, tenantId))
            {
                throw new NormConfigurationException("Tenant context mismatch");
            }
        }

        /// <summary>
        /// X1 fix: Compares two tenant ID values with type coercion to match the query filter path.
        /// The query path uses Convert.ChangeType, so ValidateTenantContext must also tolerate
        /// type mismatches (e.g., long provider ID vs int entity column).
        /// </summary>
        private static bool TenantIdsEqual(object a, object b)
        {
            if (Equals(a, b)) return true;
            // Coerce types like the query path does (e.g., long tenant ID vs int column)
            try
            {
                var coerced = Convert.ChangeType(b, a.GetType());
                return Equals(a, coerced);
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
            {
                return false;
            }
        }

        /// <summary>
        /// Sets the value of a shadow property for the specified entity instance.
        /// </summary>
        /// <param name="entity">The entity that owns the shadow property.</param>
        /// <param name="name">The name of the shadow property to set.</param>
        /// <param name="value">The value to assign.</param>
        public void SetShadowProperty(object entity, string name, object? value)
        {
            ThrowIfDisposed();
            Internal.ShadowPropertyStore.Set(entity, name, value);
        }

        /// <summary>
        /// Retrieves the value of a shadow property from the specified entity.
        /// </summary>
        /// <param name="entity">The entity that owns the shadow property.</param>
        /// <param name="name">The name of the shadow property to retrieve.</param>
        /// <returns>The current value of the shadow property, or <c>null</c> if not set.</returns>
        public object? GetShadowProperty(object entity, string name)
        {
            ThrowIfDisposed();
            return Internal.ShadowPropertyStore.Get(entity, name);
        }

        /// <summary>
        /// Creates a temporal tag entry in the database. Temporal tags can be used to
        /// correlate external events with the state of the database at a given time.
        /// </summary>
        /// <param name="tagName">The name of the tag to create.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CreateTagAsync(string tagName)
        {
            ThrowIfDisposed();
            await _executionStrategy.ExecuteAsync(async (ctx, ct) =>
            {
                await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var p0 = _p.ParamPrefix + "p0";
                var p1 = _p.ParamPrefix + "p1";
                // Use provider-escaped identifiers � raw table/column names are not safe across all
                // providers (e.g. SQL Server reserves "Timestamp" as a type keyword).
                await using var cmd = CommandPool.Get(ctx.Connection, _p.GetCreateTagSql(p0, p1));
                var span = new (string name, object value)[2];
                span[0] = (p0, tagName);
                span[1] = (p1, DateTime.UtcNow);
                cmd.SetParametersFast(span);
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                cmd.Parameters.Clear();
                return 0;
            }, default).ConfigureAwait(false);
        }

        /// <summary>
        /// Releases resources used by the context. When <paramref name="disposing"/>
        /// is <c>true</c>, both managed and unmanaged resources are released; otherwise
        /// only unmanaged resources are cleaned up.
        /// </summary>
        /// <param name="disposing">Indicates whether the method was invoked from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                // Use Dispose(WaitHandle) to ensure timer callbacks complete before proceeding.
                // Prevents ObjectDisposedException if a callback fires concurrently with Dispose.
                {
                    var waitHandle = new ManualResetEvent(false);
                    _cleanupTimer.Dispose(waitHandle);
                    waitHandle.WaitOne(TimeSpan.FromSeconds(CleanupTimerDrainTimeoutSeconds));
                    waitHandle.Dispose();
                }
                _providerInitLock.Dispose();
                _temporalInitLock.Dispose();
                DisposePreparedInsertCache();
                DisposeFastPathPreparedCommandCache();

                // Copy disposables to a local list inside the lock, then dispose outside the lock.
                // Prevents deadlock if a disposable's Dispose() acquires locks or accesses DbContext.
                List<IDisposable> toDispose = new();
                lock (_disposablesLock)
                {
                    CleanupDisposablesInternal();
                    for (var node = _disposables.First; node != null;)
                    {
                        var next = node.Next;
                        if (node.Value.TryGetTarget(out var d))
                        {
                            toDispose.Add(d);
                        }
                        _disposables.Remove(node);
                        node = next;
                    }
                }

                // Dispose items outside the lock to prevent deadlocks.
                // Exceptions are logged (not swallowed silently) so that resource-leak
                // failures surface in diagnostics, but disposal continues for remaining items.
                foreach (var d in toDispose)
                {
                    try
                    {
                        d.Dispose();
                    }
                    catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
                    {
                        Options.Logger?.LogDebug(ex,
                            "Exception during disposal of tracked resource {ResourceType}.",
                            d.GetType().Name);
                    }
                }

                // Only dispose the connection when this context owns it.
                if (_ownsConnection)
                    _cn?.Dispose();
                _disposed = true;
            }
        }

        private void CleanupDisposablesInternal()
        {
            for (var node = _disposables.First; node != null;)
            {
                var next = node.Next;
                if (!node.Value.TryGetTarget(out _))
                {
                    _disposables.Remove(node);
                }
                node = next;
            }
        }

        private void CleanupDisposables(object? state = null)
        {
            lock (_disposablesLock)
            {
                CleanupDisposablesInternal();
            }
        }

        private async Task CleanupDisposablesAsync()
        {
            List<IDisposable> toDispose = new();
            lock (_disposablesLock)
            {
                for (var n = _disposables.First; n != null;)
                {
                    var next = n.Next;
                    if (n.Value.TryGetTarget(out var d))
                        toDispose.Add(d);
                    _disposables.Remove(n);
                    n = next;
                }
            }
            foreach (var d in toDispose)
            {
                if (d is IAsyncDisposable ad) await ad.DisposeAsync().ConfigureAwait(false);
                else d.Dispose();
            }
        }

        /// <summary>
        /// Registers an <see cref="IDisposable"/> resource to be disposed when the context is disposed.
        /// </summary>
        /// <param name="disposable">The resource to track for disposal.</param>
        public void RegisterForDisposal(IDisposable disposable)
        {
            if (disposable != null)
            {
                lock (_disposablesLock)
                {
                    CleanupDisposablesInternal();
                    _disposables.AddLast(new WeakReference<IDisposable>(disposable));
                }
            }
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this context has been disposed.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name,
                    "This DbContext instance has been disposed. Create a new instance to continue.");
        }

        /// <summary>
        /// Releases all resources used by the context.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        /// <summary>
        /// Asynchronously releases all resources used by the context, including
        /// active connections and registered disposables.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                // Use WaitHandle pattern to safely stop the timer, preventing a race condition
                // where the timer callback fires concurrently with disposal.
                {
                    var waitHandle = new ManualResetEvent(false);
                    _cleanupTimer.Dispose(waitHandle);
                    // Use Task.Run with timeout to avoid blocking the async context indefinitely
                    // if a timer callback deadlocks or takes too long.
                    await Task.Run(() => waitHandle.WaitOne(TimeSpan.FromSeconds(CleanupTimerDrainTimeoutSeconds))).ConfigureAwait(false);
                    waitHandle.Dispose();
                }
                _providerInitLock.Dispose();
                _temporalInitLock.Dispose();
                await DisposePreparedInsertCacheAsync().ConfigureAwait(false);
                await DisposeFastPathPreparedCommandCacheAsync().ConfigureAwait(false);
                await CleanupDisposablesAsync().ConfigureAwait(false);
                // Only dispose the connection when this context owns it.
                // _cn is always non-null (set in constructor with null-guard), so the null
                // check is defensive only against theoretical subclass tampering.
                if (_ownsConnection && _cn != null)
                    await _cn.DisposeAsync().ConfigureAwait(false);
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }
    }

}
