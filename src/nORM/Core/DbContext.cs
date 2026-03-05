using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Transactions;
using System.Text;
using nORM.Configuration;
using nORM.Execution;
using nORM.Mapping;
using nORM.Providers;
using nORM.Internal;
using nORM.Navigation;
using nORM.Versioning;
using nORM.Scaffolding;
using nORM.Enterprise;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;
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
    public class DbContext : IDisposable, IAsyncDisposable
    {
        private readonly DbConnection _cn;
        // TX-1/MG-1: When false, Dispose/DisposeAsync must NOT close or dispose the connection
        // because it was passed in by the caller who retains ownership.
        private readonly bool _ownsConnection;
        private readonly DatabaseProvider _p;
        private readonly ConcurrentDictionary<Type, TableMapping> _m = new();
        private readonly IExecutionStrategy _executionStrategy;
        private readonly AdaptiveTimeoutManager _timeoutManager;
        private readonly ModelBuilder _modelBuilder;
        private readonly DynamicEntityTypeGenerator _typeGenerator = new();
        // PERFORMANCE FIX (TASK 3): Static cache to avoid recreating for every DbContext instance
        // Dynamic type generation is expensive (uses Reflection.Emit), so cache should be shared
        // across all DbContext instances in the application.
        // MEMORY LEAK FIX: Use LRU cache instead of unbounded ConcurrentDictionary to prevent
        // unbounded memory growth as new dynamic types are generated
        private static readonly ConcurrentLruCache<string, Lazy<Task<Type>>> _dynamicTypeCache = new(maxSize: 1000);
        private readonly LinkedList<WeakReference<IDisposable>> _disposables = new();
        private readonly object _disposablesLock = new();
        private readonly Timer _cleanupTimer;
        private bool _providerInitialized;
        private readonly SemaphoreSlim _providerInitLock = new(1, 1);
        private readonly object _providerInitSyncLock = new object(); // For synchronous initialization to avoid deadlock
        private readonly Lazy<Task>? _temporalInit;
        private DbTransaction? _currentTransaction; // Access via Interlocked.* only
        private bool _disposed;

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

        // TX-1/MG-1: Internal constructor that allows callers (e.g. migration runners) to pass an
        // externally-owned connection. When ownsConnection is false, Dispose/DisposeAsync will NOT
        // close or dispose the connection — the caller retains full ownership.
        internal DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options, bool ownsConnection)
        {
            _cn = cn ?? throw new ArgumentNullException(nameof(cn));
            _ownsConnection = ownsConnection;
            _p = p ?? throw new ArgumentNullException(nameof(p));
            Options = options ?? new DbContextOptions();
            Options.Validate();
            if (string.IsNullOrWhiteSpace(Options.TenantColumnName))
                throw new ArgumentException("TenantColumnName cannot be null or empty");
            if (Options.CacheExpiration <= TimeSpan.Zero)
                throw new ArgumentException("CacheExpiration must be positive");
            if (Options.CommandInterceptors.Any(i => i == null))
                throw new ArgumentException("CommandInterceptors cannot contain null entries");
            if (Options.SaveChangesInterceptors.Any(i => i == null))
                throw new ArgumentException("SaveChangesInterceptors cannot contain null entries");
            ChangeTracker = new ChangeTracker(Options);
            _modelBuilder = new ModelBuilder();
            Options.OnModelCreating?.Invoke(_modelBuilder);
            Database = new DatabaseFacade(this);
            _executionStrategy = Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(this, Options.RetryPolicy)
                : new DefaultExecutionStrategy(this);
            _timeoutManager = new AdaptiveTimeoutManager(Options.TimeoutConfiguration,
                Options.Logger ?? NullLogger.Instance);
            if (Options.IsTemporalVersioningEnabled)
            {
                _temporalInit = new Lazy<Task>(() => TemporalManager.InitializeAsync(this));
            }
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
                // RESOURCE LEAK FIX (TASK 16): Always dispose connection on failure.
                // If any exception occurs (including TypeLoadException, FileNotFoundException, etc.),
                // ensure the connection is properly disposed to prevent handle leaks.
                if (!success)
                {
                    connection?.Dispose();
                }
            }
        }
        /// <summary>
        /// Ensures that the underlying <see cref="DbConnection"/> is open and that the
        /// provider has performed any required initialization.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The open <see cref="DbConnection"/> instance.</returns>
        internal async Task<DbConnection> EnsureConnectionAsync(CancellationToken ct = default)
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
                        await _p.InitializeConnectionAsync(_cn, ct).ConfigureAwait(false);
                        _providerInitialized = true;
                    }
                }
                finally
                {
                    _providerInitLock.Release();
                }
            }
            if (_temporalInit != null)
            {
                await _temporalInit.Value.ConfigureAwait(false);
            }
            return _cn;
        }
        internal DbConnection EnsureConnection()
        {
            if (_cn.State != ConnectionState.Open)
                _cn.Open();
            if (!_providerInitialized)
            {
                // PROVIDER INITIALIZATION RACE FIX: Use regular lock instead of SemaphoreSlim.Wait()
                // to avoid deadlock in synchronous contexts (ASP.NET, UI threads with sync context)
                lock (_providerInitSyncLock)
                {
                    if (!_providerInitialized)
                    {
                        _p.InitializeConnection(_cn);
                        _providerInitialized = true;
                    }
                }
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
                // PERFORMANCE FIX: Skip detailed analysis for extremely large SQL (>100KB)
                // Scanning multi-megabyte strings with Contains/IndexOf causes severe slowdown
                if (sql.Length > 102400) // 100KB threshold
                {
                    baseComplexity = 20 + Math.Min(30, sql.Length / 10000);
                    Options.Logger?.LogDebug(
                        "Skipping detailed complexity analysis for large SQL ({Length} chars). Using length-based estimate: {Complexity}",
                        sql.Length, baseComplexity);
                }
                else
                {
                    baseComplexity = 1 + (sql.Length / 100);

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

                baseComplexity = Math.Min(baseComplexity, 50);
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

        /// <summary>
        /// Checks whether the database connection is healthy by executing a lightweight query.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if the connection is healthy; otherwise <c>false</c>.</returns>
        public async Task<bool> IsHealthyAsync(CancellationToken ct = default)
        {
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
                // RELIABILITY FIX: Don't hide cancellation - rethrow to respect cancellation tokens
                // Swallowing OperationCanceledException breaks proper async cancellation patterns
                throw;
            }
            catch (Exception ex)
            {
                // RELIABILITY FIX: Log exceptions instead of silently swallowing them
                // Silent failures make debugging connection issues nearly impossible
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
        /// SG-1: Hardened identifier validation that strips optional delimiters and validates
        /// the inner content against a strict allowlist. This prevents SQL injection via
        /// crafted identifiers like "[foo]; DROP TABLE Users--" that previously passed
        /// validation because they started with "[" and ended with "]".
        /// Validates that an identifier is safe for use in SQL queries. Allows:
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

        /// <summary>
        /// Gate C: Normalizes a connection string so that identical connection strings expressed
        /// with different key orderings or casing map to the same cache key. This eliminates
        /// collisions caused by using GetHashCode() (32-bit) as the cache key component.
        /// Normalization: split on ';', trim each pair, sort case-insensitively, rejoin.
        /// </summary>
        private static string NormalizeConnectionString(string? cs)
        {
            if (string.IsNullOrEmpty(cs)) return string.Empty;
            return string.Join(";", cs.Split(';', StringSplitOptions.RemoveEmptyEntries)
                .Select(p => p.Trim())
                .OrderBy(p => p, StringComparer.OrdinalIgnoreCase));
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
            // (no brackets, quotes, semicolons, hyphens, or other special chars)
            return System.Text.RegularExpressions.Regex.IsMatch(inner, @"^[\w\s]+$");
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
        public IQueryable Query(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            if (!IsSafeIdentifier(tableName))
                throw new NormUsageException("Invalid table name.");
            // Gate C fix: Use normalized full connection string instead of 32-bit hash to
            // eliminate key collision risk. GetHashCode() is a 32-bit projection — two
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
                _ => new Lazy<Task<Type>>(() => _typeGenerator.GenerateEntityTypeAsync(this.Connection, tableName)));

            // RELIABILITY FIX: Use Task.Run to avoid blocking the calling thread
            // This prevents potential deadlocks in synchronization contexts (e.g., ASP.NET, WPF)
            // The Lazy<T> ensures type generation happens only once per table, so the overhead is minimal
            var entityType = Task.Run(async () => await lazyTask.Value.ConfigureAwait(false)).GetAwaiter().GetResult();

            var method = typeof(NormQueryable).GetMethods()
                .Single(m => m.Name == nameof(NormQueryable.Query) && m.IsGenericMethodDefinition);
            var generic = method.MakeGenericMethod(entityType);
            return (IQueryable)generic.Invoke(null, new object[] { this })!;
        }

        #region Change Tracking
        /// <summary>
        /// Begins tracking the given entity in the <see cref="ChangeTracker"/> in the
        /// <see cref="EntityState.Added"/> state. The entity will be inserted into the
        /// database when <c>SaveChanges</c> is called.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity instance to add.</param>
        /// <returns>An <see cref="EntityEntry"/> representing the tracked entity.</returns>
        public EntityEntry Add<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Added, GetMapping(typeof(T)));
        }

        /// <summary>
        /// Starts tracking the entity without modifying its state. Existing values are
        /// assumed to match those in the database and no update will be sent unless
        /// changes are detected.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to attach.</param>
        /// <returns>An <see cref="EntityEntry"/> for the attached entity.</returns>
        public EntityEntry Attach<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(typeof(T)));
        }

        /// <summary>
        /// Marks the entity as <see cref="EntityState.Modified"/> so that all of its
        /// properties are treated as modified and will be persisted during
        /// <c>SaveChanges</c>.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to update.</param>
        /// <returns>An <see cref="EntityEntry"/> for the updated entity.</returns>
        public EntityEntry Update<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            return ChangeTracker.Track(entity, EntityState.Modified, GetMapping(typeof(T)));
        }

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be removed from the
        /// database when <c>SaveChanges</c> is executed.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity instance to remove.</param>
        /// <returns>An <see cref="EntityEntry"/> for the removed entity.</returns>
        public EntityEntry Remove<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            return ChangeTracker.Track(entity, EntityState.Deleted, GetMapping(typeof(T)));
        }
        /// <summary>
        /// Returns the <see cref="EntityEntry"/> for the supplied entity if it is already being tracked.
        /// SECURITY FIX (TASK 5): This method no longer auto-attaches untracked entities as this was a
        /// dangerous side-effect. If the entity is not tracked, an exception is thrown instructing the
        /// caller to use <see cref="Attach{T}"/> explicitly.
        /// </summary>
        /// <param name="entity">The entity whose tracking entry is requested.</param>
        /// <returns>An <see cref="EntityEntry"/> representing the entity's tracking information.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="entity"/> is null or invalid.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity is not currently tracked.</exception>
        public EntityEntry Entry(object entity)
        {
            NormValidator.ValidateEntity(entity, nameof(entity));

            // SECURITY FIX (TASK 5): Check if entity is already tracked before returning entry
            // Auto-attaching untracked entities is dangerous - it silently modifies tracking state
            var existingEntry = ChangeTracker.Entries.FirstOrDefault(e => ReferenceEquals(e.Entity, entity));
            if (existingEntry == null)
            {
                throw new InvalidOperationException(
                    $"The entity of type '{entity.GetType().Name}' is not being tracked by the context. " +
                    "Use context.Attach() to explicitly attach the entity before calling Entry().");
            }

            // Ensure lazy loading is enabled for the tracked entity
            var method = typeof(NavigationPropertyExtensions).GetMethod(nameof(NavigationPropertyExtensions.EnableLazyLoading))!;
            method.MakeGenericMethod(entity.GetType()).Invoke(null, new object[] { entity, this });

            return existingEntry;
        }

        /// <summary>
        /// Configures the context to automatically retry failed <c>SaveChanges</c>
        /// operations when the database reports a deadlock (SQL Server error 1205).
        /// A default retry policy with exponential backoff is applied.
        /// </summary>
        /// <returns>The current <see cref="DbContextOptions"/> instance for fluent configuration.</returns>
        public DbContextOptions UseDeadlockResilientSaveChanges()
        {
            Options.RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromSeconds(1),
                ShouldRetry = ex =>
                {
                    if (ex is DbException dbEx)
                        return (int?)dbEx.GetType().GetProperty("Number")?.GetValue(dbEx) == 1205;
                    return false;
                }
            };
            return Options;
        }
        /// <summary>
        /// Persists all tracked changes to the database. The operation is executed
        /// using the configured retry policy which transparently retries transient
        /// failures such as deadlocks.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        /// <remarks>
        /// PERFORMANCE WARNING (TASK 12): This method automatically calls DetectChanges() which
        /// performs snapshot-based comparison of ALL tracked entities. Avoid calling SaveChanges()
        /// in tight loops with many tracked entities. For bulk operations, use InsertBulkAsync()
        /// or UpdateBulkAsync() instead. For read-only queries, use AsNoTracking() to avoid
        /// change tracking overhead entirely.
        /// </remarks>
        public Task<int> SaveChangesAsync(CancellationToken ct = default)
            => SaveChangesWithRetryAsync(detectChanges: true, ct);

        /// <summary>
        /// Persists all tracked changes to the database. The operation is executed
        /// using the configured retry policy which transparently retries transient
        /// failures such as deadlocks.
        /// </summary>
        /// <param name="detectChanges">
        /// PERFORMANCE FIX (TASK 12): If true, automatically calls <see cref="ChangeTracker.DetectChanges"/>
        /// before saving. If false, assumes changes have been manually tracked or detected.
        /// Set to false for performance in scenarios with many tracked entities where you've
        /// manually set entity states using <c>context.Entry(entity).State = EntityState.Modified</c>.
        /// </param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        public Task<int> SaveChangesAsync(bool detectChanges, CancellationToken ct = default)
            => SaveChangesWithRetryAsync(detectChanges, ct);

        /// <summary>
        /// Invokes <see cref="SaveChangesInternalAsync"/> using the configured retry policy to
        /// transparently retry transient failures such as deadlocks.
        /// Exceptions thrown during or after commit are not retried because the commit outcome is unknown.
        /// </summary>
        /// <param name="detectChanges">If true, calls ChangeTracker.DetectChanges before saving.</param>
        /// <param name="ct">Token used to cancel the save operation.</param>
        /// <returns>The number of state entries written to the database.</returns>
        private async Task<int> SaveChangesWithRetryAsync(bool detectChanges, CancellationToken ct)
        {
            var policy = Options.RetryPolicy;
            var maxRetries = policy?.MaxRetries ?? 1;
            var baseDelay = policy?.BaseDelay ?? TimeSpan.Zero;
            var rand = Random.Shared;
            for (var attempt = 0; ; attempt++)
            {
                var commitAttempted = false;
                try
                {
                    return await SaveChangesInternalAsync(detectChanges, ct, onCommitAttempted: () => commitAttempted = true).ConfigureAwait(false);
                }
                catch (Exception ex) when (!commitAttempted && attempt < maxRetries - 1 && IsRetryableException(ex))
                {
                    // Only retry pre-commit transient failures — if commit was attempted, the outcome
                    // is unknown and retrying could produce duplicate rows.
                    var backoffMs = baseDelay.TotalMilliseconds * Math.Pow(2, attempt);
                    var jitter = 1 + (rand.NextDouble() * 0.4 - 0.2); // ±20%
                    var delay = TimeSpan.FromMilliseconds(backoffMs * jitter);
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Persists all tracked entity changes to the database within a single transaction.
        /// </summary>
        /// <param name="detectChanges">
        /// PERFORMANCE FIX (TASK 12): If true, calls ChangeTracker.DetectChanges before saving.
        /// DetectChanges iterates all tracked entities and compares their current values to
        /// original values, which can be expensive for contexts tracking thousands of entities.
        /// </param>
        /// <param name="ct">Token used to cancel the save operation.</param>
        /// <param name="onCommitAttempted">Optional callback invoked immediately before CommitAsync is called, used by retry logic to detect commit attempts.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        private async Task<int> SaveChangesInternalAsync(bool detectChanges, CancellationToken ct, Action? onCommitAttempted = null)
        {
            // PERFORMANCE FIX (TASK 12): Only detect changes if requested
            if (detectChanges)
            {
                ChangeTracker.DetectAllChanges();
            }
            var changedEntries = ChangeTracker.Entries
                .Where(e => e.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
                .ToList();
            if (changedEntries.Count == 0)
                return 0;

            var saveInterceptors = Options.SaveChangesInterceptors;
            if (saveInterceptors.Count > 0)
            {
                foreach (var interceptor in saveInterceptors)
                    await interceptor.SavingChangesAsync(this, changedEntries, ct).ConfigureAwait(false);

                // S4-1: Recompute changedEntries AFTER interceptors run.
                // Interceptors may call context.Add() or modify tracked entities during
                // SavingChangesAsync. Re-reading the change tracker ensures those additions
                // and modifications are included in the current save operation.
                changedEntries = ChangeTracker.Entries
                    .Where(e => e.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
                    .ToList();
                if (changedEntries.Count == 0)
                    return 0;
            }

            await using var transactionManager = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
            ct = transactionManager.Token;
            var transaction = transactionManager.Transaction;

            var totalAffected = 0;
            try
            {
                var allGroups = changedEntries.GroupBy(e => (e.State, e.Mapping)).ToList();
                var addedGroups = allGroups.Where(g => g.Key.State == EntityState.Added).ToList();
                var modifiedGroups = allGroups.Where(g => g.Key.State == EntityState.Modified).ToList();
                var deletedGroups = allGroups.Where(g => g.Key.State == EntityState.Deleted).ToList();

                var sortedAddedMappings = TopologicalSortMappings(addedGroups.Select(g => g.Key.Mapping)).ToList();
                // Gate D fix: Apply the same topological sort to modified groups so that FK
                // constraints do not fire when a dependent row is updated before its principal.
                // Inserts already follow principal-first order; updates must do the same.
                var sortedModifiedMappings = TopologicalSortMappings(modifiedGroups.Select(g => g.Key.Mapping)).ToList();
                var sortedDeletedMappings = TopologicalSortMappings(deletedGroups.Select(g => g.Key.Mapping)).Reverse().ToList();

                var orderedAddedGroups = sortedAddedMappings.Select(m => addedGroups.First(g => g.Key.Mapping == m));
                var orderedModifiedGroups = sortedModifiedMappings.Select(m => modifiedGroups.First(g => g.Key.Mapping == m));
                var orderedDeletedGroups = sortedDeletedMappings.Select(m => deletedGroups.First(g => g.Key.Mapping == m));
                var orderedGroups = orderedAddedGroups.Concat(orderedModifiedGroups).Concat(orderedDeletedGroups);

                foreach (var group in orderedGroups)
                {
                    var entries = group.ToList();
                    if (entries.Count == 0)
                        continue;
                    var map = group.Key.Mapping;
                    var state = group.Key.State;
                    await using var commandScope = new CommandScope(Connection, transaction);

                    var paramsPerEntity = state switch
                    {
                        EntityState.Added => map.InsertColumns.Length,
                        EntityState.Modified => map.UpdateColumns.Length + map.KeyColumns.Length + (map.TimestampColumn != null ? 1 : 0),
                        EntityState.Deleted => map.KeyColumns.Length + (map.TimestampColumn != null ? 1 : 0),
                        _ => 0
                    };
                    var batchSize = CalculateBatchSize(entries.Count, paramsPerEntity);
                    var templateLength = EstimateTemplateLength(state, map);

                    // PERFORMANCE FIX (TASK 14): Reuse DbCommand and StringBuilder across batches
                    // Instead of creating 100 DbCommand and StringBuilder for 10k entities in batches of 100,
                    // create ONE of each and clear/reset them between batches
                    await using var cmd = commandScope.CreateCommand();
                    var sql = new StringBuilder(templateLength * batchSize);

                    for (int start = 0; start < entries.Count; start += batchSize)
                    {
                        var batch = entries.Skip(start).Take(Math.Min(batchSize, entries.Count - start)).ToList();
                        // Clear for reuse
                        sql.Clear();
                        cmd.Parameters.Clear();

                        switch (state)
                        {
                            case EntityState.Added:
                                totalAffected += await ExecuteInsertBatch(cmd, map, batch, sql, 0, ct).ConfigureAwait(false);
                                break;
                            case EntityState.Modified:
                                totalAffected += await ExecuteUpdateBatch(cmd, map, batch, sql, 0, ct).ConfigureAwait(false);
                                break;
                            case EntityState.Deleted:
                                totalAffected += await ExecuteDeleteBatch(cmd, map, batch, sql, 0, ct).ConfigureAwait(false);
                                break;
                        }
                    }
                }
                onCommitAttempted?.Invoke();
                await transactionManager.CommitAsync().ConfigureAwait(false);

                // DATA INTEGRITY FIX (TASK 1): Accept changes only after successful commit
                // This ensures entities are not marked as Unchanged if transaction fails
                foreach (var entry in changedEntries)
                {
                    if (entry.State == EntityState.Deleted)
                    {
                        // Remove deleted entities from the ChangeTracker
                        if (entry.Entity is { } entityToRemove)
                            ChangeTracker.Remove(entityToRemove, true);
                    }
                    else
                    {
                        // Mark Added/Modified entities as Unchanged
                        entry.AcceptChanges();
                    }
                }
            }
            catch (Exception originalEx)
            {
                // S5-1: Preserve the original exception if rollback itself fails.
                // Without this guard, a rollback failure replaces originalEx entirely,
                // making it impossible to diagnose the root write failure.
                try
                {
                    await transactionManager.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception rollbackEx)
                {
                    throw new AggregateException(
                        "SaveChanges failed and rollback also failed. See inner exceptions for details.",
                        originalEx, rollbackEx);
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable — satisfies compiler
            }

            // C1: Fire SavedChangesAsync AFTER CommitAsync and AcceptChanges, and OUTSIDE the
            // try/catch block so an interceptor exception does not attempt to roll back an
            // already-committed transaction.
            if (saveInterceptors.Count > 0)
            {
                foreach (var interceptor in saveInterceptors)
                    await interceptor.SavedChangesAsync(this, changedEntries, totalAffected, ct).ConfigureAwait(false);
            }

            var cache = Options.CacheProvider;
            if (cache != null)
            {
                var tags = new HashSet<string>();
                foreach (var entry in changedEntries)
                {
                    if (entry.Entity is { } entity)
                    {
                        var map = GetMapping(entity.GetType());
                        tags.Add(map.TableName);
                    }
                }
                foreach (var tag in tags)
                    cache.InvalidateTag(tag);
            }
            return totalAffected;
        }

        private int CalculateBatchSize(int totalEntries, int paramsPerEntity)
        {
            var batchSize = totalEntries;
            if (_p.MaxParameters != int.MaxValue)
            {
                var maxParams = Math.Max(1, _p.MaxParameters - 10);
                batchSize = Math.Max(1, maxParams / Math.Max(1, paramsPerEntity));
            }
            return batchSize;
        }

        private int EstimateTemplateLength(EntityState state, TableMapping map)
            => state switch
            {
                EntityState.Added => BuildInsertBatch(map, 0).Length + 1,
                EntityState.Modified => BuildUpdateBatch(map, 0).Length + 1,
                EntityState.Deleted => BuildDeleteBatch(map, 0).Length + 1,
                _ => 0
            };

        /// <summary>
        /// Builds and executes a batched INSERT command for the provided entities.
        /// </summary>
        /// <param name="cmd">The command used to execute the batch.</param>
        /// <param name="map">Mapping information for the target table.</param>
        /// <param name="batch">Entities to insert.</param>
        /// <param name="sql">Reusable <see cref="StringBuilder"/> for composing the SQL batch.</param>
        /// <param name="paramIndex">Starting parameter index for parameter naming.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected.</returns>
        private async Task<int> ExecuteInsertBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct)
        {
            foreach (var entry in batch)
            {
                sql.Append(BuildInsertBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entry.Entity ?? throw new InvalidOperationException("Entity is null"),
                    WriteOperation.Insert, paramIndex);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            if (map.KeyColumns.Any(k => k.IsDbGenerated))
            {
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, ct).ConfigureAwait(false);
                int i = 0;
                do
                {
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var newId = reader.GetValue(0);
                        var entity = batch[i].Entity;
                        if (entity != null)
                        {
                            map.SetPrimaryKey(entity, newId);
                            // M2: Reindex the entity in the identity map now that it has a real PK
                            ChangeTracker.ReindexAfterInsert(entity, map);
                        }
                    }
                    // DATA INTEGRITY FIX (TASK 1): Removed AcceptChanges() call
                    // AcceptChanges must only be called after transaction commits successfully
                    i++;
                }
                while (await reader.NextResultAsync(ct).ConfigureAwait(false) && i < batch.Count);
                return reader.RecordsAffected;
            }

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // DATA INTEGRITY FIX (TASK 1): Removed AcceptChanges() calls
            // AcceptChanges must only be called after transaction commits successfully
            return affected;
        }

        /// <summary>
        /// Builds and executes a batched UPDATE statement for the provided entities.
        /// </summary>
        /// <param name="cmd">The command used to execute the batch.</param>
        /// <param name="map">Mapping information for the target table.</param>
        /// <param name="batch">Entities to update.</param>
        /// <param name="sql">Reusable <see cref="StringBuilder"/> for composing the SQL batch.</param>
        /// <param name="paramIndex">Starting parameter index for parameter naming.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        private async Task<int> ExecuteUpdateBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct)
        {
            foreach (var entry in batch)
            {
                var entity = entry.Entity ?? throw new InvalidOperationException("Entity is null");

                // CT-1 (PK mutation): Detect if the primary key was mutated after tracking.
                // Updating with a mutated key would target the wrong row.
                if (entry.OriginalKey != null && map.KeyColumns.Length > 0)
                {
                    object? currentKey;
                    if (map.KeyColumns.Length == 1)
                    {
                        currentKey = map.KeyColumns[0].Getter(entity);
                    }
                    else
                    {
                        var vals = new object?[map.KeyColumns.Length];
                        for (int i = 0; i < map.KeyColumns.Length; i++)
                            vals[i] = map.KeyColumns[i].Getter(entity);
                        currentKey = vals;
                    }

                    bool pkChanged;
                    if (currentKey is object?[] currentArr && entry.OriginalKey is object?[] origArr)
                        pkChanged = !currentArr.SequenceEqual(origArr);
                    else
                        pkChanged = !Equals(currentKey, entry.OriginalKey);

                    if (pkChanged)
                        throw new InvalidOperationException(
                            $"Primary key mutation detected on entity '{map.Type.Name}'. " +
                            "Primary keys cannot be changed after an entity is tracked. " +
                            "Detach the entity, modify the key, then re-attach.");
                }

                sql.Append(BuildUpdateBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entity,
                    WriteOperation.Update, paramIndex, entry.OriginalToken);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Update, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var updated = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // SG-1: Skip matched-row concurrency check for providers using affected-row semantics (e.g. MySQL).
            if (map.TimestampColumn != null && !Provider.UseAffectedRowsSemantics && updated != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            // DATA INTEGRITY FIX (TASK 1): Removed AcceptChanges() calls
            // AcceptChanges must only be called after transaction commits successfully
            return updated;
        }

        /// <summary>
        /// Builds and executes a batched DELETE statement for the provided entities.
        /// </summary>
        /// <param name="cmd">The command used to execute the batch.</param>
        /// <param name="map">Mapping information for the target table.</param>
        /// <param name="batch">Entities to delete.</param>
        /// <param name="sql">Reusable <see cref="StringBuilder"/> for composing the SQL batch.</param>
        /// <param name="paramIndex">Starting parameter index for parameter naming.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        private async Task<int> ExecuteDeleteBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct)
        {
            foreach (var entry in batch)
            {
                var entity = entry.Entity ?? throw new InvalidOperationException("Entity is null");

                // CT-1 (PK mutation): Detect if the primary key was mutated after tracking.
                // Deleting with a mutated key would target the wrong row.
                if (entry.OriginalKey != null && map.KeyColumns.Length > 0)
                {
                    object? currentKey;
                    if (map.KeyColumns.Length == 1)
                        currentKey = map.KeyColumns[0].Getter(entity);
                    else
                    {
                        var vals = new object?[map.KeyColumns.Length];
                        for (int i = 0; i < map.KeyColumns.Length; i++)
                            vals[i] = map.KeyColumns[i].Getter(entity);
                        currentKey = vals;
                    }
                    bool pkChanged;
                    if (currentKey is object?[] currentArr && entry.OriginalKey is object?[] origArr)
                        pkChanged = !currentArr.SequenceEqual(origArr);
                    else
                        pkChanged = !Equals(currentKey, entry.OriginalKey);
                    if (pkChanged)
                        throw new InvalidOperationException(
                            $"Primary key mutation detected on entity '{map.Type.Name}'. " +
                            "Primary keys cannot be changed after an entity is tracked. " +
                            "Detach the entity, modify the key, then re-attach.");
                }

                sql.Append(BuildDeleteBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entity,
                    WriteOperation.Delete, paramIndex, entry.OriginalToken);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Delete, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // SG-1: Skip matched-row concurrency check for providers using affected-row semantics (e.g. MySQL).
            if (map.TimestampColumn != null && !Provider.UseAffectedRowsSemantics && deleted != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            // DATA INTEGRITY FIX (TASK 1): Removed entity removal from ChangeTracker
            // This must only be done after transaction commits successfully
            return deleted;
        }

        /// <summary>
        /// T1: <see cref="TimeoutException"/> is intentionally NOT retried by default because a
        /// timed-out write operation may have already been partially applied by the database and
        /// retrying it could produce duplicate rows. Callers that want to retry on timeout must
        /// wrap the timeout condition inside their <see cref="DbContextOptions.RetryPolicy"/>
        /// by mapping the relevant <see cref="DbException"/> to a positive <c>ShouldRetry</c>
        /// result.
        /// </summary>
        private bool IsRetryableException(Exception ex)
        {
            if (ex is DbException dbEx && Options.RetryPolicy != null)
                return Options.RetryPolicy.ShouldRetry(dbEx);
            return false;   // T1: TimeoutException removed — retrying a timed-out write can duplicate data
        }
        /// <summary>
        /// FIX (TASK 8): Prevents integer overflow when converting TimeSpan to seconds.
        /// If TimeSpan.MaxValue or very large timeouts are provided, this ensures the value
        /// is clamped to int.MaxValue instead of wrapping to negative numbers.
        /// </summary>
        private static int ToSecondsClamped(TimeSpan t)
        {
            // Check for overflow before casting
            if (t.TotalSeconds > int.MaxValue)
                return int.MaxValue;

            return Math.Max(1, (int)Math.Ceiling(t.TotalSeconds));
        }
        #endregion

        #region Standard CRUD
        /// <summary>
        /// Inserts the specified entity into the database asynchronously using any
        /// configured retry policies.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to insert.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> InsertAsync<T>(T entity, CancellationToken ct = default) where T : class
            => InsertAsync(entity, null, ct);

        /// <summary>
        /// Inserts the specified entity within the provided transaction scope.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to insert.</param>
        /// <param name="transaction">Optional transaction used to execute the command.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> InsertAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
        {
            var result = WriteOptimizedAsync(entity, WriteOperation.Insert, ct, transaction);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return result;
        }

        /// <summary>
        /// Updates the specified entity in the database asynchronously.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to update.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> UpdateAsync<T>(T entity, CancellationToken ct = default) where T : class
            => UpdateAsync(entity, null, ct);

        /// <summary>
        /// Updates the entity within the given transaction.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Transaction to use; if null the context manages one.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> UpdateAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
            => WriteOptimizedAsync(entity, WriteOperation.Update, ct, transaction);

        /// <summary>
        /// Deletes the specified entity from the database asynchronously.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class
            => DeleteAsync(entity, null, ct);

        /// <summary>
        /// Deletes the entity within the supplied transaction.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction for the delete operation.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> DeleteAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
            => WriteOptimizedAsync(entity, WriteOperation.Delete, ct, transaction);

        private enum WriteOperation { Insert, Update, Delete }

        private async Task<int> WriteOptimizedAsync<T>(T entity, WriteOperation operation, CancellationToken ct, DbTransaction? transaction = null) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var map = GetMapping(typeof(T));
            ValidateTenantContext(entity, map, operation);
            var tx = transaction ?? Database.CurrentTransaction;

            if (operation == WriteOperation.Insert && Options.RetryPolicy == null && tx == null)
            {
                return await ExecuteFastInsert(entity, map, ct, null).ConfigureAwait(false);
            }
            if (tx != null)
            {
                return await WriteWithTransactionAsync(entity, map, operation, tx, ct, ownsTransaction: false).ConfigureAwait(false);
            }
            return await _executionStrategy.ExecuteAsync((ctx, token) =>
                WriteWithTransactionAsync(entity, map, operation, null, token, ownsTransaction: true), ct).ConfigureAwait(false);
        }

        private async Task<int> WriteWithTransactionAsync<T>(T entity, TableMapping map, WriteOperation operation, DbTransaction? transaction, CancellationToken ct, bool ownsTransaction) where T : class
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            // CT-1/TX-1: When we own the transaction, track it separately so we can always
            // dispose it (releasing server-side lock memory and log space) in a finally block,
            // regardless of whether the operation succeeds or fails.
            DbTransaction currentTransaction;
            DbTransaction? ownedTransaction = null;
            if (ownsTransaction)
            {
                ownedTransaction = await Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
                currentTransaction = ownedTransaction;
            }
            else
            {
                currentTransaction = transaction!;
            }

            try
            {
                await using var commandScope = new CommandScope(Connection, currentTransaction);
                try
                {
                    await using var cmd = commandScope.CreateCommand();
                    cmd.CommandText = operation switch
                    {
                        WriteOperation.Insert => _p.BuildInsert(map),
                        WriteOperation.Update => _p.BuildUpdate(map),
                        WriteOperation.Delete => _p.BuildDelete(map),
                        _ => throw new ArgumentOutOfRangeException(nameof(operation))
                    };
                    var opType = operation switch
                    {
                        WriteOperation.Insert => AdaptiveTimeoutManager.OperationType.Insert,
                        WriteOperation.Update => AdaptiveTimeoutManager.OperationType.Update,
                        WriteOperation.Delete => AdaptiveTimeoutManager.OperationType.Delete,
                        _ => AdaptiveTimeoutManager.OperationType.Insert
                    };
                    cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(opType, cmd.CommandText));
                    AddParametersOptimized(cmd, map, entity, operation);

                    if (operation == WriteOperation.Insert && map.KeyColumns.Any(k => k.IsDbGenerated))
                    {
                        var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
                        if (newId != null && newId != DBNull.Value) map.SetPrimaryKey(entity, newId);
                        if (ownsTransaction) await currentTransaction.CommitAsync(ct).ConfigureAwait(false);
                        return 1;
                    }
                    var recordsAffected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                    if ((operation is WriteOperation.Update or WriteOperation.Delete) &&
                        map.TimestampColumn != null && recordsAffected == 0)
                    {
                        throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
                    }
                    if (ownsTransaction) await currentTransaction.CommitAsync(ct).ConfigureAwait(false);
                    return recordsAffected;
                }
                catch (Exception originalEx)
                {
                    // S5-1: Preserve the original exception if rollback itself fails.
                    if (ownsTransaction)
                    {
                        try
                        {
                            // Use CancellationToken.None so a cancelled caller token does not abort the rollback.
                            await currentTransaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception rollbackEx)
                        {
                            throw new AggregateException(
                                "Write operation failed and rollback also failed. See inner exceptions for details.",
                                originalEx, rollbackEx);
                        }
                    }
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                    throw; // unreachable — satisfies compiler
                }
            }
            finally
            {
                // CT-1/TX-1: Always dispose the owned transaction to release server-side resources.
                if (ownedTransaction != null)
                    await ownedTransaction.DisposeAsync().ConfigureAwait(false);
            }
        }
        private async Task<int> ExecuteFastInsert<T>(T entity, TableMapping map, CancellationToken ct, DbTransaction? transaction) where T : class
        {
            if (transaction != null)
            {
                return await WriteWithTransactionAsync(entity, map, WriteOperation.Insert, transaction, ct, ownsTransaction: false).ConfigureAwait(false);
            }
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var ownTransaction = await _cn.BeginTransactionAsync(ct).ConfigureAwait(false);
            await using var commandScope = new CommandScope(_cn, ownTransaction);
            try
            {
                await using var cmd = commandScope.CreateCommand();
                cmd.CommandText = _p.BuildInsert(map);
                cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, cmd.CommandText));
                foreach (var col in map.Columns)
                {
                    if (!col.IsDbGenerated)
                    {
                        var value = col.Getter(entity) ?? DBNull.Value;
                        cmd.AddParam(_p.ParamPrefix + col.PropName, value);
                    }
                }

                // No unconditional Prepare() here either.

                if (map.KeyColumns.Any(k => k.IsDbGenerated))
                {
                    var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
                    if (newId != null && newId != DBNull.Value) map.SetPrimaryKey(entity, newId);
                    await ownTransaction.CommitAsync(ct).ConfigureAwait(false);
                    return 1;
                }
                var recordsAffected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                await ownTransaction.CommitAsync(ct).ConfigureAwait(false);
                return recordsAffected;
            }
            catch (Exception originalEx)
            {
                // S5-1: Preserve the original exception if rollback itself fails.
                try
                {
                    // Use CancellationToken.None so a cancelled caller token does not abort the rollback.
                    await ownTransaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception rollbackEx)
                {
                    throw new AggregateException(
                        "Fast insert failed and rollback also failed. See inner exceptions for details.",
                        originalEx, rollbackEx);
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable — satisfies compiler
            }
        }

        private void AddParametersOptimized<T>(DbCommand cmd, TableMapping map, T entity, WriteOperation operation) where T : class
        {
            switch (operation)
            {
                case WriteOperation.Insert:
                    foreach (var col in map.InsertColumns)
                        cmd.AddParam(_p.ParamPrefix + col.PropName, col.Getter(entity));
                    break;
                case WriteOperation.Update:
                    foreach (var col in map.UpdateColumns)
                        cmd.AddParam(_p.ParamPrefix + col.PropName, col.Getter(entity));
                    foreach (var col in map.KeyColumns)
                        cmd.AddParam(_p.ParamPrefix + col.PropName, col.Getter(entity));
                    if (map.TimestampColumn != null)
                        cmd.AddParam(_p.ParamPrefix + map.TimestampColumn.PropName, map.TimestampColumn.Getter(entity));
                    break;

                case WriteOperation.Delete:
                    foreach (var col in map.KeyColumns)
                        cmd.AddParam(_p.ParamPrefix + col.PropName, col.Getter(entity));
                    if (map.TimestampColumn != null)
                        cmd.AddParam(_p.ParamPrefix + map.TimestampColumn.PropName, map.TimestampColumn.Getter(entity));
                    break;
            }
        }

        private static IEnumerable<TableMapping> TopologicalSortMappings(IEnumerable<TableMapping> mappings)
        {
            var all = mappings.ToList();
            var deps = all.ToDictionary(
                m => m,
                m => all.Where(other => other != m && m.Columns.Any(c =>
                    // FK-2: Match by full type name first (namespace-qualified) to avoid collisions
                    // between types with the same simple name in different namespaces.
                    MatchesPrincipalType(c.ForeignKeyPrincipalTypeName, other.Type))).ToList());

            var result = new List<TableMapping>();
            var visited = new HashSet<TableMapping>();
            var inProgress = new HashSet<TableMapping>();

            void Visit(TableMapping node, List<TableMapping> path)
            {
                if (inProgress.Contains(node))
                {
                    var cycleStart = path.IndexOf(node);
                    var cyclePath = path.Skip(cycleStart).Append(node);
                    throw new NormConfigurationException(
                        $"Circular FK dependency detected: {string.Join(" -> ", cyclePath.Select(m => m.Type.Name))}");
                }
                if (!visited.Add(node)) return;
                inProgress.Add(node);
                path.Add(node);
                foreach (var dep in deps[node]) Visit(dep, path);
                path.RemoveAt(path.Count - 1);
                inProgress.Remove(node);
                result.Add(node);
            }

            foreach (var m in all) Visit(m, new List<TableMapping>());
            return result;
        }

        /// <summary>
        /// FK-2: Checks whether a FK principal type name matches the given type.
        /// Prefers exact full-name match (namespace-qualified) to avoid collisions
        /// between types with the same simple name in different namespaces.
        /// </summary>
        private static bool MatchesPrincipalType(string? principalTypeName, Type candidateType)
        {
            if (string.IsNullOrEmpty(principalTypeName)) return false;

            // Prefer full name match: e.g. "ModuleA.Customer" full name ends with ".Customer"
            var fullName = candidateType.FullName;
            if (fullName != null &&
                fullName.EndsWith("." + principalTypeName, StringComparison.OrdinalIgnoreCase))
                return true;

            // Fall back to simple name match for backward compatibility
            return string.Equals(principalTypeName, candidateType.Name, StringComparison.OrdinalIgnoreCase);
        }

        private string BuildInsertBatch(TableMapping map, int startParamIndex)
        {
            // INS-1: Only append identity retrieval when at least one key column is DB-generated.
            // For natural-key entities the fragment is wasteful and potentially wrong across providers.
            var identityFragment = map.KeyColumns.Any(k => k.IsDbGenerated)
                ? _p.GetIdentityRetrievalString(map)
                : string.Empty;
            var cols = map.InsertColumns;
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable} DEFAULT VALUES{identityFragment}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var paramNames = string.Join(", ", cols.Select((c, i) => $"{_p.ParamPrefix}p{startParamIndex + i}"));
            return $"INSERT INTO {map.EscTable} ({colNames}) VALUES ({paramNames}){identityFragment}";
        }

        private string BuildUpdateBatch(TableMapping map, int startParamIndex)
        {
            if (map.KeyColumns.Length == 0)
                throw new NormConfigurationException(string.Format(
                    ErrorMessages.InvalidConfiguration,
                    $"Entity '{map.Type.Name}' has no primary key; UPDATE requires a key."));

            // SQL-1: Guard against empty SET clause when entity has no mutable columns.
            // This happens when all columns are either keys or concurrency tokens.
            // Emitting "UPDATE T SET WHERE ..." is invalid SQL; throw a clear, actionable error.
            if (map.UpdateColumns.Length == 0)
                throw new NormConfigurationException(
                    $"Entity '{map.Type.Name}' has no mutable columns to update " +
                    "(all non-key columns are concurrency tokens or the entity only has key columns). " +
                    "Use [NotMapped] for computed properties or add at least one mutable property " +
                    "that is not a key or concurrency token.");

            var setSb = new StringBuilder();
            var idx = startParamIndex;
            for (int i = 0; i < map.UpdateColumns.Length; i++)
            {
                if (i > 0) setSb.Append(", ");
                setSb.Append(map.UpdateColumns[i].EscCol)
                    .Append('=')
                    .Append(_p.ParamPrefix).Append('p').Append(idx++);
            }
            var whereParts = new List<string>();
            foreach (var col in map.KeyColumns)
                whereParts.Add($"{col.EscCol}={_p.ParamPrefix}p{idx++}");
            if (map.TimestampColumn != null)
                whereParts.Add($"{map.TimestampColumn.EscCol}={_p.ParamPrefix}p{idx++}");
            if (Options.TenantProvider != null && map.TenantColumn != null)
                whereParts.Add($"{map.TenantColumn.EscCol}={_p.ParamPrefix}p{idx++}");
            var where = string.Join(" AND ", whereParts);
            return $"UPDATE {map.EscTable} SET {setSb} WHERE {where}";
        }

        private string BuildDeleteBatch(TableMapping map, int startParamIndex)
        {
            if (map.KeyColumns.Length == 0)
                throw new NormConfigurationException(string.Format(
                    ErrorMessages.InvalidConfiguration,
                    $"Entity '{map.Type.Name}' has no primary key; DELETE requires a key."));
            var idx = startParamIndex;
            var whereParts = new List<string>();
            foreach (var col in map.KeyColumns)
                whereParts.Add($"{col.EscCol}={_p.ParamPrefix}p{idx++}");
            if (map.TimestampColumn != null)
                whereParts.Add($"{map.TimestampColumn.EscCol}={_p.ParamPrefix}p{idx++}");
            if (Options.TenantProvider != null && map.TenantColumn != null)
                whereParts.Add($"{map.TenantColumn.EscCol}={_p.ParamPrefix}p{idx++}");
            var where = string.Join(" AND ", whereParts);
            return $"DELETE FROM {map.EscTable} WHERE {where}";
        }

        private int AddParametersBatched(DbCommand cmd, TableMapping map, object entity, WriteOperation operation, int startIndex, object? originalToken = null)
        {
            var index = startIndex;
            switch (operation)
            {
                case WriteOperation.Insert:
                    foreach (var col in map.InsertColumns)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", col.Getter(entity));
                    break;
                case WriteOperation.Update:
                    foreach (var col in map.UpdateColumns)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", col.Getter(entity));
                    foreach (var col in map.KeyColumns)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", col.Getter(entity));
                    if (map.TimestampColumn != null)
                    {
                        // CT-1: Use the original snapshot token when available rather than the current
                        // (possibly mutated) property value, to ensure the correct concurrency check.
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", tokenValue);
                    }
                    if (Options.TenantProvider != null && map.TenantColumn != null)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", Options.TenantProvider.GetCurrentTenantId());
                    break;
                case WriteOperation.Delete:
                    foreach (var col in map.KeyColumns)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", col.Getter(entity));
                    if (map.TimestampColumn != null)
                    {
                        // CT-1: Use the original snapshot token when available.
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", tokenValue);
                    }
                    if (Options.TenantProvider != null && map.TenantColumn != null)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", Options.TenantProvider.GetCurrentTenantId());
                    break;
            }
            return index;
        }

        private IReadOnlyDictionary<string, object> AddParametersFast(DbCommand cmd, object[] parameters)
        {
            var span = new (string name, object value)[parameters.Length];
            for (int i = 0; i < parameters.Length; i++)
            {
                var name = $"{_p.ParamPrefix}p{i}";
                var value = parameters[i] ?? DBNull.Value;
                span[i] = (name, value);
            }
            cmd.SetParametersFast(span);

            // Gate B fix: Always populate the parameter dictionary so that ValidateRawSql
            // receives accurate parameter metadata regardless of logging state.
            // The dictionary is required for validation (not just logging) — decoupling the
            // two concerns ensures parameterized queries are never incorrectly flagged by
            // the validator when debug logging is disabled.
            if (parameters.Length == 0)
                return EmptyDictionary<string, object>.Instance;

            var dict = new Dictionary<string, object>(parameters.Length);
            foreach (var (name, value) in span) dict[name] = value;
            return dict;
        }

        internal static class EmptyDictionary<TKey, TValue> where TKey : notnull
        {
            public static readonly IReadOnlyDictionary<TKey, TValue> Instance = new Dictionary<TKey, TValue>(0);
        }

        private readonly struct CommandScope : IAsyncDisposable
        {
            private readonly DbConnection _connection;
            private readonly DbTransaction? _transaction;
            public CommandScope(DbConnection connection, DbTransaction? transaction)
            {
                _connection = connection;
                _transaction = transaction;
            }

            /// <summary>
            /// Creates a <see cref="DbCommand"/> tied to the scoped connection and transaction.
            /// </summary>
            /// <returns>A configured command ready for parameter population and execution.</returns>
            public DbCommand CreateCommand()
            {
                var cmd = _connection.CreateCommand();
                if (_transaction != null)
                    cmd.Transaction = _transaction;
                return cmd;
            }
            /// <summary>
            /// Disposes the command scope. For pooled connections no additional
            /// cleanup is required so a completed <see cref="ValueTask"/> is returned.
            /// </summary>
            /// <returns>A completed task representing the asynchronous dispose operation.</returns>
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
        #endregion

        #region Bulk Operations
        /// <summary>
        /// Efficiently inserts a collection of entities using provider specific bulk
        /// techniques. Validation and tenant checks are applied to each entity before
        /// execution.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of inserted rows.</returns>
        public Task<int> BulkInsertAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                NormValidator.ValidateBulkOperation(entities, "insert");
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var map = GetMapping(typeof(T));
                foreach (var entity in entities)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Insert);
                }
                return await _p.BulkInsertAsync(ctx, map, entities, token).ConfigureAwait(false);
            }, ct);

        /// <summary>
        /// Performs a set based update of the provided entities using the provider's
        /// bulk update facilities.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">Entities to update.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of updated rows.</returns>
        public Task<int> BulkUpdateAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                NormValidator.ValidateBulkOperation(entities, "update");
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var map = GetMapping(typeof(T));
                foreach (var entity in entities)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Update);
                }
                return await _p.BulkUpdateAsync(ctx, map, entities, token).ConfigureAwait(false);
            }, ct);

        /// <summary>
        /// Removes a collection of entities from the database using bulk delete
        /// operations.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">Entities to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of deleted rows.</returns>
        public Task<int> BulkDeleteAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                NormValidator.ValidateBulkOperation(entities, "delete");
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var map = GetMapping(typeof(T));
                foreach (var entity in entities)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Delete);
                }
                return await _p.BulkDeleteAsync(ctx, map, entities, token).ConfigureAwait(false);
            }, ct);
        #endregion

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
            if (transaction == null)
                throw new InvalidOperationException("No active transaction.");
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
            if (transaction == null)
                throw new InvalidOperationException("No active transaction.");
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));
            return _p.RollbackToSavepointAsync(transaction, name, ct);
        }
        #endregion

        #region Raw SQL & Stored Procedures
        /// <summary>
        /// Executes the provided SQL and materializes the results into instances of
        /// <typeparamref name="T"/> without tracking them in the <see cref="ChangeTracker"/>.
        /// </summary>
        /// <typeparam name="T">Type to materialize each row to.</typeparam>
        /// <param name="sql">Raw SQL query to execute.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Optional parameters for the SQL query.</param>
        /// <returns>A list of entities populated from the query results.</returns>
        public Task<List<T>> QueryUnchangedAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, sql);
                // TX-1: Bind to active transaction so raw SQL reads participate in the unit-of-work.
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                if (!NormValidator.IsSafeRawSql(sql, ctx.Provider))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");
                NormValidator.ValidateRawSql(sql, paramDict);

                // PERFORMANCE FIX (TASK 8): Use MaterializerFactory for fast compiled materialization
                // instead of slow reflection. MaterializerFactory generates IL.Emit or Expression-based
                // materializers that are 10-100x faster than reflection for large result sets.
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                
                // Get or create mapping for type T - this supports both mapped entities and ad-hoc types
                var mapping = ctx.GetMapping(typeof(T));
                
                // Create fast compiled materializer using MaterializerFactory
                var factory = new global::nORM.Query.MaterializerFactory();
                var materializer = factory.CreateSyncMaterializer(mapping, typeof(T));

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    var item = (T)materializer(reader);
                    list.Add(item);
                }

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);

        /// <summary>
        /// Executes a raw SQL query and materializes the results into instances of
        /// <typeparamref name="T"/>. Unlike <see cref="QueryUnchangedAsync"/>, the
        /// entities are materialized using the nORM query translation pipeline which
        /// supports projections and navigations.
        /// </summary>
        /// <typeparam name="T">Result entity type.</typeparam>
        /// <param name="sql">Raw SQL query to execute.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Optional parameters for the SQL query.</param>
        /// <returns>A list of materialized entities.</returns>
        public Task<List<T>> FromSqlRawAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, sql);
                // TX-1: Bind to active transaction so raw SQL reads participate in the unit-of-work.
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                if (!NormValidator.IsSafeRawSql(sql, ctx.Provider))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");
                NormValidator.ValidateRawSql(sql, paramDict);

                using var translator = global::nORM.Query.QueryTranslator.Rent(this);
                var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add((T)await materializer(reader, token).ConfigureAwait(false));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);

        /// <summary>
        /// Executes a stored procedure and materializes the first result set into
        /// instances of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Type to materialize the rows to.</typeparam>
        /// <param name="procedureName">Name of the stored procedure.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Anonymous object containing input parameters.</param>
        /// <returns>A list of results returned by the procedure.</returns>
        public Task<List<T>> ExecuteStoredProcedureAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, procedureName);
                // TX-1: Bind to active transaction so stored procedure calls participate in the unit-of-work.
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandType = ctx._p.StoredProcedureCommandType;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

                var paramDict = new Dictionary<string, object>();
                if (parameters != null)
                {
                    var props = parameters.GetType().GetProperties();
                    var span = new (string name, object value)[props.Length];
                    for (int i = 0; i < props.Length; i++)
                    {
                        var pName = ctx._p.ParamPrefix + props[i].Name;
                        var pValue = props[i].GetValue(parameters) ?? DBNull.Value;
                        span[i] = (pName, pValue);
                        paramDict[pName] = pValue;
                    }
                    cmd.SetParametersFast(span);
                }

                // PRV-1: Use the same dual-check as the async-enumerable variant: accept either
                // a safe identifier (stored proc name) or a safe raw SQL (for providers like SQLite
                // that use CommandType.Text and pass a SELECT query as the "procedure name").
                if (!IsSafeIdentifier(procedureName) && !NormValidator.IsSafeRawSql(procedureName, ctx._p))
                    throw new NormUsageException("Potential SQL injection detected in stored procedure name.");

                NormValidator.ValidateRawSql(procedureName, paramDict);

                using var translator = global::nORM.Query.QueryTranslator.Rent(this);
                var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add((T)await materializer(reader, token).ConfigureAwait(false));

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);

        /// <summary>
        /// Streams the results of a stored procedure as an <see cref="IAsyncEnumerable{T}"/>.
        /// This is useful for large result sets where buffering would be prohibitive.
        /// </summary>
        /// <typeparam name="T">Type of objects yielded.</typeparam>
        /// <param name="procedureName">Name of the stored procedure.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Anonymous object containing input parameters.</param>
        /// <returns>An asynchronous stream of materialized entities.</returns>
        public async IAsyncEnumerable<T> ExecuteStoredProcedureAsAsyncEnumerable<T>(string procedureName, [EnumeratorCancellation] CancellationToken ct = default, object? parameters = null) where T : class, new()
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            await using var cmd = CommandPool.Get(Connection, procedureName);
            // TX-1: Bind to active transaction so stored procedure calls participate in the unit-of-work.
            if (CurrentTransaction != null)
                cmd.Transaction = CurrentTransaction;
            cmd.CommandType = _p.StoredProcedureCommandType;
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

            var paramDict = new Dictionary<string, object>();
            if (parameters != null)
            {
                var props = parameters.GetType().GetProperties();
                var span = new (string name, object value)[props.Length];
                for (int i = 0; i < props.Length; i++)
                {
                    var pName = _p.ParamPrefix + props[i].Name;
                    var pValue = props[i].GetValue(parameters) ?? DBNull.Value;
                    span[i] = (pName, pValue);
                    paramDict[pName] = pValue;
                }
                cmd.SetParametersFast(span);
            }

            if (!IsSafeIdentifier(procedureName) && !NormValidator.IsSafeRawSql(procedureName, Provider))
                throw new NormUsageException("Potential SQL injection detected in stored procedure name.");

            NormValidator.ValidateRawSql(procedureName, paramDict);

            using var translator = global::nORM.Query.QueryTranslator.Rent(this);
            var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
            var count = 0;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.SequentialAccess, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = (T)await materializer(reader, ct).ConfigureAwait(false);
                count++;
                yield return entity;
            }

            Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, count);
            cmd.Parameters.Clear();
        }

        /// <summary>
        /// Executes a stored procedure that returns both a result set and output
        /// parameters. The result set is materialized to <typeparamref name="T"/> and
        /// output parameters are captured in the returned <see cref="StoredProcedureResult{T}"/>.
        /// </summary>
        /// <typeparam name="T">Type to materialize the first result set.</typeparam>
        /// <param name="procedureName">Name of the stored procedure.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Anonymous object containing input parameters.</param>
        /// <param name="outputParameters">Definitions of output parameters to retrieve.</param>
        /// <returns>A <see cref="StoredProcedureResult{T}"/> containing results and output values.</returns>
        public Task<StoredProcedureResult<T>> ExecuteStoredProcedureWithOutputAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null, params OutputParameter[] outputParameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, procedureName);
                // TX-1: Bind to active transaction so stored procedure calls participate in the unit-of-work.
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandType = ctx._p.StoredProcedureCommandType;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

                var paramDict = new Dictionary<string, object>();
                if (parameters != null)
                {
                    var props = parameters.GetType().GetProperties();
                    var span = new (string name, object value)[props.Length];
                    for (int i = 0; i < props.Length; i++)
                    {
                        var pName = ctx._p.ParamPrefix + props[i].Name;
                        var pValue = props[i].GetValue(parameters) ?? DBNull.Value;
                        span[i] = (pName, pValue);
                        paramDict[pName] = pValue;
                    }
                    cmd.SetParametersFast(span);
                }

                var outputParamMap = new Dictionary<string, DbParameter>();
                foreach (var op in outputParameters)
                {
                    // SECURITY FIX (TASK 20): Validate parameter name to prevent SQL injection
                    if (!IsSafeIdentifier(op.Name))
                        throw new NormUsageException($"Invalid output parameter name: '{op.Name}'. " +
                            "Parameter names must contain only alphanumeric characters, underscores, and periods.");
                    var pName = _p.ParamPrefix + op.Name;
                    var p = cmd.CreateParameter();
                    p.ParameterName = pName;
                    p.DbType = op.DbType;
                    p.Direction = ParameterDirection.Output;
                    if (op.Size.HasValue) p.Size = op.Size.Value;
                    cmd.Parameters.Add(p);
                    outputParamMap[op.Name] = p;
                }

                // PRV-1: Use dual-check — accept either a safe identifier or safe raw SQL
                if (!IsSafeIdentifier(procedureName) && !NormValidator.IsSafeRawSql(procedureName, ctx._p))
                    throw new NormUsageException("Potential SQL injection detected in stored procedure name.");

                NormValidator.ValidateRawSql(procedureName, paramDict);

                using var translator = global::nORM.Query.QueryTranslator.Rent(this);
                var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add((T)await materializer(reader, token).ConfigureAwait(false));
                await reader.DisposeAsync().ConfigureAwait(false);

                var outputs = new Dictionary<string, object?>();
                foreach (var kv in outputParamMap)
                    outputs[kv.Key] = kv.Value.Value == DBNull.Value ? null : kv.Value.Value;

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return new StoredProcedureResult<T>(list, outputs);
            }, ct);
        #endregion

        private void ValidateTenantContext<T>(T entity, TableMapping map, WriteOperation operation) where T : class
        {
            if (Options.TenantProvider == null) return;
            var tenantCol = map.TenantColumn;
            if (tenantCol == null) return;
            var tenantId = Options.TenantProvider.GetCurrentTenantId();
            if (tenantId == null)
                throw new InvalidOperationException("Tenant context required but not available");
            var entityTenant = tenantCol.Getter(entity);
            // SECURITY FIX (TASK 18): Changed auto-injection to throw exception.
            // Auto-injecting tenant ID is dangerous - developers might intend null for global records.
            // Requiring explicit tenant ID setting prevents accidental data leakage.
            if (entityTenant == null)
            {
                throw new InvalidOperationException($"Tenant ID is required for {operation} operation but was null. " +
                    "Explicitly set the tenant ID on the entity before saving. Auto-injection has been disabled for security.");
            }
            else if (!Equals(entityTenant, tenantId))
            {
                throw new InvalidOperationException("Tenant context mismatch");
            }
        }

        /// <summary>
        /// Sets the value of a shadow property for the specified entity instance.
        /// </summary>
        /// <param name="entity">The entity that owns the shadow property.</param>
        /// <param name="name">The name of the shadow property to set.</param>
        /// <param name="value">The value to assign.</param>
        public void SetShadowProperty(object entity, string name, object? value)
            => Internal.ShadowPropertyStore.Set(entity, name, value);

        /// <summary>
        /// Retrieves the value of a shadow property from the specified entity.
        /// </summary>
        /// <param name="entity">The entity that owns the shadow property.</param>
        /// <param name="name">The name of the shadow property to retrieve.</param>
        /// <returns>The current value of the shadow property, or <c>null</c> if not set.</returns>
        public object? GetShadowProperty(object entity, string name)
            => Internal.ShadowPropertyStore.Get(entity, name);

        /// <summary>
        /// Creates a temporal tag entry in the database. Temporal tags can be used to
        /// correlate external events with the state of the database at a given time.
        /// </summary>
        /// <param name="tagName">The name of the tag to create.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CreateTagAsync(string tagName)
        {
            await _executionStrategy.ExecuteAsync(async (ctx, ct) =>
            {
                await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var p0 = _p.ParamPrefix + "p0";
                var p1 = _p.ParamPrefix + "p1";
                await using var cmd = CommandPool.Get(ctx.Connection, $"INSERT INTO __NormTemporalTags (TagName, Timestamp) VALUES ({p0}, {p1})");
                var span = new (string name, object value)[2];
                span[0] = (p0, tagName);
                span[1] = (p1, DateTime.UtcNow);
                cmd.SetParametersFast(span);
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                cmd.Parameters.Clear();
                return 0;
            }, default).ConfigureAwait(false);
        }

        #region Prepared Statements
        /// <summary>
        /// Creates a prepared INSERT statement for the specified entity type. This allows
        /// the cost of SQL generation and command preparation to be paid only once, with
        /// subsequent executions only updating parameter values. Ideal for batch insert
        /// scenarios where the same operation is repeated many times.
        /// </summary>
        /// <typeparam name="T">The entity type to prepare the insert for.</typeparam>
        /// <param name="ct">Cancellation token for the operation.</param>
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
        public async Task<PreparedOperation<T>> PrepareInsertAsync<T>(CancellationToken ct = default) where T : class
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var mapping = GetMapping(typeof(T));
            var cmd = CreateCommand();
            cmd.CommandText = _p.BuildInsert(mapping);
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, cmd.CommandText));

            // Pre-create parameters for all insert columns
            foreach (var col in mapping.InsertColumns)
            {
                var p = cmd.CreateParameter();
                p.ParameterName = _p.ParamPrefix + col.PropName;
                cmd.Parameters.Add(p);
            }

            // Prepare the command at the database level
            await cmd.PrepareAsync(ct).ConfigureAwait(false);

            return new PreparedOperation<T>(cmd, mapping, this);
        }
        #endregion

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
                // RESOURCE LEAK FIX (TASK 15): Use Dispose(WaitHandle) to ensure timer callbacks complete
                // before proceeding. This prevents ObjectDisposedException if callback is running.
                if (_cleanupTimer != null)
                {
                    var waitHandle = new ManualResetEvent(false);
                    _cleanupTimer.Dispose(waitHandle);
                    waitHandle.WaitOne();
                    waitHandle.Dispose();
                }
                _providerInitLock?.Dispose();

                // DEADLOCK FIX (TASK 5): Copy disposables to local list inside lock, then dispose outside lock
                // This prevents deadlock if a disposable's Dispose() method tries to access DbContext or acquire locks
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

                // Dispose items outside the lock to prevent deadlocks
                foreach (var d in toDispose)
                {
                    try
                    {
                        d.Dispose();
                    }
                    catch
                    {
                        // Suppress exceptions during disposal
                    }
                }

                // TX-1/MG-1: Only dispose the connection when this context owns it.
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
                // RESOURCE LEAK FIX (TASK 6): Use WaitHandle pattern like Dispose() to safely stop timer
                // Prevents race condition where timer callback runs during disposal
                if (_cleanupTimer != null)
                {
                    var waitHandle = new ManualResetEvent(false);
                    _cleanupTimer.Dispose(waitHandle);
                    // Use Task.Run to avoid blocking the async context
                    await Task.Run(() => waitHandle.WaitOne()).ConfigureAwait(false);
                    waitHandle.Dispose();
                }
                _providerInitLock?.Dispose();
                await CleanupDisposablesAsync().ConfigureAwait(false);
                // TX-1/MG-1: Only dispose the connection when this context owns it.
                if (_ownsConnection && _cn != null)
                    await _cn.DisposeAsync().ConfigureAwait(false);
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Represents an output parameter for stored procedure execution.
    /// </summary>
    /// <param name="Name">Name of the parameter without provider-specific prefix.</param>
    /// <param name="DbType">Database type of the output parameter.</param>
    /// <param name="Size">Optional size for variable-length parameters.</param>
    public sealed record OutputParameter(string Name, DbType DbType, int? Size = null);
    /// <summary>
    /// Encapsulates the results of a stored procedure that returns both a result set
    /// and output parameters.
    /// </summary>
    /// <typeparam name="T">Type of entities in the result set.</typeparam>
    /// <param name="Results">List of materialized entities returned by the procedure.</param>
    /// <param name="OutputParameters">Dictionary of output parameter values keyed by name.</param>
    public sealed record StoredProcedureResult<T>(List<T> Results, IReadOnlyDictionary<string, object?> OutputParameters);

    /// <summary>
    /// Represents a prepared database operation that can be executed multiple times
    /// with different parameter values. The SQL and command structure are prepared
    /// once and reused, providing significant performance benefits for repeated
    /// operations like batch inserts.
    /// </summary>
    /// <typeparam name="T">The entity type this operation works with.</typeparam>
    public sealed class PreparedOperation<T> : IAsyncDisposable where T : class
    {
        private readonly DbCommand _command;
        private readonly TableMapping _mapping;
        private readonly DbContext _context;
        // OPTIMIZATION: Cache parameter objects and their corresponding column accessors
        // to avoid dictionary lookups (O(N) or O(1)) inside the tight execution loop.
        private readonly (DbParameter Parameter, Mapping.Column Column)[] _bindings;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PreparedOperation{T}"/> class.
        /// </summary>
        /// <param name="command">The prepared database command.</param>
        /// <param name="mapping">The table mapping for the entity type.</param>
        /// <param name="context">The database context that created this operation.</param>
        internal PreparedOperation(DbCommand command, TableMapping mapping, DbContext context)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
            _context = context ?? throw new ArgumentNullException(nameof(context));

            // Pre-calculate bindings
            var insertCols = _mapping.InsertColumns;
            _bindings = new (DbParameter, Mapping.Column)[insertCols.Length];
            var prefix = _context.Provider.ParamPrefix;

            for (int i = 0; i < insertCols.Length; i++)
            {
                var col = insertCols[i];
                var paramName = prefix + col.PropName;
                _bindings[i] = (_command.Parameters[paramName], col);
            }
        }

        /// <summary>
        /// Executes the prepared operation for the specified entity. Parameter values
        /// are updated from the entity properties and the command is executed.
        /// </summary>
        /// <param name="entity">The entity to insert.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of rows affected.</returns>
        public async Task<int> ExecuteAsync(T entity, CancellationToken ct = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(PreparedOperation<T>));
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // Update parameter values using cached bindings (Array iteration is faster than Dictionary lookup)
            var bindings = _bindings; // Local copy for elimination of bounds checks (potentially)
            for (int i = 0; i < bindings.Length; i++)
            {
                var (param, col) = bindings[i];
                var value = col.Getter(entity);
                param.Value = value ?? DBNull.Value;
            }

            // Execute the command
            if (_mapping.KeyColumns.Any(k => k.IsDbGenerated))
            {
                // For tables with db-generated keys, use ExecuteScalar to get the new ID
                var newId = await _command.ExecuteScalarWithInterceptionAsync(_context, ct).ConfigureAwait(false);
                if (newId != null && newId != DBNull.Value)
                {
                    _mapping.SetPrimaryKey(entity, newId);
                }
                return 1;
            }
            else
            {
                // For tables without db-generated keys, use ExecuteNonQuery
                return await _command.ExecuteNonQueryWithInterceptionAsync(_context, ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Releases all resources used by this prepared operation.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                await _command.DisposeAsync().ConfigureAwait(false);
                _disposed = true;
            }
        }
    }
}
