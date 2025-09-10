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
using System.Text.RegularExpressions;
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
    public class DbContext : IDisposable, IAsyncDisposable
    {
        private readonly DbConnection _cn;
        private readonly DatabaseProvider _p;
        private readonly ConcurrentDictionary<Type, TableMapping> _m = new();
        private readonly IExecutionStrategy _executionStrategy;
        private readonly AdaptiveTimeoutManager _timeoutManager;
        private readonly ModelBuilder _modelBuilder;
        private readonly DynamicEntityTypeGenerator _typeGenerator = new();
        private readonly ConcurrentDictionary<string, Type> _dynamicTypeCache = new();
        private readonly LinkedList<WeakReference<IDisposable>> _disposables = new();
        private readonly object _disposablesLock = new();
        private readonly Timer _cleanupTimer;
        private bool _providerInitialized;
        private readonly SemaphoreSlim _providerInitLock = new(1, 1);
        private DbTransaction? _currentTransaction; // Access via Interlocked.* only
        private bool _disposed;
        public DbContextOptions Options { get; }
        public ChangeTracker ChangeTracker { get; }
        public DatabaseFacade Database { get; }
        public DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options = null)
        {
            _cn = cn ?? throw new ArgumentNullException(nameof(cn));
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
                TemporalManager.InitializeAsync(this).GetAwaiter().GetResult();
            }
            _cleanupTimer = new Timer(_ => CleanupDisposables(), null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }
        public DbContext(string connectionString, DatabaseProvider p, DbContextOptions? options = null)
            : this(CreateConnectionSafe(connectionString, p), p, options)
        {
        }
        private static DbConnection CreateConnectionSafe(string connectionString, DatabaseProvider provider)
        {
            DbConnection? connection = null;
            try
            {
                connection = DbConnectionFactory.Create(connectionString, provider);
                return connection;
            }
            catch (Exception ex)
            {
                connection?.Dispose();
                var safeConnStr = NormValidator.MaskSensitiveConnectionStringData(connectionString);
                throw new ArgumentException($"Invalid connection string format: {safeConnStr}", nameof(connectionString), ex);
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
            return _cn;
        }
        internal DbConnection EnsureConnection()
        {
            if (_cn.State != ConnectionState.Open)
                _cn.Open();
            if (!_providerInitialized)
            {
                _providerInitLock.Wait();
                try
                {
                    if (!_providerInitialized)
                    {
                        _p.InitializeConnection(_cn);
                        _providerInitialized = true;
                    }
                }
                finally
                {
                    _providerInitLock.Release();
                }
            }
            return _cn;
        }
        internal TimeSpan GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType operationType, string? sql = null, int recordCount = 1)
        {
            var complexity = EstimateQueryComplexity(sql);
            return _timeoutManager.GetTimeoutForOperation(operationType, recordCount, complexity);
        }

        private static readonly Regex JoinRegex = new(@"\bJOIN\b", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex SelectRegex = new(@"\bSELECT\b", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static int EstimateQueryComplexity(string? sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return 1;
            var joinCount = JoinRegex.Matches(sql).Count;
            var subQueryCount = Math.Max(0, SelectRegex.Matches(sql).Count - 1);
            return joinCount * 1000 + subQueryCount * 500;
        }

        public DbConnection Connection => _cn;
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
            catch { return false; }
        }

        internal TableMapping GetMapping(Type t) => _m.GetOrAdd(t, static (k, args) =>
            new TableMapping(k, args.p, args.ctx, args.modelBuilder.GetConfiguration(k)), (p: _p, ctx: this, modelBuilder: _modelBuilder));

        internal IEnumerable<TableMapping> GetAllMappings()
        {
            foreach (var type in _modelBuilder.GetConfiguredEntityTypes())
                yield return GetMapping(type);
        }

        private static bool IsSafeIdentifier(string name)
            => !string.IsNullOrWhiteSpace(name) && Regex.IsMatch(name, @"^[A-Za-z0-9_\.]+$");

        /// <summary>
        /// Creates a non-typed <see cref="IQueryable"/> over the specified table name.
        /// This is primarily used for querying tables that do not have a compile-time
        /// entity type, such as dynamic projections or tables discovered at runtime.
        /// </summary>
        /// <param name="tableName">Name of the table to query.</param>
        /// <returns>An <see cref="IQueryable"/> representing the requested table.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or empty.</exception>
        /// <exception cref="NormUsageException">Thrown when the provided name contains invalid characters.</exception>
        public IQueryable Query(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            if (!IsSafeIdentifier(tableName))
                throw new NormUsageException("Invalid table name.");
            var logical = tableName;
            var entityType = _dynamicTypeCache.GetOrAdd(logical,
                _ => _typeGenerator.GenerateEntityType(this.Connection, logical));
            var method = typeof(NormQueryable).GetMethods()
                .Single(m => m.Name == nameof(NormQueryable.Query) && m.IsGenericMethodDefinition);
            var generic = method.MakeGenericMethod(entityType);
            return (IQueryable)generic.Invoke(null, new object[] { this })!;
        }

        #region Change Tracking
        public EntityEntry Add<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Added, GetMapping(typeof(T)));
        }
        public EntityEntry Attach<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(typeof(T)));
        }
        public EntityEntry Update<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            return ChangeTracker.Track(entity, EntityState.Modified, GetMapping(typeof(T)));
        }
        public EntityEntry Remove<T>(T entity) where T : class
        {
            NormValidator.ValidateEntity(entity);
            return ChangeTracker.Track(entity, EntityState.Deleted, GetMapping(typeof(T)));
        }
        /// <summary>
        /// Returns the tracking entry for the specified entity instance and enables
        /// lazy loading on the entity when applicable.
        /// </summary>
        /// <param name="entity">The entity whose tracking entry is requested.</param>
        /// <returns>An <see cref="EntityEntry"/> representing the entity's tracking information.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="entity"/> is null or invalid.</exception>
        public EntityEntry Entry(object entity)
        {
            NormValidator.ValidateEntity(entity, nameof(entity));
            var method = typeof(NavigationPropertyExtensions).GetMethod(nameof(NavigationPropertyExtensions.EnableLazyLoading))!;
            method.MakeGenericMethod(entity.GetType()).Invoke(null, new object[] { entity, this });
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(entity.GetType()));
        }

        /// <summary>
        /// Configures the context to automatically retry failed save operations
        /// when the database reports a deadlock condition.
        /// </summary>
        /// <returns>The current <see cref="DbContextOptions"/> instance for fluent configuration.</returns>
        public DbContextOptions UseDeadlockResilientSaveChanges()
        {
            Options.RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromSeconds(1),
                ShouldRetry = ex => ex is DbException dbEx &&
                    (int?)dbEx.GetType().GetProperty("Number")?.GetValue(dbEx) == 1205
            };
            return Options;
        }
        /// <summary>
        /// Persists all tracked changes to the database using the configured retry policy.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        public Task<int> SaveChangesAsync(CancellationToken ct = default)
            => SaveChangesWithRetryAsync(ct);

        /// <summary>
        /// Invokes <see cref="SaveChangesInternalAsync"/> using the configured retry policy to
        /// transparently retry transient failures such as deadlocks.
        /// </summary>
        /// <param name="ct">Token used to cancel the save operation.</param>
        /// <returns>The number of state entries written to the database.</returns>
        private async Task<int> SaveChangesWithRetryAsync(CancellationToken ct)
        {
            var policy = Options.RetryPolicy;
            var maxRetries = policy?.MaxRetries ?? 1;
            var baseDelay = policy?.BaseDelay ?? TimeSpan.Zero;
            var rand = Random.Shared;
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    return await SaveChangesInternalAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (attempt < maxRetries - 1 && IsRetryableException(ex))
                {
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
        /// <param name="ct">Token used to cancel the save operation.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        private async Task<int> SaveChangesInternalAsync(CancellationToken ct)
        {
            ChangeTracker.DetectChanges();
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
            }

            await using var transactionManager = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
            ct = transactionManager.Token;
            var transaction = transactionManager.Transaction;
            if (transaction is null)
                throw new InvalidOperationException("Transaction cannot be null when creating a CommandScope.");

            var totalAffected = 0;
            try
            {
                foreach (var group in changedEntries.GroupBy(e => (e.State, e.Mapping)))
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

                    for (int start = 0; start < entries.Count; start += batchSize)
                    {
                        var batch = entries.Skip(start).Take(Math.Min(batchSize, entries.Count - start)).ToList();
                        await using var cmd = commandScope.CreateCommand();
                        var sql = new StringBuilder(templateLength * batch.Count);
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
                if (saveInterceptors.Count > 0)
                {
                    foreach (var interceptor in saveInterceptors)
                        await interceptor.SavedChangesAsync(this, changedEntries, totalAffected, ct).ConfigureAwait(false);
                }
                await transactionManager.CommitAsync().ConfigureAwait(false);
            }
            catch
            {
                await transactionManager.RollbackAsync().ConfigureAwait(false);
                throw;
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
            ChangeTracker.Clear();
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
                            map.SetPrimaryKey(entity, newId);
                    }
                    batch[i].AcceptChanges();
                    i++;
                }
                while (await reader.NextResultAsync(ct).ConfigureAwait(false) && i < batch.Count);
                return reader.RecordsAffected;
            }

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            foreach (var entry in batch)
                entry.AcceptChanges();
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
                sql.Append(BuildUpdateBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entry.Entity ?? throw new InvalidOperationException("Entity is null"),
                    WriteOperation.Update, paramIndex);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Update, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var updated = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            if (map.TimestampColumn != null && updated != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            foreach (var entry in batch)
                entry.AcceptChanges();
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
                sql.Append(BuildDeleteBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entry.Entity ?? throw new InvalidOperationException("Entity is null"),
                    WriteOperation.Delete, paramIndex);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Delete, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            if (map.TimestampColumn != null && deleted != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            foreach (var entry in batch)
            {
                if (entry.Entity is { } entityToRemove)
                    ChangeTracker.Remove(entityToRemove, true);
            }
            return deleted;
        }

        private bool IsRetryableException(Exception ex)
        {
            if (ex is DbException dbEx && Options.RetryPolicy != null)
            {
                if (Options.RetryPolicy.ShouldRetry(dbEx))
                    return true;
            }
            return ex is TimeoutException;
        }
        private static int ToSecondsClamped(TimeSpan t) => Math.Max(1, (int)Math.Ceiling(t.TotalSeconds));
        #endregion

        #region Standard CRUD
        public Task<int> InsertAsync<T>(T entity, CancellationToken ct = default) where T : class
            => InsertAsync(entity, null, ct);
        public Task<int> InsertAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
        {
            var result = WriteOptimizedAsync(entity, WriteOperation.Insert, ct, transaction);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return result;
        }
        public Task<int> UpdateAsync<T>(T entity, CancellationToken ct = default) where T : class
            => UpdateAsync(entity, null, ct);
        public Task<int> UpdateAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
            => WriteOptimizedAsync(entity, WriteOperation.Update, ct, transaction);
        public Task<int> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class
            => DeleteAsync(entity, null, ct);
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
            var currentTransaction = transaction ?? await Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
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

                // No unconditional Prepare() here  reduces overhead for single-row ops.

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
            catch
            {
                if (ownsTransaction) await currentTransaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
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
            catch
            {
                await ownTransaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
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

        private string BuildInsertBatch(TableMapping map, int startParamIndex)
        {
            var cols = map.InsertColumns;
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable} DEFAULT VALUES{_p.GetIdentityRetrievalString(map)}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var paramNames = string.Join(", ", cols.Select((c, i) => $"{_p.ParamPrefix}p{startParamIndex + i}"));
            return $"INSERT INTO {map.EscTable} ({colNames}) VALUES ({paramNames}){_p.GetIdentityRetrievalString(map)}";
        }

        private string BuildUpdateBatch(TableMapping map, int startParamIndex)
        {
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
            var where = string.Join(" AND ", whereParts);
            return $"UPDATE {map.EscTable} SET {setSb} WHERE {where}";
        }

        private string BuildDeleteBatch(TableMapping map, int startParamIndex)
        {
            var idx = startParamIndex;
            var whereParts = new List<string>();
            foreach (var col in map.KeyColumns)
                whereParts.Add($"{col.EscCol}={_p.ParamPrefix}p{idx++}");
            if (map.TimestampColumn != null)
                whereParts.Add($"{map.TimestampColumn.EscCol}={_p.ParamPrefix}p{idx++}");
            var where = string.Join(" AND ", whereParts);
            return $"DELETE FROM {map.EscTable} WHERE {where}";
        }

        private int AddParametersBatched(DbCommand cmd, TableMapping map, object entity, WriteOperation operation, int startIndex)
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
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", map.TimestampColumn.Getter(entity));
                    break;
                case WriteOperation.Delete:
                    foreach (var col in map.KeyColumns)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", col.Getter(entity));
                    if (map.TimestampColumn != null)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", map.TimestampColumn.Getter(entity));
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

            if (Options.Logger?.IsEnabled(LogLevel.Debug) != true)
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
            private readonly DbTransaction _transaction;
            public CommandScope(DbConnection connection, DbTransaction transaction)
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
        public Task<List<T>> QueryUnchangedAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, sql);
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                if (!NormValidator.IsSafeRawSql(sql))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");
                NormValidator.ValidateRawSql(sql, paramDict);

                var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanWrite)
                    .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);

                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var fieldCount = reader.FieldCount;
                var fieldNames = new string[fieldCount];
                for (int i = 0; i < fieldCount; i++) fieldNames[i] = reader.GetName(i);

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    var item = new T();
                    for (int i = 0; i < fieldCount; i++)
                    {
                        if (!props.TryGetValue(fieldNames[i], out var prop)) continue;
                        var value = reader.GetValue(i);
                        if (value == DBNull.Value) continue;
                        var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                        try
                        {
                            var converted = targetType.IsEnum ? Enum.ToObject(targetType, value) : Convert.ChangeType(value, targetType);
                            prop.SetValue(item, converted);
                        }
                        catch
                        {
                            prop.SetValue(item, value);
                        }
                    }
                    list.Add(item);
                }

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);

        public Task<List<T>> FromSqlRawAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, sql);
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                if (!NormValidator.IsSafeRawSql(sql))
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

        public Task<List<T>> ExecuteStoredProcedureAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, procedureName);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

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

                if (!IsSafeIdentifier(procedureName))
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

        public async IAsyncEnumerable<T> ExecuteStoredProcedureAsAsyncEnumerable<T>(string procedureName, [EnumeratorCancellation] CancellationToken ct = default, object? parameters = null) where T : class, new()
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            await using var cmd = CommandPool.Get(Connection, procedureName);
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

            if (!IsSafeIdentifier(procedureName))
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

        public Task<StoredProcedureResult<T>> ExecuteStoredProcedureWithOutputAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null, params OutputParameter[] outputParameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.Connection, procedureName);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

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

                var outputParamMap = new Dictionary<string, DbParameter>();
                foreach (var op in outputParameters)
                {
                    var pName = _p.ParamPrefix + op.Name;
                    var p = cmd.CreateParameter();
                    p.ParameterName = pName;
                    p.DbType = op.DbType;
                    p.Direction = ParameterDirection.Output;
                    if (op.Size.HasValue) p.Size = op.Size.Value;
                    cmd.Parameters.Add(p);
                    outputParamMap[op.Name] = p;
                }

                if (!IsSafeIdentifier(procedureName))
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
            if (entityTenant == null)
            {
                if (operation == WriteOperation.Insert)
                {
                    tenantCol.Setter(entity, tenantId);
                }
                else
                {
                    throw new InvalidOperationException("Tenant context required but not available");
                }
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

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _cleanupTimer?.Dispose();
                _providerInitLock?.Dispose();
                lock (_disposablesLock)
                {
                    CleanupDisposablesInternal();
                    for (var node = _disposables.First; node != null;)
                    {
                        var next = node.Next;
                        if (node.Value.TryGetTarget(out var d))
                        {
                            d.Dispose();
                        }
                        _disposables.Remove(node);
                        node = next;
                    }
                }
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
                _cleanupTimer?.Dispose();
                _providerInitLock?.Dispose();
                await CleanupDisposablesAsync();
                if (_cn != null)
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
    public sealed record StoredProcedureResult<T>(List<T> Results, IReadOnlyDictionary<string, object?> OutputParameters);
}
