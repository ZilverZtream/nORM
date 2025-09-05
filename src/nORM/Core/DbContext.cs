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
        private DbTransaction? _currentTransaction;
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

        private static int EstimateQueryComplexity(string? sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return 1;
            var joinCount = Regex.Matches(sql, "\\bJOIN\\b", RegexOptions.IgnoreCase).Count;
            var subQueryCount = Math.Max(0, Regex.Matches(sql, "\\bSELECT\\b", RegexOptions.IgnoreCase).Count - 1);
            return joinCount * 1000 + subQueryCount * 500;
        }

        public DbConnection Connection => EnsureConnection();
        public DatabaseProvider Provider => _p;

        internal DbTransaction? CurrentTransaction
        {
            get => _currentTransaction;
            set => _currentTransaction = value;
        }

        internal void ClearTransaction(DbTransaction transaction)
        {
            if (ReferenceEquals(_currentTransaction, transaction))
                _currentTransaction = null;
        }

        public async Task<bool> IsHealthyAsync(CancellationToken ct = default)
        {
            try
            {
                return await _executionStrategy.ExecuteAsync(async (ctx, token) =>
                {
                    await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.CommandTimeout = (int)TimeSpan.FromSeconds(5).TotalSeconds;
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

        public IQueryable Query(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));

            var entityType = _dynamicTypeCache.GetOrAdd(tableName, t => _typeGenerator.GenerateEntityType(this.Connection, t));

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

        public EntityEntry Entry(object entity)
        {
            NormValidator.ValidateEntity(entity, nameof(entity));
            var method = typeof(NavigationPropertyExtensions).GetMethod(nameof(NavigationPropertyExtensions.EnableLazyLoading))!;
            method.MakeGenericMethod(entity.GetType()).Invoke(null, new object[] { entity, this });
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(entity.GetType()));
        }

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

        public Task<int> SaveChangesAsync(CancellationToken ct = default)
            => SaveChangesWithRetryAsync(ct);

        private async Task<int> SaveChangesWithRetryAsync(CancellationToken ct)
        {
            var policy = Options.RetryPolicy;
            var maxRetries = policy?.MaxRetries ?? 1;
            var baseDelay = policy?.BaseDelay ?? TimeSpan.Zero;

            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    return await SaveChangesInternalAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (attempt < maxRetries - 1 && IsRetryableException(ex))
                {
                    var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, attempt));
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
            }
        }

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

            var totalAffected = 0;
            try
            {
                foreach (var entry in changedEntries)
                {
                    totalAffected += await ProcessEntityChangeAsync(entry, transaction, ct).ConfigureAwait(false);
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

            return totalAffected;
        }

        private async Task<int> ProcessEntityChangeAsync(EntityEntry entry, DbTransaction? transaction, CancellationToken ct)
        {
            switch (entry.State)
            {
                case EntityState.Added:
                    var inserted = await InvokeWriteAsync(nameof(InsertAsync), entry, transaction, ct).ConfigureAwait(false);
                    entry.AcceptChanges();
                    return inserted;
                case EntityState.Modified:
                    var updated = await InvokeWriteAsync(nameof(UpdateAsync), entry, transaction, ct).ConfigureAwait(false);
                    entry.AcceptChanges();
                    return updated;
                case EntityState.Deleted:
                    var deleted = await InvokeWriteAsync(nameof(DeleteAsync), entry, transaction, ct).ConfigureAwait(false);
                      if (entry.Entity is { } entityToRemove)
                          ChangeTracker.Remove(entityToRemove, true);
                    return deleted;
                default:
                    return 0;
            }
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

        private Task<int> InvokeWriteAsync(string methodName, EntityEntry entry, DbTransaction? transaction, CancellationToken ct)
        {
            var method = typeof(DbContext).GetMethods()
                .First(m => m.Name == methodName && m.GetParameters().Length == 3)
                  .MakeGenericMethod((entry.Entity ?? throw new InvalidOperationException("Entity is null")).GetType());
            return (Task<int>)method.Invoke(this, new object?[] { entry.Entity, transaction, ct })!;
        }
        #endregion

        #region Standard CRUD
        public Task<int> InsertAsync<T>(T entity, CancellationToken ct = default) where T : class
            => InsertAsync(entity, null, ct);

        public Task<int> InsertAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
        {
            var result = WriteOptimizedAsync(entity, WriteOperation.Insert, ct, transaction);

            // Enable lazy loading for the inserted entity
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
            if (entity is null) throw new ArgumentNullException(nameof(entity));

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
            try
            {
                await using var cmd = Connection.CreateCommand();
                cmd.Transaction = currentTransaction;

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
                cmd.CommandTimeout = (int)GetAdaptiveTimeout(opType, cmd.CommandText).TotalSeconds;

                AddParametersOptimized(cmd, map, entity, operation);

                await cmd.PrepareAsync(ct).ConfigureAwait(false);

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
            try
            {
                await using var cmd = _cn.CreateCommand();
                cmd.Transaction = ownTransaction;
                cmd.CommandText = _p.BuildInsert(map);
                cmd.CommandTimeout = (int)GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, cmd.CommandText).TotalSeconds;

                foreach (var col in map.Columns)
                {
                    if (!col.IsDbGenerated)
                    {
                        var value = col.Getter(entity) ?? DBNull.Value;
                        cmd.AddParam(_p.ParamPrefix + col.PropName, value);
                    }
                }

                await cmd.PrepareAsync(ct).ConfigureAwait(false);

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
        public Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction == null)
                throw new InvalidOperationException("No active transaction.");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));

            return _p.CreateSavepointAsync(transaction, name, ct);
        }

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
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandTimeout = (int)ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText).TotalSeconds;
                var paramDict = ParameterHelper.AddParameters(_p, cmd, parameters);

                if (!NormValidator.IsSafeRawSql(sql))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");

                NormValidator.ValidateRawSql(sql, paramDict);

                var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanWrite)
                    .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);

                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var fieldCount = reader.FieldCount;
                var columns = new string[fieldCount];
                for (int i = 0; i < fieldCount; i++) columns[i] = reader.GetName(i);

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    var item = new T();
                    for (int i = 0; i < fieldCount; i++)
                    {
                        if (!props.TryGetValue(columns[i], out var prop)) continue;
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
                return list;
            }, ct);

        public Task<List<T>> FromSqlRawAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandTimeout = (int)ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText).TotalSeconds;
                var paramDict = ParameterHelper.AddParameters(_p, cmd, parameters);

                if (!NormValidator.IsSafeRawSql(sql))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");

                NormValidator.ValidateRawSql(sql, paramDict);

                using var translator = global::nORM.Query.QueryTranslator.Rent(this);
                var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false)) list.Add((T)await materializer(reader, token).ConfigureAwait(false));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                return list;
            }, ct);

        public Task<List<T>> ExecuteStoredProcedureAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandText = procedureName;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandTimeout = (int)ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText).TotalSeconds;
                var paramDict = new Dictionary<string, object>();
                if (parameters != null)
                {
                    foreach (var prop in parameters.GetType().GetProperties())
                    {
                        var pName = _p.ParamPrefix + prop.Name;
                        var pValue = prop.GetValue(parameters);
                        cmd.AddParam(pName, pValue);
                        paramDict[pName] = pValue ?? DBNull.Value;
                    }
                }

                if (!NormValidator.IsSafeRawSql(procedureName))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");

                NormValidator.ValidateRawSql(procedureName, paramDict);

                using var translator = global::nORM.Query.QueryTranslator.Rent(this);
                var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false)) list.Add((T)await materializer(reader, token).ConfigureAwait(false));

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
                return list;
            }, ct);

        public async IAsyncEnumerable<T> ExecuteStoredProcedureAsAsyncEnumerable<T>(string procedureName, [EnumeratorCancellation] CancellationToken ct = default, object? parameters = null) where T : class, new()
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            await using var cmd = Connection.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = _p.StoredProcedureCommandType;
            cmd.CommandTimeout = (int)GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText).TotalSeconds;
            var paramDict = new Dictionary<string, object>();
            if (parameters != null)
            {
                foreach (var prop in parameters.GetType().GetProperties())
                {
                    var pName = _p.ParamPrefix + prop.Name;
                    var pValue = prop.GetValue(parameters);
                    cmd.AddParam(pName, pValue);
                    paramDict[pName] = pValue ?? DBNull.Value;
                }
            }

            if (!NormValidator.IsSafeRawSql(procedureName))
                throw new NormUsageException("Potential SQL injection detected in raw query.");

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
        }

        public Task<StoredProcedureResult<T>> ExecuteStoredProcedureWithOutputAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null, params OutputParameter[] outputParameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandText = procedureName;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandTimeout = (int)ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText).TotalSeconds;
                var paramDict = new Dictionary<string, object>();
                if (parameters != null)
                {
                    foreach (var prop in parameters.GetType().GetProperties())
                    {
                        var pName = _p.ParamPrefix + prop.Name;
                        var pValue = prop.GetValue(parameters);
                        cmd.AddParam(pName, pValue);
                        paramDict[pName] = pValue ?? DBNull.Value;
                    }
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

                if (!NormValidator.IsSafeRawSql(procedureName))
                    throw new NormUsageException("Potential SQL injection detected in raw query.");

                NormValidator.ValidateRawSql(procedureName, paramDict);

                using var translator = global::nORM.Query.QueryTranslator.Rent(this);
                var materializer = translator.CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false)) list.Add((T)await materializer(reader, token).ConfigureAwait(false));
                await reader.DisposeAsync().ConfigureAwait(false);

                var outputs = new Dictionary<string, object?>();
                foreach (var kv in outputParamMap)
                {
                    outputs[kv.Key] = kv.Value.Value == DBNull.Value ? null : kv.Value.Value;
                }

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
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

        public void SetShadowProperty(object entity, string name, object? value)
            => Internal.ShadowPropertyStore.Set(entity, name, value);

        public object? GetShadowProperty(object entity, string name)
            => Internal.ShadowPropertyStore.Get(entity, name);

        public async Task CreateTagAsync(string tagName)
        {
            await _executionStrategy.ExecuteAsync(async (ctx, ct) =>
            {
                await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = ctx.Connection.CreateCommand();
                var p0 = _p.ParamPrefix + "p0";
                var p1 = _p.ParamPrefix + "p1";
                cmd.CommandText = $"INSERT INTO __NormTemporalTags (TagName, Timestamp) VALUES ({p0}, {p1})";
                cmd.AddParam(p0, tagName);
                cmd.AddParam(p1, DateTime.UtcNow);
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                return 0;
            }, default).ConfigureAwait(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _cleanupTimer?.Dispose();
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

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _cleanupTimer?.Dispose();
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

                if (_cn != null)
                {
                    await _cn.DisposeAsync().ConfigureAwait(false);
                }

                _disposed = true;
            }

            GC.SuppressFinalize(this);
        }
    }

    public sealed record OutputParameter(string Name, DbType DbType, int? Size = null);

    public sealed record StoredProcedureResult<T>(List<T> Results, IReadOnlyDictionary<string, object?> OutputParameters);
}