using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Execution;
using nORM.Mapping;
using nORM.Providers;
using nORM.Internal;
using nORM.Navigation;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

#nullable enable

namespace nORM.Core
{
    public class DbContext : IDisposable
    {
        private readonly DbConnection _cn;
        private readonly DatabaseProvider _p;
        private readonly ConcurrentDictionary<Type, TableMapping> _m = new();
        private readonly IExecutionStrategy _executionStrategy;
        private readonly ModelBuilder _modelBuilder;
        private bool _sqliteInitialized;
        private DbTransaction? _currentTransaction;

        public DbContextOptions Options { get; }
        public ChangeTracker ChangeTracker { get; } = new();
        public DatabaseFacade Database { get; }

        public DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options = null)
        {
            _cn = cn;
            _p = p;
            Options = options ?? new DbContextOptions();
            _modelBuilder = new ModelBuilder();
            Options.OnModelCreating?.Invoke(_modelBuilder);

            Database = new DatabaseFacade(this);

            _executionStrategy = Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(this, Options.RetryPolicy)
                : new DefaultExecutionStrategy(this);
        }

        public DbContext(string connectionString, DatabaseProvider p, DbContextOptions? options = null)
            : this(CreateConnection(connectionString, p), p, options)
        {
        }

        private static DbConnection CreateConnection(string connectionString, DatabaseProvider provider)
        {
            if (provider is SqlServerProvider)
                return new SqlConnection(connectionString);
            if (provider is SqliteProvider)
                return new SqliteConnection(connectionString);
            if (provider is PostgresProvider)
            {
                var type = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
                if (type == null)
                    throw new InvalidOperationException("Npgsql package is required for PostgreSQL support. Please install the Npgsql NuGet package.");
                return (DbConnection)Activator.CreateInstance(type, connectionString)!;
            }
            if (provider is MySqlProvider)
            {
                var type = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector") ??
                           Type.GetType("MySql.Data.MySqlClient.MySqlConnection, MySql.Data");
                if (type == null)
                    throw new InvalidOperationException("MySQL package is required for MySQL support. Please install MySqlConnector or MySql.Data.");
                return (DbConnection)Activator.CreateInstance(type, connectionString)!;
            }

            throw new NotSupportedException($"Unsupported provider type: {provider.GetType().Name}");
        }

        internal async Task<DbConnection> EnsureConnectionAsync(CancellationToken ct = default)
        {
            if (_cn.State != ConnectionState.Open)
                await _cn.OpenAsync(ct);

            if (!_sqliteInitialized && _p is SqliteProvider)
            {
                await using var pragmaCmd = _cn.CreateCommand();
                pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
                await pragmaCmd.ExecuteNonQueryAsync(ct);
                _sqliteInitialized = true;
            }

            return _cn;
        }

        public DbConnection Connection
        {
            get
            {
                EnsureConnectionAsync().GetAwaiter().GetResult();
                return _cn;
            }
        }
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
                    await ctx.EnsureConnectionAsync(token);
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.CommandTimeout = (int)TimeSpan.FromSeconds(5).TotalSeconds;
                    var result = await cmd.ExecuteScalarWithInterceptionAsync(ctx, token);
                    return result is 1 or 1L;
                }, ct);
            }
            catch { return false; }
        }

        internal TableMapping GetMapping(Type t) => _m.GetOrAdd(t, static (k, args) =>
            new TableMapping(k, args.p, args.ctx, args.modelBuilder.GetConfiguration(k)), (p: _p, ctx: this, modelBuilder: _modelBuilder));

        #region Change Tracking
        public EntityEntry Add<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Added, GetMapping(typeof(T)));
        }

        public EntityEntry Attach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(typeof(T)));
        }

        public EntityEntry Update<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            return ChangeTracker.Track(entity, EntityState.Modified, GetMapping(typeof(T)));
        }

        public EntityEntry Remove<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            return ChangeTracker.Track(entity, EntityState.Deleted, GetMapping(typeof(T)));
        }

        public EntityEntry Entry(object entity)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var method = typeof(NavigationPropertyExtensions).GetMethod(nameof(NavigationPropertyExtensions.EnableLazyLoading))!;
            method.MakeGenericMethod(entity.GetType()).Invoke(null, new object[] { entity, this });
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(entity.GetType()));
        }

        public int SaveChanges() => SaveChangesAsync().GetAwaiter().GetResult();

        public Task<int> SaveChangesAsync(CancellationToken ct = default)
            => SaveChangesWithRetryAsync(ct);

        private async Task<int> SaveChangesWithRetryAsync(CancellationToken ct)
        {
            const int maxRetries = 3;
            var baseDelay = TimeSpan.FromMilliseconds(100);

            for (int attempt = 0; attempt < maxRetries - 1; attempt++)
            {
                try
                {
                    return await SaveChangesInternalAsync(ct);
                }
                catch (Exception ex) when (IsRetryableException(ex))
                {
                    var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, attempt));
                    await Task.Delay(delay, ct);
                }
            }

            return await SaveChangesInternalAsync(ct);
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
                    await interceptor.SavingChangesAsync(this, changedEntries, ct);
            }

            var existingTransaction = Database.CurrentTransaction;
            var ownsTransaction = existingTransaction == null;
            DbTransaction transaction;
            CancellationTokenSource? timeoutCts = null;
            CancellationTokenSource? linkedCts = null;

            if (ownsTransaction)
            {
                await EnsureConnectionAsync(ct);
                var isolationLevel = DetermineIsolationLevel(changedEntries);
                transaction = await Connection.BeginTransactionAsync(isolationLevel, ct);

                timeoutCts = new CancellationTokenSource(Options.CommandTimeout);
                linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
                ct = linkedCts.Token;
            }
            else
            {
                transaction = existingTransaction!;
            }

            var totalAffected = 0;
            try
            {
                foreach (var entry in changedEntries)
                {
                    totalAffected += await ProcessEntityChangeAsync(entry, transaction, ct);
                }

                if (ownsTransaction)
                    await transaction.CommitAsync(ct);

                var cache = Options.CacheProvider;
                if (cache != null)
                {
                    var tags = new HashSet<string>();
                    foreach (var entry in changedEntries)
                    {
                        var map = GetMapping(entry.Entity.GetType());
                        tags.Add(map.TableName);
                    }
                    foreach (var tag in tags)
                        cache.InvalidateTag(tag);
                }
            }
            catch
            {
                if (ownsTransaction)
                    await transaction.RollbackAsync(ct);
                throw;
            }
            finally
            {
                if (ownsTransaction)
                    await transaction.DisposeAsync();
                linkedCts?.Dispose();
                timeoutCts?.Dispose();
            }

            if (saveInterceptors.Count > 0)
            {
                foreach (var interceptor in saveInterceptors)
                    await interceptor.SavedChangesAsync(this, changedEntries, totalAffected, ct);
            }

            return totalAffected;
        }

        private async Task<int> ProcessEntityChangeAsync(EntityEntry entry, DbTransaction transaction, CancellationToken ct)
        {
            switch (entry.State)
            {
                case EntityState.Added:
                    var inserted = await InvokeWriteAsync(nameof(InsertAsync), entry, transaction, ct);
                    entry.AcceptChanges();
                    return inserted;
                case EntityState.Modified:
                    var updated = await InvokeWriteAsync(nameof(UpdateAsync), entry, transaction, ct);
                    entry.AcceptChanges();
                    return updated;
                case EntityState.Deleted:
                    var deleted = await InvokeWriteAsync(nameof(DeleteAsync), entry, transaction, ct);
                    ChangeTracker.Remove(entry.Entity);
                    return deleted;
                default:
                    return 0;
            }
        }

        private static IsolationLevel DetermineIsolationLevel(IEnumerable<EntityEntry> entries)
        {
            return entries.Any(e => e.State == EntityState.Deleted)
                ? IsolationLevel.Serializable
                : IsolationLevel.ReadCommitted;
        }

        private static bool IsRetryableException(Exception ex)
        {
            return ex switch
            {
                SqlException sqlEx => sqlEx.Number is 1205 or 1222,
                TimeoutException => true,
                _ => false
            };
        }

        private Task<int> InvokeWriteAsync(string methodName, EntityEntry entry, DbTransaction transaction, CancellationToken ct)
        {
            var method = typeof(DbContext).GetMethods()
                .First(m => m.Name == methodName && m.GetParameters().Length == 3)
                .MakeGenericMethod(entry.Entity.GetType());
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
            if (operation == WriteOperation.Insert) SetTenantId(entity, map);

            var tx = transaction ?? Database.CurrentTransaction;

            if (operation == WriteOperation.Insert && Options.RetryPolicy == null && tx == null)
            {
                return await ExecuteFastInsert(entity, map, ct, null);
            }

            if (tx != null)
            {
                return await WriteWithTransactionAsync(entity, map, operation, tx, ct, ownsTransaction: false);
            }

            return await _executionStrategy.ExecuteAsync((ctx, token) =>
                WriteWithTransactionAsync(entity, map, operation, null, token, ownsTransaction: true), ct);
        }

        private async Task<int> WriteWithTransactionAsync<T>(T entity, TableMapping map, WriteOperation operation, DbTransaction? transaction, CancellationToken ct, bool ownsTransaction) where T : class
        {
            await EnsureConnectionAsync(ct);
            var currentTransaction = transaction ?? await Connection.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = Connection.CreateCommand();
                cmd.Transaction = currentTransaction;
                cmd.CommandTimeout = (int)Options.CommandTimeout.TotalSeconds;

                cmd.CommandText = operation switch
                {
                    WriteOperation.Insert => _p.BuildInsert(map),
                    WriteOperation.Update => _p.BuildUpdate(map),
                    WriteOperation.Delete => _p.BuildDelete(map),
                    _ => throw new ArgumentOutOfRangeException(nameof(operation))
                };

                AddParametersOptimized(cmd, map, entity, operation);

                await cmd.PrepareAsync(ct);

                if (operation == WriteOperation.Insert && map.KeyColumns.Any(k => k.IsDbGenerated))
                {
                    var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct);
                    if (newId != null && newId != DBNull.Value) map.SetPrimaryKey(entity, newId);
                    if (ownsTransaction) await currentTransaction.CommitAsync(ct);
                    return 1;
                }

                var recordsAffected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct);
                if ((operation is WriteOperation.Update or WriteOperation.Delete) &&
                    map.TimestampColumn != null && recordsAffected == 0)
                {
                    throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
                }
                if (ownsTransaction) await currentTransaction.CommitAsync(ct);
                return recordsAffected;
            }
            catch
            {
                if (ownsTransaction) await currentTransaction.RollbackAsync(ct);
                throw;
            }
        }

        private async Task<int> ExecuteFastInsert<T>(T entity, TableMapping map, CancellationToken ct, DbTransaction? transaction) where T : class
        {
            if (transaction != null)
            {
                return await WriteWithTransactionAsync(entity, map, WriteOperation.Insert, transaction, ct, ownsTransaction: false);
            }

            await EnsureConnectionAsync(ct);
            await using var ownTransaction = await _cn.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = _cn.CreateCommand();
                cmd.Transaction = ownTransaction;
                cmd.CommandTimeout = 30;
                cmd.CommandText = _p.BuildInsert(map);

                foreach (var col in map.Columns)
                {
                    if (!col.IsDbGenerated)
                    {
                        var value = col.Getter(entity) ?? DBNull.Value;
                        cmd.AddParam(_p.ParamPrefix + col.PropName, value);
                    }
                }

                await cmd.PrepareAsync(ct);

                if (map.KeyColumns.Any(k => k.IsDbGenerated))
                {
                    var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct);
                    if (newId != null && newId != DBNull.Value) map.SetPrimaryKey(entity, newId);
                    await ownTransaction.CommitAsync(ct);
                    return 1;
                }

                var recordsAffected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct);
                await ownTransaction.CommitAsync(ct);
                return recordsAffected;
            }
            catch
            {
                await ownTransaction.RollbackAsync(ct);
                throw;
            }
        }

        private void AddParametersOptimized<T>(DbCommand cmd, TableMapping map, T entity, WriteOperation operation) where T : class
        {
            switch (operation)
            {
                case WriteOperation.Insert:
                    foreach (var col in map.Columns.Where(c => !c.IsDbGenerated))
                        cmd.AddParam(_p.ParamPrefix + col.PropName, col.Getter(entity));
                    break;
                    
                case WriteOperation.Update:
                    foreach (var col in map.Columns.Where(c => !c.IsKey && !c.IsTimestamp))
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
                await ctx.EnsureConnectionAsync(token);
                var map = GetMapping(typeof(T));
                foreach (var entity in entities) SetTenantId(entity, map);
                return await _p.BulkInsertAsync(ctx, map, entities, token);
            }, ct);

        public Task<int> BulkUpdateAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token);
                return await _p.BulkUpdateAsync(ctx, GetMapping(typeof(T)), entities, token);
            }, ct);

        public Task<int> BulkDeleteAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token);
                return await _p.BulkDeleteAsync(ctx, GetMapping(typeof(T)), entities, token);
            }, ct);
        #endregion

        #region Transaction Savepoints
        public Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction == null)
                throw new InvalidOperationException("No active transaction.");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));

            switch (transaction)
            {
                case SqlTransaction sqlTransaction:
                    sqlTransaction.Save(name);
                    break;
                case SqliteTransaction sqliteTransaction:
                    sqliteTransaction.Save(name);
                    break;
                default:
                    var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) });
                    if (saveMethod != null)
                    {
                        saveMethod.Invoke(transaction, new object[] { name });
                    }
                    else
                    {
                        throw new NotSupportedException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
                    }
                    break;
            }

            return Task.CompletedTask;
        }

        public Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction == null)
                throw new InvalidOperationException("No active transaction.");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));

            switch (transaction)
            {
                case SqlTransaction sqlTransaction:
                    sqlTransaction.Rollback(name);
                    break;
                case SqliteTransaction sqliteTransaction:
                    sqliteTransaction.Rollback(name);
                    break;
                default:
                    var rollbackMethod = transaction.GetType().GetMethod("Rollback", new[] { typeof(string) });
                    if (rollbackMethod != null)
                    {
                        rollbackMethod.Invoke(transaction, new object[] { name });
                    }
                    else
                    {
                        throw new NotSupportedException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
                    }
                    break;
            }

            return Task.CompletedTask;
        }
        #endregion

        #region Raw SQL & Stored Procedures
        public Task<List<T>> QueryUnchangedAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token);
                var sw = Stopwatch.StartNew();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = sql;
                var paramDict = new Dictionary<string, object>();
                for (int i = 0; i < parameters.Length; i++)
                {
                    var pName = $"{_p.ParamPrefix}p{i}";
                    cmd.AddParam(pName, parameters[i]);
                    paramDict[pName] = parameters[i];
                }

                var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanWrite)
                    .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);

                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token);
                var fieldCount = reader.FieldCount;
                var columns = new string[fieldCount];
                for (int i = 0; i < fieldCount; i++) columns[i] = reader.GetName(i);

                while (await reader.ReadAsync(token))
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
                await ctx.EnsureConnectionAsync(token);
                var sw = Stopwatch.StartNew();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = sql;
                var paramDict = new Dictionary<string, object>();
                for (int i = 0; i < parameters.Length; i++)
                {
                    var pName = $"{_p.ParamPrefix}p{i}";
                    cmd.AddParam(pName, parameters[i]);
                    paramDict[pName] = parameters[i];
                }

                var materializer = Query.QueryTranslator.Rent(this).CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token);
                while (await reader.ReadAsync(token)) list.Add((T)await materializer(reader, token));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                return list;
            }, ct);

        public Task<List<T>> ExecuteStoredProcedureAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token);
                var sw = Stopwatch.StartNew();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = procedureName;
                cmd.CommandType = CommandType.StoredProcedure;
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

                var materializer = Query.QueryTranslator.Rent(this).CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token);
                while (await reader.ReadAsync(token)) list.Add((T)await materializer(reader, token));

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
                return list;
            }, ct);
        #endregion

        private void SetTenantId<T>(T entity, TableMapping map) where T : class
        {
            if (Options.TenantProvider == null) return;
            var tenantCol = map.Columns.FirstOrDefault(c => c.PropName == Options.TenantColumnName);
            if (tenantCol != null)
            {
                tenantCol.Setter(entity, Options.TenantProvider.GetCurrentTenantId());
            }
        }

        public void SetShadowProperty(object entity, string name, object? value)
            => Internal.ShadowPropertyStore.Set(entity, name, value);

        public object? GetShadowProperty(object entity, string name)
            => Internal.ShadowPropertyStore.Get(entity, name);

        public void Dispose() => _cn?.Dispose();
    }
}