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

#nullable enable

namespace nORM.Core
{
    public sealed class DbContext : IDisposable
    {
        private readonly DbConnection _cn;
        private readonly DatabaseProvider _p;
        private readonly ConcurrentDictionary<Type, TableMapping> _m = new();
        private readonly IExecutionStrategy _executionStrategy;
        private readonly ModelBuilder _modelBuilder;

        public DbContextOptions Options { get; }
        public ChangeTracker ChangeTracker { get; } = new();

        public DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options = null)
        {
            _cn = cn;
            _p = p;
            Options = options ?? new DbContextOptions();
            _modelBuilder = new ModelBuilder();
            Options.OnModelCreating?.Invoke(_modelBuilder);

            _executionStrategy = Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(this, Options.RetryPolicy)
                : new DefaultExecutionStrategy(this);

            if (_cn.State != ConnectionState.Open)
                _cn.Open();

            // Apply SQLite optimizations once on connection open
            if (_p is SqliteProvider)
            {
                using var pragmaCmd = _cn.CreateCommand();
                pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
                pragmaCmd.ExecuteNonQuery();
            }
        }

        public DbConnection Connection => _cn;
        public DatabaseProvider Provider => _p;

        public async Task<bool> IsHealthyAsync(CancellationToken ct = default)
        {
            try
            {
                return await _executionStrategy.ExecuteAsync(async (ctx, token) =>
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.CommandTimeout = (int)TimeSpan.FromSeconds(5).TotalSeconds;
                    var result = await cmd.ExecuteScalarAsync(token);
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

        public async Task<int> SaveChangesAsync(CancellationToken ct = default)
        {
            ChangeTracker.DetectChanges();
            var entries = ChangeTracker.Entries.ToList();
            var total = 0;
            foreach (var entry in entries)
            {
                switch (entry.State)
                {
                    case EntityState.Added:
                        total += await InvokeWriteAsync(nameof(InsertAsync), entry, ct);
                        entry.AcceptChanges();
                        break;
                    case EntityState.Modified:
                        total += await InvokeWriteAsync(nameof(UpdateAsync), entry, ct);
                        entry.AcceptChanges();
                        break;
                    case EntityState.Deleted:
                        total += await InvokeWriteAsync(nameof(DeleteAsync), entry, ct);
                        ChangeTracker.Remove(entry.Entity);
                        break;
                }
            }
            return total;
        }

        private Task<int> InvokeWriteAsync(string methodName, EntityEntry entry, CancellationToken ct)
        {
            var method = typeof(DbContext).GetMethod(methodName)!.MakeGenericMethod(entry.Entity.GetType());
            return (Task<int>)method.Invoke(this, new object?[] { entry.Entity, ct })!;
        }
        #endregion

        #region Standard CRUD
        public Task<int> InsertAsync<T>(T entity, CancellationToken ct = default) where T : class
        {
            var result = WriteOptimizedAsync(entity, WriteOperation.Insert, ct);
            
            // Enable lazy loading for the inserted entity
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            
            return result;
        }

        public Task<int> UpdateAsync<T>(T entity, CancellationToken ct = default) where T : class
            => WriteOptimizedAsync(entity, WriteOperation.Update, ct);

        public Task<int> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class
            => WriteOptimizedAsync(entity, WriteOperation.Delete, ct);

        private enum WriteOperation { Insert, Update, Delete }

        private async Task<int> WriteOptimizedAsync<T>(T entity, WriteOperation operation, CancellationToken ct) where T : class
        {
            if (entity is null) throw new ArgumentNullException(nameof(entity));

            var map = GetMapping(typeof(T));
            if (operation == WriteOperation.Insert) SetTenantId(entity, map);

            if (operation == WriteOperation.Insert && Options.RetryPolicy == null)
            {
                return await ExecuteFastInsert(entity, map, ct);
            }

            return await _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await using var transaction = await ctx.Connection.BeginTransactionAsync(token);
                try
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                    
                    cmd.CommandText = operation switch
                    {
                        WriteOperation.Insert => _p.BuildInsert(map),
                        WriteOperation.Update => _p.BuildUpdate(map),
                        WriteOperation.Delete => _p.BuildDelete(map),
                        _ => throw new ArgumentOutOfRangeException(nameof(operation))
                    };

                    AddParametersOptimized(cmd, map, entity, operation);

                    await cmd.PrepareAsync(token);

                    if (operation == WriteOperation.Insert && map.KeyColumns.Any(k => k.IsDbGenerated))
                    {
                        var newId = await cmd.ExecuteScalarAsync(token);
                        if (newId != null && newId != DBNull.Value) map.SetPrimaryKey(entity, newId);
                        await transaction.CommitAsync(token);
                        return 1;
                    }

                    var recordsAffected = await cmd.ExecuteNonQueryAsync(token);
                    if (operation != WriteOperation.Insert && map.TimestampColumn != null && recordsAffected == 0)
                    {
                        throw new DBConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
                    }
                    await transaction.CommitAsync(token);
                    return recordsAffected;
                }
                catch
                {
                    await transaction.RollbackAsync(token);
                    throw;
                }
            }, ct);
        }

        private async Task<int> ExecuteFastInsert<T>(T entity, TableMapping map, CancellationToken ct) where T : class
        {
            await using var transaction = await _cn.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = _cn.CreateCommand();
                cmd.Transaction = transaction;
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
                    var newId = await cmd.ExecuteScalarAsync(ct);
                    if (newId != null && newId != DBNull.Value) map.SetPrimaryKey(entity, newId);
                    await transaction.CommitAsync(ct);
                    return 1;
                }

                var recordsAffected = await cmd.ExecuteNonQueryAsync(ct);
                await transaction.CommitAsync(ct);
                return recordsAffected;
            }
            catch
            {
                await transaction.RollbackAsync(ct);
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
            => _executionStrategy.ExecuteAsync((ctx, token) =>
            {
                var map = GetMapping(typeof(T));
                foreach (var entity in entities) SetTenantId(entity, map);
                return _p.BulkInsertAsync(ctx, map, entities, token);
            }, ct);

        public Task<int> BulkUpdateAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync((ctx, token) => _p.BulkUpdateAsync(ctx, GetMapping(typeof(T)), entities, token), ct);

        public Task<int> BulkDeleteAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
            => _executionStrategy.ExecuteAsync((ctx, token) => _p.BulkDeleteAsync(ctx, GetMapping(typeof(T)), entities, token), ct);
        #endregion

        #region Raw SQL & Stored Procedures
        public Task<List<T>> FromSqlRawAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
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

                var materializer = new Query.QueryTranslator(this).CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token)) list.Add((T)materializer(reader));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                return list;
            }, ct);

        public Task<List<T>> ExecuteStoredProcedureAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null) where T : class, new()
            => _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
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

                var materializer = new Query.QueryTranslator(this).CreateMaterializer(GetMapping(typeof(T)), typeof(T));
                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token)) list.Add((T)materializer(reader));

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

        public void Dispose() => _cn?.Dispose();
    }
}