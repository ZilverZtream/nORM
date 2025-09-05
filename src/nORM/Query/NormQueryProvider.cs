using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Security.Cryptography;
using System.Runtime.CompilerServices;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Navigation;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Query
{
    internal sealed class NormQueryProvider : IQueryProvider
    {
        internal readonly DbContext _ctx;
        private static readonly ConcurrentDictionary<int, QueryPlan> _planCache = new();
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _cacheLocks = new();
        private readonly QueryExecutor _executor;
        private readonly IncludeProcessor _includeProcessor;
        private readonly BulkCudBuilder _cudBuilder;

        public NormQueryProvider(DbContext ctx)
        {
            _ctx = ctx;
            _includeProcessor = new IncludeProcessor(ctx);
            _executor = new QueryExecutor(ctx, _includeProcessor);
            _cudBuilder = new BulkCudBuilder(ctx);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = expression.Type.GetGenericArguments()[0];
            return CreateQueryInternal(elementType, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            var query = CreateQueryInternal(typeof(TElement), expression);
            if (query is IQueryable<TElement> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidOperationException($"Unable to create IQueryable for type '{typeof(TElement)}'.");
        }

        private IQueryable CreateQueryInternal(Type elementType, Expression expression)
        {
            // Check if the type can satisfy the 'new()' constraint
            if (CanUseConstrainedQueryable(elementType))
            {
                // Use the constrained version for regular entity types
                var constrainedQueryableType = typeof(NormQueryableImpl<>).MakeGenericType(elementType);
                return (IQueryable)Activator.CreateInstance(constrainedQueryableType, new object[] { this, expression })!;
            }
            else
            {
                // Use the unconstrained version for anonymous types and other types without parameterless constructors
                var unconstrainedQueryableType = typeof(NormQueryableImplUnconstrained<>).MakeGenericType(elementType);
                return (IQueryable)Activator.CreateInstance(unconstrainedQueryableType, new object[] { this, expression })!;
            }
        }

        private static bool CanUseConstrainedQueryable(Type elementType)
        {
            // Check if type is a class and has a public parameterless constructor
            if (!elementType.IsClass)
                return false;

            // Anonymous types start with '<>' and don't have public parameterless constructors
            if (elementType.Name.StartsWith("<>"))
                return false;

            // Check for public parameterless constructor
            var defaultConstructor = elementType.GetConstructor(Type.EmptyTypes);
            return defaultConstructor != null && defaultConstructor.IsPublic;
        }

        public TResult Execute<TResult>(Expression expression) 
            => ExecuteAsync<TResult>(expression, CancellationToken.None).GetAwaiter().GetResult();
            
        public object? Execute(Expression expression) => Execute<object>(expression);

        public Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
               ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteInternalAsync<TResult>(expression, token), ct)
               : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteInternalAsync<TResult>(expression, token), ct);
        }

        public Task<int> ExecuteDeleteAsync(Expression expression, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteDeleteInternalAsync(expression, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteDeleteInternalAsync(expression, token), ct);
        }

        public Task<int> ExecuteUpdateAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteUpdateInternalAsync(expression, set, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteUpdateInternalAsync(expression, set, token), ct);
        }

        private async Task<TResult> ExecuteInternalAsync<TResult>(Expression expression, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered);

            Func<Task<TResult>> queryExecutorFactory = async () =>
            {
                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                foreach (var p in plan.Parameters) cmd.AddOptimizedParam(p.Key, p.Value);

                object result;
                if (plan.IsScalar)
                {
                    var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw.Elapsed, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull) return default(TResult)!;
                    var resultType = typeof(TResult);
                    result = Convert.ChangeType(scalarResult, Nullable.GetUnderlyingType(resultType) ?? resultType)!;
                }
                else
                {
                    var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw.Elapsed, list.Count);
                    if (plan.SingleResult)
                    {
                        result = plan.MethodName switch
                        {
                            "First" => ((IEnumerable)list).Cast<object>().First(),
                            "FirstOrDefault" => ((IEnumerable)list).Cast<object>().FirstOrDefault(),
                            "Single" => ((IEnumerable)list).Cast<object>().Single(),
                            "SingleOrDefault" => ((IEnumerable)list).Cast<object>().SingleOrDefault(),
                            "ElementAt" => ((IEnumerable)list).Cast<object>().First(),
                            "ElementAtOrDefault" => ((IEnumerable)list).Cast<object>().FirstOrDefault(),
                            "Last" => ((IEnumerable)list).Cast<object>().First(),
                            "LastOrDefault" => ((IEnumerable)list).Cast<object>().FirstOrDefault(),
                            _ => list
                        } ?? (object)list;
                    }
                    else
                    {
                        result = list;
                    }
                }

                return (TResult)result!;
            };

            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                var cacheKey = BuildCacheKeyWithValues<TResult>(filtered, plan.Parameters);
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
            }
            else
            {
                return await queryExecutorFactory().ConfigureAwait(false);
            }
        }

        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledInternalAsync<TResult>(plan, parameters, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteCompiledInternalAsync<TResult>(plan, parameters, token), ct);
        }

        private async Task<TResult> ExecuteCompiledInternalAsync<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();

            Func<Task<TResult>> queryExecutorFactory = async () =>
            {
                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                foreach (var p in parameters) cmd.AddOptimizedParam(p.Key, p.Value);

                object result;
                if (plan.IsScalar)
                {
                    var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, parameters, sw.Elapsed, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull) return default!;
                    var resultType = typeof(TResult);
                    result = Convert.ChangeType(scalarResult, Nullable.GetUnderlyingType(resultType) ?? resultType)!;
                }
                else
                {
                    var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, parameters, sw.Elapsed, list.Count);
                    if (plan.SingleResult)
                    {
                        result = plan.MethodName switch
                        {
                            "First" => ((IEnumerable)list).Cast<object>().First(),
                            "FirstOrDefault" => ((IEnumerable)list).Cast<object>().FirstOrDefault(),
                            "Single" => ((IEnumerable)list).Cast<object>().Single(),
                            "SingleOrDefault" => ((IEnumerable)list).Cast<object>().SingleOrDefault(),
                            "ElementAt" => ((IEnumerable)list).Cast<object>().First(),
                            "ElementAtOrDefault" => ((IEnumerable)list).Cast<object>().FirstOrDefault(),
                            "Last" => ((IEnumerable)list).Cast<object>().First(),
                            "LastOrDefault" => ((IEnumerable)list).Cast<object>().FirstOrDefault(),
                            _ => list
                        } ?? (object)list;
                    }
                    else
                    {
                        result = list;
                    }
                }

                return (TResult)result!;
            };

            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, parameters);
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
            }
            else
            {
                return await queryExecutorFactory().ConfigureAwait(false);
            }
        }

        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, IReadOnlyCollection<string> tables, TimeSpan expiration, Func<Task<TResult>> factory, CancellationToken ct)
        {
            var cache = _ctx.Options.CacheProvider;
            if (cache == null)
                return await factory().ConfigureAwait(false);

            if (cache.TryGet(cacheKey, out TResult? cached))
                return cached!;

            var semaphore = _cacheLocks.GetOrAdd(cacheKey, _ => new SemaphoreSlim(1, 1));
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (cache.TryGet(cacheKey, out cached))
                    return cached!;

                var result = await factory().ConfigureAwait(false);
                cache.Set(cacheKey, result!, expiration, tables);
                return result;
            }
            finally
            {
                semaphore.Release();
                if (semaphore.CurrentCount == 1)
                    _cacheLocks.TryRemove(cacheKey, out _);
            }
        }

        private string BuildCacheKeyFromPlan<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
        {
            var sb = new StringBuilder();
            sb.Append(plan.Sql);
            sb.Append('|').Append(typeof(TResult).FullName);

            var tenant = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            if (_ctx.Options.TenantProvider != null)
            {
                if (tenant == null)
                    throw new InvalidOperationException("Tenant context required but not available");
                sb.Append("|TENANT:").Append(tenant);
            }

            foreach (var kvp in parameters.OrderBy(k => k.Key))
            {
                sb.Append('|').Append(kvp.Key).Append('=').Append(kvp.Value?.GetHashCode() ?? 0);
            }

            using var sha = SHA256.Create();
            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return Convert.ToHexString(sha.ComputeHash(bytes));
        }

        private async Task<int> ExecuteDeleteInternalAsync(Expression expression, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteDeleteAsync only supports single table queries.");

            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);

            _cudBuilder.ValidateCudPlan(plan.Sql);
            var whereClause = _cudBuilder.ExtractWhereClause(plan.Sql, mapping.EscTable);

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            var finalSql = $"DELETE FROM {mapping.EscTable}{whereClause}";
            cmd.CommandText = finalSql;
            foreach (var p in plan.Parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(finalSql, plan.Parameters, sw.Elapsed, affected);
            return affected;
        }

        private async Task<int> ExecuteUpdateInternalAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteUpdateAsync only supports single table queries.");

            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);

            _cudBuilder.ValidateCudPlan(plan.Sql);
            var whereClause = _cudBuilder.ExtractWhereClause(plan.Sql, mapping.EscTable);
            var (setClause, setParams) = _cudBuilder.BuildSetClause(mapping, set);

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            var finalSql = $"UPDATE {mapping.EscTable} SET {setClause}{whereClause}";
            cmd.CommandText = finalSql;
            foreach (var p in plan.Parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            foreach (var p in setParams)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var allParams = plan.Parameters.ToDictionary(k => k.Key, v => v.Value);
            foreach (var p in setParams)
                allParams[p.Key] = p.Value;

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(finalSql, allParams, sw.Elapsed, affected);
            return affected;
        }


        public async IAsyncEnumerable<T> AsAsyncEnumerable<T>(Expression expression, [EnumeratorCancellation] CancellationToken ct = default)
        {
            // Streaming queries bypass the cache to avoid materializing entire result sets in memory.
            var plan = GetPlan(expression, out _);

            var sw = Stopwatch.StartNew();
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            foreach (var p in plan.Parameters) cmd.AddOptimizedParam(p.Key, p.Value);

            if (plan.Includes.Count > 0 || plan.GroupJoinInfo != null)
                throw new NotSupportedException("AsAsyncEnumerable does not support Include or GroupJoin operations.");

            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null;
            if (trackable)
                _ctx.GetMapping(plan.ElementType);

            var count = 0;
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess, ct)
                .ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = (T)await plan.Materializer(reader, ct).ConfigureAwait(false);
                if (trackable)
                {
                    var actualMap = _ctx.GetMapping(entity!.GetType());
                    var entry = _ctx.ChangeTracker.Track(entity!, EntityState.Unchanged, actualMap);
                    entity = (T)entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading((object)entity!, _ctx);
                }
                count++;
                yield return entity;
            }

            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw.Elapsed, count);
        }

        internal QueryPlan GetPlan(Expression expression, out Expression filtered)
        {
            filtered = ApplyGlobalFilters(expression);
            var elementType = GetElementType(UnwrapQueryExpression(filtered));
            var fingerprint = ExpressionFingerprint.Compute(filtered);
            var tenantHash = _ctx.Options.TenantProvider?.GetCurrentTenantId()?.GetHashCode() ?? 0;
            var cacheKey = HashCode.Combine(fingerprint, tenantHash, elementType.GetHashCode(), filtered.Type.GetHashCode());

            var localFiltered = filtered;
            return _planCache.GetOrAdd(cacheKey, _ =>
            {
                using var translator = new QueryTranslator(_ctx);
                return translator.Translate(localFiltered);
            });
        }

        private string BuildCacheKeyWithValues<TResult>(Expression expression, IReadOnlyDictionary<string, object> parameters)
        {
            var sb = new StringBuilder();
            sb.Append(ExpressionFingerprint.Compute(expression));
            sb.Append('|').Append(typeof(TResult).FullName);

            var tenant = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            if (_ctx.Options.TenantProvider != null)
            {
                if (tenant == null)
                    throw new InvalidOperationException("Tenant context required but not available");
                sb.Append("|TENANT:").Append(tenant);
            }

            foreach (var kvp in parameters.OrderBy(k => k.Key))
            {
                sb.Append('|').Append(kvp.Key).Append('=').Append(kvp.Value?.GetHashCode() ?? 0);
            }

            using var sha = SHA256.Create();
            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return Convert.ToHexString(sha.ComputeHash(bytes));
        }

        private static Expression UnwrapQueryExpression(Expression expression)
        {
            return expression is MethodCallExpression mc &&
                   !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                   mc.Arguments.Count > 0
                ? mc.Arguments[0]
                : expression;
        }

        private Expression ApplyGlobalFilters(Expression expression)
        {
            if (expression is MethodCallExpression mc &&
                !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                mc.Arguments.Count > 0)
            {
                var filteredSource = ApplyGlobalFilters(mc.Arguments[0]);
                var args = mc.Arguments.ToArray();
                args[0] = filteredSource;
                return mc.Update(mc.Object, args);
            }

            var entityType = GetElementType(expression);

            if (_ctx.Options.GlobalFilters.Count > 0)
            {
                foreach (var kvp in _ctx.Options.GlobalFilters)
                {
                    if (!kvp.Key.IsAssignableFrom(entityType)) continue;
                    foreach (var filter in kvp.Value)
                    {
                        LambdaExpression lambda;
                        if (filter.Parameters.Count == 2)
                        {
                            var replacer = new ParameterReplacer(filter.Parameters[0], Expression.Constant(_ctx));
                            var body = replacer.Visit(filter.Body)!;
                            lambda = Expression.Lambda(body, filter.Parameters[1]);
                        }
                        else
                        {
                            lambda = filter;
                        }

                        expression = Expression.Call(
                            typeof(Queryable),
                            nameof(Queryable.Where),
                            new[] { entityType },
                            expression,
                            Expression.Quote(lambda));
                    }
                }
            }

            if (_ctx.Options.TenantProvider != null)
            {
                var map = _ctx.GetMapping(entityType);
                var tenantCol = map.Columns.FirstOrDefault(c => c.PropName == _ctx.Options.TenantColumnName);
                if (tenantCol != null)
                {
                    var param = Expression.Parameter(entityType, "t");
                    var prop = Expression.Property(param, tenantCol.Prop.Name);
                    var tenantId = _ctx.Options.TenantProvider.GetCurrentTenantId();
                    var constant = Expression.Constant(tenantId, tenantCol.Prop.PropertyType);
                    var body = Expression.Equal(prop, constant);
                    var lambda = Expression.Lambda(body, param);
                    expression = Expression.Call(
                        typeof(Queryable),
                        nameof(Queryable.Where),
                        new[] { entityType },
                        expression,
                        Expression.Quote(lambda));
                }
            }

            return expression;
        }

        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }

            var iface = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
            if (iface != null) return iface.GetGenericArguments()[0];

            throw new ArgumentException($"Cannot determine element type from expression of type {type}");
        }
    }
}
