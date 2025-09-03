using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Runtime.CompilerServices;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Navigation;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed class NormQueryProvider : IQueryProvider
    {
        internal readonly DbContext _ctx;
        private static readonly ConcurrentLruCache<QueryPlanCacheKey, QueryPlan> _planCache = new(maxSize: 1000);

        public NormQueryProvider(DbContext ctx) => _ctx = ctx;

        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = expression.Type.GetGenericArguments()[0];
            return CreateQueryInternal(elementType, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return (IQueryable<TElement>)CreateQueryInternal(typeof(TElement), expression);
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
            string? cacheKey = null;
            var cache = _ctx.Options.CacheProvider;
            if (cache != null)
            {
                cacheKey = BuildCacheKey<TResult>(filtered, plan.Parameters);
                if (cache.TryGet(cacheKey, out TResult? cached))
                    return cached!;
            }

            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            foreach (var p in plan.Parameters) cmd.AddParam(p.Key, p.Value);

            object result;
            if (plan.IsScalar)
            {
                var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct);
                _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw.Elapsed, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                if (scalarResult == null || scalarResult is DBNull) return default(TResult)!;
                var resultType = typeof(TResult);
                result = Convert.ChangeType(scalarResult, Nullable.GetUnderlyingType(resultType) ?? resultType)!;
            }
            else
            {
                var list = await MaterializeAsync(plan, cmd, ct);
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

            if (cache != null && cacheKey != null)
                cache.Set(cacheKey, (TResult)result!, _ctx.Options.CacheExpiration, plan.Tables);

            return (TResult)result!;
        }

        private async Task<int> ExecuteDeleteInternalAsync(Expression expression, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteDeleteAsync only supports single table queries.");

            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);

            ValidateCudPlan(plan.Sql);
            var whereClause = ExtractWhereClause(plan.Sql, mapping.EscTable);

            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.CommandTimeout.TotalSeconds;
            var finalSql = $"DELETE FROM {mapping.EscTable}{whereClause}";
            cmd.CommandText = finalSql;
            foreach (var p in plan.Parameters)
                cmd.AddParam(p.Key, p.Value);

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct);
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

            ValidateCudPlan(plan.Sql);
            var whereClause = ExtractWhereClause(plan.Sql, mapping.EscTable);
            var (setClause, setParams) = BuildSetClause(mapping, set);

            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.CommandTimeout.TotalSeconds;
            var finalSql = $"UPDATE {mapping.EscTable} SET {setClause}{whereClause}";
            cmd.CommandText = finalSql;
            foreach (var p in plan.Parameters)
                cmd.AddParam(p.Key, p.Value);
            foreach (var p in setParams)
                cmd.AddParam(p.Key, p.Value);

            var allParams = plan.Parameters.ToDictionary(k => k.Key, v => v.Value);
            foreach (var p in setParams)
                allParams[p.Key] = p.Value;

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct);
            _ctx.Options.Logger?.LogQuery(finalSql, allParams, sw.Elapsed, affected);
            return affected;
        }

        private static void ValidateCudPlan(string sql)
        {
            if (sql.IndexOf(" GROUP BY ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                sql.IndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                sql.IndexOf(" HAVING ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                sql.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase) >= 0)
                throw new NotSupportedException("ExecuteUpdate/Delete does not support grouped, ordered, joined or aggregated queries.");
        }

        private string ExtractWhereClause(string sql, string escTable)
        {
            var upper = sql.ToUpperInvariant();
            var fromIndex = upper.IndexOf("FROM " + escTable.ToUpperInvariant(), StringComparison.Ordinal);
            string? alias = null;
            if (fromIndex >= 0)
            {
                var after = sql.Substring(fromIndex + ("FROM " + escTable).Length);
                var tokens = after.TrimStart().Split(new[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length > 0)
                    alias = tokens[0];
            }
            var whereIndex = upper.IndexOf(" WHERE", StringComparison.Ordinal);
            if (whereIndex < 0) return string.Empty;
            var where = sql.Substring(whereIndex);
            if (!string.IsNullOrEmpty(alias))
                where = where.Replace(alias + ".", "");
            return where;
        }

        private (string Sql, Dictionary<string, object> Params) BuildSetClause<T>(TableMapping mapping, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set)
        {
            var assigns = new List<(string Column, object? Value)>();
            var call = set.Body as MethodCallExpression;
            while (call != null)
            {
                var lambda = (LambdaExpression)StripQuotes(call.Arguments[0]);
                var member = (MemberExpression)lambda.Body;
                var column = mapping.Columns.First(c => c.Prop.Name == member.Member.Name).EscCol;
                var value = Expression.Lambda(call.Arguments[1]).Compile().DynamicInvoke();
                assigns.Add((column, value));
                call = call.Object as MethodCallExpression;
            }
            assigns.Reverse();
            var sb = new StringBuilder();
            var parameters = new Dictionary<string, object>();
            for (int i = 0; i < assigns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                var pName = _ctx.Provider.ParamPrefix + "u" + i;
                sb.Append($"{assigns[i].Column} = {pName}");
                parameters[pName] = assigns[i].Value ?? DBNull.Value;
            }
            return (sb.ToString(), parameters);
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote) e = ((UnaryExpression)e).Operand;
            return e;
        }

        public async IAsyncEnumerable<T> AsAsyncEnumerable<T>(Expression expression, [EnumeratorCancellation] CancellationToken ct = default)
        {
            var plan = GetPlan(expression, out var filtered);
            var cache = _ctx.Options.CacheProvider;
            string? cacheKey = null;
            if (cache != null)
            {
                cacheKey = BuildCacheKey<T>(filtered, plan.Parameters);
                if (cache.TryGet(cacheKey, out List<T>? cachedList) && cachedList != null)
                {
                    foreach (var item in cachedList)
                        yield return item;
                    yield break;
                }
            }

            var sw = Stopwatch.StartNew();
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            foreach (var p in plan.Parameters) cmd.AddParam(p.Key, p.Value);

            if (plan.Includes.Count > 0 || plan.GroupJoinInfo != null)
                throw new NotSupportedException("AsAsyncEnumerable does not support Include or GroupJoin operations.");

            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null;
            TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

            var count = 0;
            var cacheList = cache != null ? new List<T>() : null;
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess, ct);
            while (await reader.ReadAsync(ct))
            {
                var entity = (T)plan.Materializer(reader);
                if (trackable)
                {
                    NavigationPropertyExtensions.EnableLazyLoading((object)entity!, _ctx);
                    var actualMap = _ctx.GetMapping(entity!.GetType());
                    _ctx.ChangeTracker.Track(entity!, EntityState.Unchanged, actualMap);
                }
                count++;
                cacheList?.Add(entity);
                yield return entity;
            }

            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw.Elapsed, count);
            if (cache != null && cacheKey != null)
                cache.Set(cacheKey, cacheList ?? new List<T>(), _ctx.Options.CacheExpiration, plan.Tables);
        }

        private async Task<IList> MaterializeAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var listType = typeof(List<>).MakeGenericType(plan.ElementType);
            var list = (IList)Activator.CreateInstance(listType)!;

            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null;
            TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess, ct);
            while (await reader.ReadAsync(ct))
            {
                var entity = plan.Materializer(reader);

                if (trackable)
                {
                    NavigationPropertyExtensions.EnableLazyLoading(entity, _ctx);
                    var actualMap = _ctx.GetMapping(entity.GetType());
                    _ctx.ChangeTracker.Track(entity, EntityState.Unchanged, actualMap);
                }

                list.Add(entity);
            }

            foreach (var include in plan.Includes)
            {
                await EagerLoadAsync(include, list, ct, plan.NoTracking);
            }

            if (plan.GroupJoinInfo != null && list.Count > 0)
            {
                await EagerLoadGroupJoinAsync(plan.GroupJoinInfo, list, ct, plan.NoTracking);
            }

            return list;
        }

        private async Task EagerLoadGroupJoinAsync(GroupJoinInfo info, IList parents, CancellationToken ct, bool noTracking)
        {
            var childMap = _ctx.GetMapping(info.InnerType);
            var keys = parents.Cast<object>().Select(info.OuterKeySelector).Where(k => k != null).Distinct().ToList();
            if (keys.Count == 0) return;

            var paramNames = new List<string>();
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.CommandTimeout.TotalSeconds;
            for (int i = 0; i < keys.Count; i++)
            {
                var paramName = $"{_ctx.Provider.ParamPrefix}fk{i}";
                paramNames.Add(paramName);
                cmd.AddParam(paramName, keys[i]);
            }
            cmd.CommandText = $"SELECT * FROM {childMap.EscTable} WHERE {info.InnerKeyColumn.EscCol} IN ({string.Join(",", paramNames)})";

            var childMaterializer = new QueryTranslator(_ctx).CreateMaterializer(childMap, childMap.Type);
            var children = new List<object>();
            await using (var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default, ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var child = childMaterializer(reader);
                    if (!noTracking)
                    {
                        NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                        _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
                    }
                    children.Add(child);
                }
            }

            var childGroups = children.GroupBy(info.InnerKeySelector).ToDictionary(g => g.Key!, g => g.ToList());

            var linesProperty = info.ResultType.GetProperty(info.CollectionName)!;

            foreach (var p in parents.Cast<object>())
            {
                var pk = info.OuterKeySelector(p);
                var listType = typeof(List<>).MakeGenericType(info.InnerType);
                var childList = (IList)Activator.CreateInstance(listType)!;

                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    foreach (var item in c) childList.Add(item);
                }
                linesProperty.SetValue(p, childList);
            }
        }

        private async Task EagerLoadAsync(IncludePlan include, IList parents, CancellationToken ct, bool noTracking)
        {
            IList current = parents;
            foreach (var relation in include.Path)
            {
                current = await EagerLoadLevelAsync(relation, current, ct, noTracking);
                if (current.Count == 0) break;
            }
        }

        private async Task<IList> EagerLoadLevelAsync(TableMapping.Relation relation, IList parents, CancellationToken ct, bool noTracking)
        {
            var resultChildren = new List<object>();
            if (parents.Count == 0) return resultChildren;

            var childMap = _ctx.GetMapping(relation.DependentType);
            var keys = parents.Cast<object>().Select(relation.PrincipalKey.Getter).Where(k => k != null).Distinct().ToList();
            if (keys.Count == 0) return resultChildren;

            var paramNames = new List<string>();
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.CommandTimeout.TotalSeconds;
            for (int i = 0; i < keys.Count; i++)
            {
                var paramName = $"{_ctx.Provider.ParamPrefix}fk{i}";
                paramNames.Add(paramName);
                cmd.AddParam(paramName, keys[i]);
            }
            cmd.CommandText = $"SELECT * FROM {childMap.EscTable} WHERE {relation.ForeignKey.EscCol} IN ({string.Join(",", paramNames)})";

            var childMaterializer = new QueryTranslator(_ctx).CreateMaterializer(childMap, childMap.Type);
            await using (var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default, ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var child = childMaterializer(reader);
                    if (!noTracking)
                    {
                        NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                        _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
                    }
                    resultChildren.Add(child);
                }
            }

            var childGroups = resultChildren.GroupBy(relation.ForeignKey.Getter).ToDictionary(g => g.Key!, g => g.ToList());
            foreach (var p in parents.Cast<object>())
            {
                var pk = relation.PrincipalKey.Getter(p);
                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    var listType = typeof(List<>).MakeGenericType(relation.DependentType);
                    var childList = (IList)Activator.CreateInstance(listType)!;
                    foreach (var item in c) childList.Add(item);
                    relation.NavProp.SetValue(p, childList);
                }
            }

            return resultChildren;
        }

        private QueryPlan GetPlan(Expression expression, out Expression filtered)
        {
            filtered = ApplyGlobalFilters(expression);
            var elementType = GetElementType(filtered);
            var key = new QueryPlanCacheKey(filtered, _ctx.Options.TenantProvider?.GetCurrentTenantId(), elementType);
            var local = filtered;
            return _planCache.GetOrAdd(key, _ => new QueryTranslator(_ctx).Translate(local));
        }

        private string BuildCacheKey<TResult>(Expression expression, IReadOnlyDictionary<string, object> parameters)
        {
            var hash = new HashCode();
            hash.Add(ExpressionFingerprint.Compute(expression));
            hash.Add(typeof(TResult));
            foreach (var kvp in parameters.OrderBy(k => k.Key))
            {
                hash.Add(kvp.Key);
                hash.Add(kvp.Value?.GetHashCode() ?? 0);
            }
            var tenant = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            if (tenant != null) hash.Add(tenant);
            return hash.ToHashCode().ToString();
        }

        private Expression ApplyGlobalFilters(Expression expression)
        {
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