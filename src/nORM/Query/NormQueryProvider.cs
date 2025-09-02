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

        private async Task<TResult> ExecuteInternalAsync<TResult>(Expression expression, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression);
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
            return (TResult)result!;
        }

        private async Task<IList> MaterializeAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var listType = typeof(List<>).MakeGenericType(plan.ElementType);
            var list = (IList)Activator.CreateInstance(listType)!;

            var trackable = plan.ElementType.IsClass &&
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
                    _ctx.ChangeTracker.Track(entity, EntityState.Unchanged, entityMap!);
                }

                list.Add(entity);
            }

            foreach (var include in plan.Includes)
            {
                await EagerLoadAsync(include, list, ct);
            }

            if (plan.GroupJoinInfo != null && list.Count > 0)
            {
                await EagerLoadGroupJoinAsync(plan.GroupJoinInfo, list, ct);
            }

            return list;
        }

        private async Task EagerLoadGroupJoinAsync(GroupJoinInfo info, IList parents, CancellationToken ct)
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
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                    _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
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

        private async Task EagerLoadAsync(IncludePlan include, IList parents, CancellationToken ct)
        {
            if (parents.Count == 0) return;

            var childMap = _ctx.GetMapping(include.Relation.DependentType);
            var keys = parents.Cast<object>().Select(include.Relation.PrincipalKey.Getter).Where(k => k != null).Distinct().ToList();
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
            cmd.CommandText = $"SELECT * FROM {childMap.EscTable} WHERE {include.Relation.ForeignKey.EscCol} IN ({string.Join(",", paramNames)})";

            var childMaterializer = new QueryTranslator(_ctx).CreateMaterializer(childMap, childMap.Type);
            var children = new List<object>();
            await using (var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default, ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var child = childMaterializer(reader);
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                    _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
                    children.Add(child);
                }
            }

            var childGroups = children.GroupBy(include.Relation.ForeignKey.Getter).ToDictionary(g => g.Key!, g => g.ToList());
            foreach (var p in parents.Cast<object>())
            {
                var pk = include.Relation.PrincipalKey.Getter(p);
                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    var listType = typeof(List<>).MakeGenericType(include.Relation.DependentType);
                    var childList = (IList)Activator.CreateInstance(listType)!;
                    foreach (var item in c) childList.Add(item);
                    include.Relation.NavProp.SetValue(p, childList);
                }
            }
        }

        private QueryPlan GetPlan(Expression expression)
        {
            var filtered = ApplyGlobalFilters(expression);
            var key = new QueryPlanCacheKey(filtered, _ctx.Options.TenantProvider?.GetCurrentTenantId());
            return _planCache.GetOrAdd(key, _ => new QueryTranslator(_ctx).Translate(filtered));
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