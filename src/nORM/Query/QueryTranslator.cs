using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator : ExpressionVisitor, IDisposable
    {
        private DbContext _ctx = null!;
        private SqlClauseBuilder _clauses = new();
        private Dictionary<string, object> _params = new();
        private readonly MaterializerFactory _materializerFactory = new();
        private TableMapping _mapping = null!;
        private Type? _rootType;
        private int _paramIndex;
        private List<string> _compiledParams = new();
        private Dictionary<ParameterExpression, string> _paramMap = new();
        private List<IncludePlan> _includes = new();
        private LambdaExpression? _projection;
        private bool _isAggregate;
        private string _methodName = "";
        private Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _correlatedParams = new();
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value - used in complex join scenarios
        private GroupJoinInfo? _groupJoinInfo;
#pragma warning restore CS0649
        private int _joinCounter;
        private DatabaseProvider _provider = null!;
        private bool _singleResult;
        private bool _noTracking;
        private bool _splitQuery;
        private HashSet<string> _tables = new();
        private TimeSpan _estimatedTimeout;
        private bool _isCacheable;
        private TimeSpan? _cacheExpiration;
        private DateTime? _asOfTimestamp;

        private OptimizedSqlBuilder _sql => _clauses.Sql;
        private OptimizedSqlBuilder _where => _clauses.Where;
        private OptimizedSqlBuilder _having => _clauses.Having;
        private List<(string col, bool asc)> _orderBy => _clauses.OrderBy;
        private List<string> _groupBy => _clauses.GroupBy;
        private int? _take { get => _clauses.Take; set => _clauses.Take = value; }
        private int? _skip { get => _clauses.Skip; set => _clauses.Skip = value; }
        private string? _takeParam { get => _clauses.TakeParam; set => _clauses.TakeParam = value; }
        private string? _skipParam { get => _clauses.SkipParam; set => _clauses.SkipParam = value; }
        private bool _isDistinct { get => _clauses.IsDistinct; set => _clauses.IsDistinct = value; }

        // Initialize _groupJoinInfo in constructor to suppress warning
        // This field is used in complex join scenarios

        private static ThreadLocal<QueryTranslator?> _threadLocalTranslator =
            new(() => null, trackAllValues: false);

        private static readonly AdaptiveQueryComplexityAnalyzer _complexityAnalyzer =
            new AdaptiveQueryComplexityAnalyzer(new SystemMemoryMonitor());

        private QueryTranslator()
        {
        }

        public QueryTranslator(DbContext ctx)
        {
            Reset(ctx);
        }

        internal QueryTranslator(
            DbContext ctx,
            TableMapping mapping,
            Dictionary<string, object> parameters,
            ref int pIndex,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlated,
            HashSet<string> tables,
            List<string> compiledParams,
            Dictionary<ParameterExpression, string> paramMap,
            int joinStart = 0)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _mapping = mapping;
            _rootType = mapping.Type;
            _params = parameters;
            _paramIndex = pIndex;
            _correlatedParams = correlated;
            _tables = tables;
            _compiledParams = compiledParams;
            _paramMap = paramMap;
            _tables.Add(mapping.TableName);
            _joinCounter = joinStart;
        }

        internal static QueryTranslator Rent(DbContext ctx)
        {
            var t = _threadLocalTranslator.Value;
            if (t is null)
            {
                t = new QueryTranslator();
                _threadLocalTranslator.Value = t;
            }
            t.Reset(ctx);
            return t;
        }

        private void Reset(DbContext ctx)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _mapping = null!;
            _rootType = null;
            _paramIndex = 0;
            _compiledParams = new List<string>();
            _paramMap = new Dictionary<ParameterExpression, string>();
            _includes = new List<IncludePlan>();
            _projection = null;
            _isAggregate = false;
            _methodName = string.Empty;
            _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            _groupJoinInfo = null;
            _joinCounter = 0;
            _singleResult = false;
            _noTracking = false;
            _splitQuery = false;
            _tables = new HashSet<string>();
            _params = new Dictionary<string, object>();
            try
            {
                _clauses?.Dispose();
            }
            catch (Exception ex)
            {
                _ctx.Options.Logger?.LogError(ex, "Failed to dispose SqlClauseBuilder");
            }
            finally
            {
                _clauses = new SqlClauseBuilder();
            }
            _estimatedTimeout = ctx.Options.TimeoutConfiguration.BaseTimeout;
            _isCacheable = false;
            _cacheExpiration = null;
            _asOfTimestamp = null;
        }

        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
            => _materializerFactory.CreateMaterializer(mapping, targetType, projection);

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

            throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Cannot determine element type from expression of type {type}"));
        }

        public QueryPlan Translate(Expression e)
        {
            if (e == null) throw new ArgumentNullException(nameof(e));
            if (_ctx == null) throw new InvalidOperationException("QueryTranslator not properly initialized");
            if (_provider == null) throw new InvalidOperationException("Provider not set");

            // Analyze query complexity before translation
            var complexityInfo = _complexityAnalyzer.AnalyzeQuery(e, _ctx.Options);

            if (complexityInfo.WarningMessages.Any())
            {
                var warnings = string.Join("; ", complexityInfo.WarningMessages);
                _ctx.Options.Logger?.LogQuery($"-- WARN: {warnings}", new Dictionary<string, object>(), TimeSpan.Zero, 0);
            }

            var timeoutMultiplier = Math.Max(1.0, complexityInfo.EstimatedCost / 1000.0);
            var adjustedTimeout = TimeSpan.FromMilliseconds(_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalMilliseconds * timeoutMultiplier);
            _estimatedTimeout = adjustedTimeout;

            // Determine root query type and handle TPH discriminator filters
            var rootExpr = UnwrapQueryExpression(e);
            _rootType = GetElementType(rootExpr);
            _mapping = TrackMapping(_rootType);

            // Walk up inheritance hierarchy to find base mapping with discriminator
            var baseType = _rootType.BaseType;
            while (baseType != null && baseType != typeof(object))
            {
                var baseMap = TrackMapping(baseType);
                if (baseMap.DiscriminatorColumn != null)
                {
                    _mapping = baseMap;
                    var discAttr = _rootType.GetCustomAttribute<DiscriminatorValueAttribute>();
                    if (discAttr != null)
                    {
                        var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
                        _params[paramName] = discAttr.Value;
                        _where.Append($"({_mapping.DiscriminatorColumn!.EscCol} = {paramName})");
                    }
                    break;
                }
                baseType = baseType.BaseType;
            }

            Visit(e);

            var materializerType = _projection?.Body.Type ?? _rootType ?? _mapping.Type;
            if (_isAggregate && _groupBy.Count == 0 && (e as MethodCallExpression)?.Method.Name is "Count" or "LongCount")
            {
                materializerType = typeof(int);
            }

            var materializer = _materializerFactory.CreateMaterializer(_mapping, materializerType, _projection);
            var isScalar = _isAggregate && _groupBy.Count == 0;

            if (_sql.Length == 0)
            {
                var fromClause = _mapping.EscTable;
                var alias = _correlatedParams.Count > 0 ? _correlatedParams.Values.First().Alias : null;
                if (_asOfTimestamp.HasValue)
                {
                    alias ??= "T0";
                    var timeParamName = _provider.ParamPrefix + "p" + _paramIndex++;
                    _params[timeParamName] = _asOfTimestamp.Value;
                    var historyTable = _provider.Escape(_mapping.TableName + "_History");
                    var cols = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                    var temporalQuery = $@"
(
    SELECT {cols} FROM {_mapping.EscTable} T1
    WHERE {timeParamName} >= T1.{_provider.Escape("__ValidFrom")} AND {timeParamName} < T1.{_provider.Escape("__ValidTo")}
    UNION ALL
    SELECT {cols} FROM {historyTable} T2
    WHERE {timeParamName} >= T2.{_provider.Escape("__ValidFrom")} AND {timeParamName} < T2.{_provider.Escape("__ValidTo")}
)";
                    fromClause = temporalQuery;
                }

                if (_isAggregate && _groupBy.Count == 0)
                {
                    _sql.AppendFragment("SELECT COUNT(*) FROM ").Append(fromClause);
                    if (alias != null) _sql.Append(' ').Append(alias);
                }
                else
                {
                    var windowFuncs = _clauses.WindowFunctions;
                    if (windowFuncs.Count > 0 && _projection == null)
                        _projection = windowFuncs[^1].ResultSelector;

                    string select;
                    if (windowFuncs.Count > 0 && _projection != null)
                    {
                        var orderByForOverClause = _orderBy.Any()
                            ? $"ORDER BY {string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}"))}"
                            : "ORDER BY (SELECT NULL)";
                        select = BuildSelectWithWindowFunctions(_projection, windowFuncs, orderByForOverClause);
                    }
                    else if (_projection != null)
                    {
                        var selectVisitor = new SelectClauseVisitor(_mapping, _groupBy, _provider);
                        select = selectVisitor.Translate(_projection.Body);
                    }
                    else
                    {
                        select = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                    }

                    var distinct = _isDistinct ? "DISTINCT " : string.Empty;
                    using var prefix = new OptimizedSqlBuilder(select.Length + _mapping.EscTable.Length + 32);
                    prefix.AppendFragment("SELECT ").Append(distinct).Append(select).AppendFragment(" FROM ").Append(fromClause);
                    if (alias != null) prefix.Append(' ').Append(alias);
                    _sql.Insert(0, prefix.ToSqlString());
                }
            }

            if (_where.Length > 0)
            {
                _sql.AppendFragment(" WHERE ").Append(_where.ToSqlString());
            }

            if (_groupBy.Count > 0)
                _sql.AppendFragment(" GROUP BY ").Append(string.Join(", ", _groupBy));
            if (_having.Length > 0)
                _sql.AppendFragment(" HAVING ").Append(_having.ToSqlString());
            if (_orderBy.Count > 0)
                _sql.AppendFragment(" ORDER BY ").Append(string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}")));
            _ctx.Provider.ApplyPaging(_sql.InnerBuilder, _take, _skip, _takeParam, _skipParam);

            var singleResult = _singleResult || _methodName is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault"
                or "ElementAt" or "ElementAtOrDefault" or "Last" or "LastOrDefault" || isScalar;

            var elementType = _groupJoinInfo?.ResultType ?? materializerType;

            var plan = new QueryPlan(_sql.ToString(), _params, _compiledParams, materializer, elementType, isScalar, singleResult, _noTracking, _methodName, _includes, _groupJoinInfo, _tables.ToArray(), _splitQuery, _estimatedTimeout, _isCacheable, _cacheExpiration);
            QueryPlanValidator.Validate(plan, _provider);
            return plan;
        }

        private TableMapping TrackMapping(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            var map = _ctx?.GetMapping(type) ?? throw new InvalidOperationException("Context not available");
            _tables.Add(map.TableName);
            return map;
        }

        private string BuildSelectWithWindowFunctions(LambdaExpression projection, List<WindowFunctionInfo> windowFuncs, string overClause)
        {
            if (projection.Body is not NewExpression ne)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Window function projection must be an anonymous object initializer."));

            var paramMap = windowFuncs.ToDictionary(w => w.ResultParameter, w => w);
            var items = new List<string>();
            for (int i = 0; i < ne.Arguments.Count; i++)
            {
                var arg = ne.Arguments[i];
                var alias = ne.Members![i].Name;
                if (arg is MemberExpression me)
                {
                    var col = _mapping.Columns.First(c => c.Prop.Name == me.Member.Name);
                    items.Add($"{col.EscCol} AS {_provider.Escape(alias)}");
                }
                else if (arg is ParameterExpression p && paramMap.TryGetValue(p, out var wf))
                {
                    var wfSql = BuildWindowFunctionSql(wf, overClause);
                    items.Add($"{wfSql} AS {_provider.Escape(alias)}");
                }
                else
                {
                    var param = projection.Parameters[0];
                    if (!_correlatedParams.TryGetValue(param, out var info))
                    {
                        info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? "T" + _joinCounter);
                        _correlatedParams[param] = info;
                    }
                    var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap);
                    var visitor = ExpressionVisitorPool.Get(in vctx);
                    var sql = visitor.Translate(arg);
                    foreach (var kvp in visitor.GetParameters())
                        _params[kvp.Key] = kvp.Value;
                    ExpressionVisitorPool.Return(visitor);
                    items.Add($"{sql} AS {_provider.Escape(alias)}");
                }
            }
            return string.Join(", ", items);
        }

        private string BuildWindowFunctionSql(WindowFunctionInfo wf, string overClause)
        {
            if (wf.ValueSelector != null)
            {
                var param = wf.ValueSelector.Parameters[0];
                if (!_correlatedParams.TryGetValue(param, out var info))
                {
                    info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? "T" + _joinCounter);
                    _correlatedParams[param] = info;
                }
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap);
                var visitor = ExpressionVisitorPool.Get(in vctx);
                var valueSql = visitor.Translate(wf.ValueSelector.Body);
                foreach (var kvp in visitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                ExpressionVisitorPool.Return(visitor);

                string defaultSql = string.Empty;
                if (wf.DefaultValueSelector != null)
                {
                    var dParam = wf.DefaultValueSelector.Parameters[0];
                    if (!_correlatedParams.TryGetValue(dParam, out info))
                    {
                        info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? "T" + _joinCounter);
                        _correlatedParams[dParam] = info;
                    }
                    var vctx2 = new VisitorContext(_ctx, _mapping, _provider, dParam, info.Alias, _correlatedParams, _compiledParams, _paramMap);
                    var visitor2 = ExpressionVisitorPool.Get(in vctx2);
                    var defSql = visitor2.Translate(wf.DefaultValueSelector.Body);
                    foreach (var kv in visitor2.GetParameters())
                        _params[kv.Key] = kv.Value;
                    ExpressionVisitorPool.Return(visitor2);
                    defaultSql = $", {defSql}";
                }

                var offsetParam = _provider.ParamPrefix + "p" + _paramIndex++;
                _params[offsetParam] = wf.Offset;
                return $"{wf.FunctionName}({valueSql}, {offsetParam}{defaultSql}) OVER ({overClause})";
            }
            return $"{wf.FunctionName}() OVER ({overClause})";
        }

        private string TranslateSubExpression(Expression e)
        {
            using var subTranslator = new QueryTranslator(_ctx, _mapping, _params, ref _paramIndex, _correlatedParams, _tables, _compiledParams, _paramMap, _joinCounter);
            var subPlan = subTranslator.Translate(e);
            _paramIndex = subTranslator._paramIndex;
            return subPlan.Sql;
        }

        private async Task<DateTime> GetTimestampForTagAsync(string tagName, CancellationToken ct = default)
        {
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            var pName = _provider.ParamPrefix + "p0";
            cmd.CommandText = $"SELECT Timestamp FROM __NormTemporalTags WHERE TagName = {pName}";
            cmd.AddParam(pName, tagName);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result == null || result == DBNull.Value)
                throw new NormQueryException($"Tag '{tagName}' not found.");
            return Convert.ToDateTime(result);
        }

        public void Dispose()
        {
            try
            {
                _clauses?.Dispose();
            }
            catch
            {
                // Swallow any exceptions to avoid masking disposal failures
            }
            finally
            {
                // Clear thread-local reference to avoid retaining disposed translators
                if (_threadLocalTranslator.IsValueCreated)
                    _threadLocalTranslator.Value = null;
            }
        }




        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            _methodName = node.Method.Name;
            if (_methodTranslators.TryGetValue(_methodName, out var translator))
            {
                return translator.Translate(this, node);
            }
            return base.VisitMethodCall(node);
        }

        private Expression HandleJoin(MethodCallExpression node, bool isGroupJoin = false)
        {
            // Join signature: Join<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector)
            // node.Arguments[0] = outer query
            // node.Arguments[1] = inner query  
            // node.Arguments[2] = outer key selector
            // node.Arguments[3] = inner key selector
            // node.Arguments[4] = result selector

            if (node.Arguments.Count < 5)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join operation requires 5 arguments"));

            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;

            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join selectors must be lambda expressions"));

            // Visit the outer query first to establish the base table
            Visit(outerQuery);

            // Get the inner table mapping
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);

            // Generate table aliases
            var outerAlias = "T0";

            if (isGroupJoin)
            {
                var outerType = outerKeySelector.Parameters[0].Type;
                var innerType = innerKeySelector.Parameters[0].Type;
                var resultType = resultSelector.Body.Type;

                var innerAliasG = "T" + (++_joinCounter);

                if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                    _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
                var vctxOuterG = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap);
                var outerKeyVisitorG = ExpressionVisitorPool.Get(in vctxOuterG);
                var outerKeySqlG = outerKeyVisitorG.Translate(outerKeySelector.Body);

                if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                    _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAliasG);
                var vctxInnerG = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAliasG, _correlatedParams, _compiledParams, _paramMap);
                var innerKeyVisitorG = ExpressionVisitorPool.Get(in vctxInnerG);
                var innerKeySqlG = innerKeyVisitorG.Translate(innerKeySelector.Body);

                foreach (var kvp in outerKeyVisitorG.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                ExpressionVisitorPool.Return(outerKeyVisitorG);
                foreach (var kvp in innerKeyVisitorG.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                ExpressionVisitorPool.Return(innerKeyVisitorG);

                using var joinSqlG = new OptimizedSqlBuilder(256);
                var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerColumns = innerMapping.Columns.Select(c => $"{innerAliasG}.{c.EscCol}");
                joinSqlG.AppendSelect(ReadOnlySpan<char>.Empty);
                joinSqlG.InnerBuilder.AppendJoin(", ", outerColumns.Concat(innerColumns));
                joinSqlG.Append(' ');
                joinSqlG.Append($"FROM {_mapping.EscTable} {outerAlias} ");
                joinSqlG.Append($"LEFT JOIN {innerMapping.EscTable} {innerAliasG} ON {outerKeySqlG} = {innerKeySqlG}");
                joinSqlG.Append($" ORDER BY {outerKeySqlG}");

                _sql.Clear();
                _sql.Append(joinSqlG.ToSqlString());

                // Reset projection so outer entities are materialized directly
                _projection = null;

                var innerKeyColumn = innerMapping.Columns.FirstOrDefault(c =>
                    ExtractPropertyName(innerKeySelector.Body) == c.PropName);
                if (innerKeyColumn != null)
                {
                    var outerKeyFunc = CreateObjectKeySelector(outerKeySelector);
                    var resultSelectorFunc = CompileGroupJoinResultSelector(resultSelector);
                    _groupJoinInfo = new GroupJoinInfo(
                        outerType,
                        innerType,
                        resultType,
                        outerKeyFunc,
                        innerKeyColumn,
                        resultSelectorFunc
                    );
                }

                return node;
            }

            var innerAlias = "T" + (++_joinCounter);

            var joinType = "INNER JOIN";

            if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap);
            var outerKeyVisitor = ExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);

            if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap);
            var innerKeyVisitor = ExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);

            foreach (var kvp in outerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(outerKeyVisitor);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(innerKeyVisitor);

              if (resultSelector != null)
              {
                  _projection = resultSelector;
                  if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                      _correlatedParams[resultSelector.Parameters[0]] = (_mapping, outerAlias);
                  if (!_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                      _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
              }

              using var joinSql = new OptimizedSqlBuilder(256);

              if (_projection?.Body is NewExpression newExpr)
              {
                  var neededColumns = ExtractNeededColumns(newExpr, _mapping, innerMapping);
                  if (neededColumns.Count == 0)
                  {
                      var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                      var innerColumns = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                      joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                      joinSql.InnerBuilder.AppendJoin(", ", outerColumns.Concat(innerColumns));
                      joinSql.Append(' ');
                  }
                  else
                  {
                      joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                      joinSql.InnerBuilder.AppendJoin(", ", neededColumns);
                      joinSql.Append(' ');
                  }
              }
              else
              {
                  var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                  var innerColumns = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                  joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                  joinSql.InnerBuilder.AppendJoin(", ", outerColumns.Concat(innerColumns));
                  joinSql.Append(' ');
              }

              joinSql.Append($"FROM {_mapping.EscTable} {outerAlias} ");
              joinSql.Append($"{joinType} {innerMapping.EscTable} {innerAlias} ");
              joinSql.Append($"ON {outerKeySql} = {innerKeySql}");

              _sql.Clear();
              _sql.Append(joinSql.ToSqlString());

              return node;
          }

        private Expression HandleGroupJoin(MethodCallExpression node)
        {
            return HandleJoin(node, isGroupJoin: true);
        }

        private Expression HandleSelectMany(MethodCallExpression node)
        {
            // SelectMany can be used in different ways:
            // 1. SelectMany(collectionSelector) - flattens collections
            // 2. SelectMany(collectionSelector, resultSelector) - joins and projects

            if (node.Arguments.Count < 2)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "SelectMany requires at least 2 arguments"));

            var sourceQuery = node.Arguments[0];
            var collectionSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                                   ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Collection selector must be a lambda expression"));

            // Visit the source query first to establish base mapping
            Visit(sourceQuery);

            var outerMapping = _mapping;
            var outerAlias = "T0";

            // Track the outer parameter for correlated references
            if (!_correlatedParams.ContainsKey(collectionSelector.Parameters[0]))
                _correlatedParams[collectionSelector.Parameters[0]] = (outerMapping, outerAlias);

            // Determine if a result selector is provided
            var resultSelector = node.Arguments.Count > 2
                ? StripQuotes(node.Arguments[2]) as LambdaExpression
                : null;

            // Navigation property: treat as INNER JOIN
            if (collectionSelector.Body is MemberExpression memberExpr &&
                outerMapping.Relations.TryGetValue(memberExpr.Member.Name, out var relation))
            {
                var innerMapping = TrackMapping(relation.DependentType);
                var innerAlias = "T" + (++_joinCounter);

                if (resultSelector != null && resultSelector.Parameters.Count > 1 &&
                    !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                {
                    _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
                }

                using var joinSql = new OptimizedSqlBuilder(256);

                if (_projection?.Body is NewExpression newExpr)
                {
                    var neededColumns = ExtractNeededColumns(newExpr, outerMapping, innerMapping);
                    if (neededColumns.Count == 0)
                    {
                        var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                        var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                        joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                        joinSql.InnerBuilder.AppendJoin(", ", outerCols.Concat(innerCols));
                        joinSql.Append(' ');
                    }
                    else
                    {
                        joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                        joinSql.InnerBuilder.AppendJoin(", ", neededColumns);
                        joinSql.Append(' ');
                    }
                }
                else if (resultSelector == null)
                {
                    var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                    joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                    joinSql.InnerBuilder.AppendJoin(", ", innerCols);
                    joinSql.Append(' ');
                }
                else
                {
                    var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                    joinSql.AppendSelect(ReadOnlySpan<char>.Empty);
                    joinSql.InnerBuilder.AppendJoin(", ", outerCols.Concat(innerCols));
                    joinSql.Append(' ');
                }

                joinSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");
                joinSql.Append($"INNER JOIN {innerMapping.EscTable} {innerAlias} ");
                joinSql.Append($"ON {outerAlias}.{relation.PrincipalKey.EscCol} = {innerAlias}.{relation.ForeignKey.EscCol}");

                _sql.Clear();
                _sql.Append(joinSql.ToSqlString());

                if (resultSelector != null)
                {
                    _projection = resultSelector;
                }
                else
                {
                    _mapping = innerMapping;
                }

                return node;
            }

            // Otherwise treat as CROSS JOIN
            var innerType = GetElementType(collectionSelector.Body);
            var crossMapping = TrackMapping(innerType);
            var crossAlias = "T" + (++_joinCounter);

            if (resultSelector != null && resultSelector.Parameters.Count > 1 &&
                !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
            {
                _correlatedParams[resultSelector.Parameters[1]] = (crossMapping, crossAlias);
            }

            using var crossSql = new OptimizedSqlBuilder(256);

            if (_projection?.Body is NewExpression crossNew)
            {
                var neededColumns = ExtractNeededColumns(crossNew, outerMapping, crossMapping);
                if (neededColumns.Count == 0)
                {
                    var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                    crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                    crossSql.InnerBuilder.AppendJoin(", ", outerCols.Concat(innerCols));
                    crossSql.Append(' ');
                }
                else
                {
                    crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                    crossSql.InnerBuilder.AppendJoin(", ", neededColumns);
                    crossSql.Append(' ');
                }
            }
            else if (resultSelector == null)
            {
                var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                crossSql.InnerBuilder.AppendJoin(", ", innerCols);
                crossSql.Append(' ');
            }
            else
            {
                var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                crossSql.InnerBuilder.AppendJoin(", ", outerCols.Concat(innerCols));
                crossSql.Append(' ');
            }

            crossSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");
            crossSql.Append($"CROSS JOIN {crossMapping.EscTable} {crossAlias}");

            _sql.Clear();
            _sql.Append(crossSql.ToSqlString());

            if (resultSelector != null)
            {
                _projection = resultSelector;
            }
            else
            {
                _mapping = crossMapping;
            }

            return node;
        }

        private Expression HandleSetOperation(MethodCallExpression node)
        {
            _isAggregate = true;
            _singleResult = true;

            var source = node.Arguments[0];
            var elementType = source.Type.GetGenericArguments().First();

            if (node.Method.Name == nameof(Queryable.Any) && node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression anyPred)
            {
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(anyPred));
            }
            else if (node.Method.Name == nameof(Queryable.All) && node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression allPred)
            {
                var param = allPred.Parameters[0];
                var notBody = Expression.Not(allPred.Body);
                var notPred = Expression.Lambda(notBody, param);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(notPred));
            }
            else if (node.Method.Name == nameof(Queryable.Contains) && node.Arguments.Count == 2)
            {
                var param = Expression.Parameter(elementType, "x");
                var value = Expression.Convert(node.Arguments[1], elementType);
                var eq = Expression.Equal(param, value);
                var lambda = Expression.Lambda(eq, param);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(lambda));
            }

            using var subTranslator = new QueryTranslator(_ctx, _mapping, _params, ref _paramIndex, _correlatedParams, _tables, _compiledParams, _paramMap, _joinCounter);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator._paramIndex;
            _mapping = subTranslator._mapping;

            using var subSqlBuilder = new OptimizedSqlBuilder();
            var fromIndex = subPlan.Sql.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);
            if (fromIndex >= 0)
            {
                subSqlBuilder.Append("SELECT 1 ");
                subSqlBuilder.Append(subPlan.Sql[fromIndex..]);
            }
            else
            {
                subSqlBuilder.Append(subPlan.Sql);
            }
            var limitParam = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
            _params[limitParam] = 1;
            _ctx.Provider.ApplyPaging(subSqlBuilder.InnerBuilder, 1, null, limitParam, null);

            switch (node.Method.Name)
            {
                case nameof(Queryable.Any):
                case nameof(Queryable.Contains):
                    _sql.Append("SELECT 1 WHERE EXISTS(");
                    _sql.Append(subSqlBuilder.ToSqlString());
                    _sql.Append(")");
                    break;
                case nameof(Queryable.All):
                    _sql.Append("SELECT 1 WHERE NOT EXISTS(");
                    _sql.Append(subSqlBuilder.ToSqlString());
                    _sql.Append(")");
                    break;
            }

            return node;
        }

        private static bool TryGetConstantValue(Expression e, out object? value)
        {
            switch (e)
            {
                case ConstantExpression ce:
                    value = ce.Value;
                    return true;
                case MemberExpression me:
                    if (me.Expression != null && TryGetConstantValue(me.Expression, out var obj))
                    {
                        value = me.Member switch
                        {
                            FieldInfo fi => fi.GetValue(obj),
                            PropertyInfo pi => pi.GetValue(obj),
                            _ => null
                        };
                        return true;
                    }
                    break;
                case MethodCallExpression mce:
                    object? instance = null;
                    if (mce.Object != null && !TryGetConstantValue(mce.Object, out instance))
                    {
                        value = null;
                        return false;
                    }

                    var args = new object?[mce.Arguments.Count];
                    for (int i = 0; i < mce.Arguments.Count; i++)
                    {
                        if (!TryGetConstantValue(mce.Arguments[i], out var argVal))
                        {
                            value = null;
                            return false;
                        }
                        args[i] = argVal;
                    }

                    value = mce.Method.Invoke(instance, args);
                    return true;
            }

            value = null;
            return false;
        }

        private static bool TryGetIntValue(Expression expr, out int value)
        {
            value = 0;
            if (expr is ConstantExpression c && c.Value is int i)
            {
                value = i;
                return true;
            }
            return false;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable q && q.ElementType != null)
            {
                if (_rootType == null || q.ElementType != _rootType)
                {
                    _rootType = q.ElementType;
            _mapping = TrackMapping(q.ElementType);
                }
                return node;
            }

            if (node.Value != null)
            {
                var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
                _params[paramName] = node.Value;
                _sql.Append(paramName);
            }
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (_correlatedParams.ContainsKey(node))
                return base.VisitParameter(node);

            if (_paramMap.TryGetValue(node, out var existing))
            {
                _sql.Append(existing);
                return node;
            }

            var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _sql.Append("(");
            Visit(node.Left);
            _sql.Append(node.NodeType switch
            {
                ExpressionType.Equal => " = ",
                ExpressionType.NotEqual => " <> ",
                ExpressionType.GreaterThan => " > ",
                ExpressionType.GreaterThanOrEqual => " >= ",
                ExpressionType.LessThan => " < ",
                ExpressionType.LessThanOrEqual => " <= ",
                ExpressionType.AndAlso => " AND ",
                ExpressionType.OrElse => " OR ",
                _ => throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, $"Op '{node.NodeType}'"))
            });
            Visit(node.Right);
            _sql.Append(")");
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ParameterExpression pe)
            {
                if (_correlatedParams.TryGetValue(pe, out var info))
                {
                    var col = info.Mapping.Columns.First(c => c.Prop.Name == node.Member.Name);
                    _sql.Append($"{ValidateTableAlias(info.Alias)}.{col.EscCol}");
                }
                else
                {
                    var col = _mapping.Columns.First(c => c.Prop.Name == node.Member.Name);
                    _sql.Append(col.EscCol);
                }
                return node;
            }

            if (TryGetConstantValue(node, out var value))
            {
                var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
                _params[paramName] = value ?? DBNull.Value;
                _sql.Append(paramName);
                return node;
            }

            throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, $"Member '{node.Member.Name}'"));
        }

        private string ValidateTableAlias(string alias)
        {
            if (!IsValidIdentifier(alias))
                throw new NormQueryException($"Invalid table alias: {alias}");
            return alias;
        }

        private static bool IsValidIdentifier(string identifier) =>
            !string.IsNullOrEmpty(identifier) &&
            identifier.All(c => char.IsLetterOrDigit(c) || c == '_') &&
            char.IsLetter(identifier[0]);

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

        private LambdaExpression ExpandProjection(LambdaExpression lambda)
        {
            if (_projection != null &&
                lambda.Parameters.Count == 1 &&
                lambda.Parameters[0].Type == _projection.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(lambda.Parameters[0], _projection.Body).Visit(lambda.Body)!;
                body = new ProjectionMemberReplacer().Visit(body);
                return Expression.Lambda(body, _projection.Parameters);
            }
            return lambda;
        }

        private static Expression UnwrapQueryExpression(Expression expression) =>
            expression is MethodCallExpression mc &&
            !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
            mc.Arguments.Count > 0
                ? mc.Arguments[0]
                : expression;

        private static List<string> ExtractNeededColumns(NewExpression newExpr, TableMapping outerMapping, TableMapping innerMapping)
        {
            var neededColumns = new List<string>();
            var outerAlias = "T0";
            var innerAlias = "T1";

            foreach (var arg in newExpr.Arguments)
            {
                if (arg is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression paramExpr)
                {
                    var isOuterTable = paramExpr.Type == outerMapping.Type;
                    var mapping = isOuterTable ? outerMapping : innerMapping;
                    var alias = isOuterTable ? outerAlias : innerAlias;

                    var column = mapping.Columns.FirstOrDefault(c => c.Prop.Name == memberExpr.Member.Name);
                    if (column != null)
                    {
                        var colSql = $"{alias}.{column.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                }
                else if (arg is ParameterExpression param)
                {
                    var isOuter = param.Type == outerMapping.Type;
                    var mapping = isOuter ? outerMapping : innerMapping;
                    var alias = isOuter ? outerAlias : innerAlias;
                    foreach (var col in mapping.Columns)
                    {
                        var colSql = $"{alias}.{col.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                }
            }

            return neededColumns;
        }

        private static bool IsRecordType(Type type) =>
            type.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.NonPublic) != null;

        private static string? ExtractPropertyName(Expression expression)
        {
            return expression switch
            {
                MemberExpression member => member.Member.Name,
                UnaryExpression { Operand: MemberExpression member } => member.Member.Name,
                _ => null
            };
        }

        private Expression HandleAggregateExpression(MethodCallExpression node)
        {
            // node.Arguments[0] = source query
            // node.Arguments[1] = selector lambda
            // node.Arguments[2] = function name
            
            var sourceQuery = node.Arguments[0];
            var selectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
            var functionConstant = node.Arguments[2] as ConstantExpression;
            
            if (selectorLambda == null || functionConstant?.Value is not string functionName)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Invalid aggregate expression structure"));

            Visit(sourceQuery);
            
            var param = selectorLambda.Parameters[0];
            var alias = "T" + _joinCounter;
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
            var visitor = ExpressionVisitorPool.Get(in vctx);
            var columnSql = visitor.Translate(selectorLambda.Body);

            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(visitor);

            _isAggregate = true;
            _sql.Clear();
            
            var sqlFunction = functionName.ToUpperInvariant();
            if (sqlFunction == "AVERAGE") sqlFunction = "AVG";
            
            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            _sql.AppendAggregateFunction(sqlFunction, columnSql);
            
            return node;
        }

        private Expression HandleGroupBy(MethodCallExpression node)
        {
            // GroupBy(source, keySelector) or GroupBy(source, keySelector, resultSelector)
            var sourceQuery = node.Arguments[0];
            var keySelectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
            
            if (keySelectorLambda == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "GroupBy key selector must be a lambda expression"));

            Visit(sourceQuery);
            
            var param = keySelectorLambda.Parameters[0];
            var alias = "T" + _joinCounter;
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
            var visitor = ExpressionVisitorPool.Get(in vctx2);
            var groupBySql = visitor.Translate(keySelectorLambda.Body);

            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(visitor);

            _groupBy.Add(groupBySql);
            
            // If there's a result selector, handle the projection
            if (node.Arguments.Count > 2)
            {
                var resultSelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
                if (resultSelector != null)
                {
                    _projection = resultSelector;
                    
                    // Clear the default select and let the projection handling rebuild it
                    _sql.Clear();
                    
                    // Analyze the result selector to build appropriate SELECT clause
                    BuildGroupBySelectClause(resultSelector, groupBySql, alias);
                }
            }
            
            return node;
        }

        private void BuildGroupBySelectClause(LambdaExpression resultSelector, string groupBySql, string alias)
        {
            var selectItems = new List<string>();
            
            // Add the grouping key
            selectItems.Add($"{groupBySql} AS GroupKey");
            
            // Analyze the result selector body to find aggregates
            if (resultSelector.Body is NewExpression newExpr)
            {
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                    
                    if (arg is MethodCallExpression methodCall)
                    {
                        var aggregateSql = TranslateGroupAggregateMethod(methodCall, alias);
                        if (aggregateSql != null)
                        {
                            selectItems.Add($"{aggregateSql} AS {memberName}");
                        }
                    }
                    else if (arg is ParameterExpression param && param == resultSelector.Parameters[0])
                    {
                        // This is the key parameter, already added
                        continue;
                    }
                    else
                    {
                        // Try to translate as regular expression
                        if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                            _correlatedParams[resultSelector.Parameters[0]] = (_mapping, alias);
                        var vctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                        var visitor = ExpressionVisitorPool.Get(in vctx);
                        var sql = visitor.Translate(arg);
                        selectItems.Add($"{sql} AS {memberName}");

                        foreach (var kvp in visitor.GetParameters())
                            _params[kvp.Key] = kvp.Value;
                        ExpressionVisitorPool.Return(visitor);
                    }
                }
            }
            
            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            _sql.InnerBuilder.AppendJoin(", ", selectItems);
        }

        private string? TranslateGroupAggregateMethod(MethodCallExpression methodCall, string alias)
        {
            var methodName = methodCall.Method.Name;
            
            // Handle IGrouping<TKey, TElement> methods
            switch (methodName)
            {
                case "Count":
                    return "COUNT(*)";
                case "LongCount":
                    return "COUNT(*)";
                case "Sum":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var visitor = ExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            ExpressionVisitorPool.Return(visitor);
                            return $"SUM({columnSql})";
                        }
                    }
                    return "SUM(*)";
                case "Average":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var visitor = ExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            ExpressionVisitorPool.Return(visitor);
                            return $"AVG({columnSql})";
                        }
                    }
                    return "AVG(*)";
                case "Min":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var visitor = ExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            ExpressionVisitorPool.Return(visitor);
                            return $"MIN({columnSql})";
                        }
                    }
                    return null;
                case "Max":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var visitor = ExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            ExpressionVisitorPool.Return(visitor);
                            return $"MAX({columnSql})";
                        }
                    }
                    return null;
                default:
                    return null;
            }
        }

        private Expression HandleDirectAggregate(MethodCallExpression node)
        {
            // Handle direct aggregate calls like query.Sum(x => x.Amount)
            var sourceQuery = node.Arguments[0];
            
            Visit(sourceQuery);
            
            if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression selector)
            {
                var param = selector.Parameters[0];
                var alias = "T" + _joinCounter;
                if (!_correlatedParams.ContainsKey(param))
                    _correlatedParams[param] = (_mapping, alias);
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
                var visitor = ExpressionVisitorPool.Get(in vctx);
                var columnSql = visitor.Translate(selector.Body);

                foreach (var kvp in visitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                ExpressionVisitorPool.Return(visitor);

                _isAggregate = true;
                _sql.Clear();
                
                var sqlFunction = node.Method.Name.ToUpperInvariant();
                if (sqlFunction == "AVERAGE") sqlFunction = "AVG";
                
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.AppendAggregateFunction(sqlFunction, columnSql);
            }
            
            return node;
        }

        private Expression HandleAllOperation(MethodCallExpression node)
        {
            // ALL is translated as NOT EXISTS with negated predicate
            var sourceQuery = node.Arguments[0];
            var predicate = node.Arguments[1] as LambdaExpression;
            
            if (predicate == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "All operation requires a predicate"));

            Visit(sourceQuery);
            
            // Create negated predicate: NOT (predicate)
            var param = predicate.Parameters[0];
            var alias = "T" + _joinCounter;
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
            var visitor = ExpressionVisitorPool.Get(in vctx2);
            var predicateSql = visitor.Translate(predicate.Body);

            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(visitor);

            // Wrap in NOT EXISTS
            _sql.Insert(0, "SELECT CASE WHEN NOT EXISTS(");
            if (_where.Length > 0)
                _where.Append($" AND NOT ({predicateSql})");
            else
                _where.Append($"NOT ({predicateSql})");
            _sql.Append(") THEN 1 ELSE 0 END");
            
            return node;
        }


        private static Func<object, object> CreateObjectKeySelector(LambdaExpression keySelector)
        {
            var parameterType = keySelector.Parameters[0].Type;
            var returnType = keySelector.ReturnType;

            var objParam = Expression.Parameter(typeof(object), "obj");
            var castParam = Expression.Convert(objParam, parameterType);
            var body = new ParameterReplacer(keySelector.Parameters[0], castParam).Visit(keySelector.Body)!;
            var convertBody = Expression.Convert(body, typeof(object));
            var lambda = Expression.Lambda<Func<object, object>>(convertBody, objParam);

            ExpressionUtils.ValidateExpression(lambda);
            var timeout = ExpressionUtils.GetCompilationTimeout(lambda);
            using var cts = new CancellationTokenSource(timeout);
            var invoker = ExpressionUtils.CompileWithTimeout(lambda, cts.Token);

            return obj =>
            {
                try
                {
                    var result = invoker(obj);
                    if (result == null)
                        return DBNull.Value;
                    if (returnType.IsValueType && result.GetType() != typeof(object))
                        return result;
                    return result;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Error executing key selector for type {parameterType.Name}: {ex.Message}", ex);
                }
            };
        }

        private static Func<object, IEnumerable<object>, object> CompileGroupJoinResultSelector(LambdaExpression resultSelector)
        {
            var outerParam = Expression.Parameter(typeof(object), "outer");
            var innerParam = Expression.Parameter(typeof(IEnumerable<object>), "inners");

            var castOuter = Expression.Convert(outerParam, resultSelector.Parameters[0].Type);
            var innerElementType = resultSelector.Parameters[1].Type.GetGenericArguments()[0];
            var castMethod = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(innerElementType);
            var castInner = Expression.Call(castMethod, innerParam);

            Expression body = resultSelector.Body;
            body = new ParameterReplacer(resultSelector.Parameters[0], castOuter).Visit(body)!;
            body = new ParameterReplacer(resultSelector.Parameters[1], castInner).Visit(body)!;
            body = Expression.Convert(body, typeof(object));

            var lambda = Expression.Lambda<Func<object, IEnumerable<object>, object>>(body, outerParam, innerParam);
            ExpressionUtils.ValidateExpression(lambda);
            var timeout = ExpressionUtils.GetCompilationTimeout(lambda);
            using var cts = new CancellationTokenSource(timeout);
            return ExpressionUtils.CompileWithTimeout(lambda, cts.Token);
        }

        private sealed class ParameterReplacer : ExpressionVisitor
        {
            private readonly ParameterExpression _from;
            private readonly Expression _to;

            public ParameterReplacer(ParameterExpression from, Expression to)
            {
                _from = from;
                _to = to;
            }

            protected override Expression VisitParameter(ParameterExpression node) =>
                node == _from ? _to : base.VisitParameter(node);
        }

        private sealed class ProjectionMemberReplacer : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression is NewExpression newExpr && newExpr.Members != null)
                {
                    for (int i = 0; i < newExpr.Members.Count; i++)
                    {
                        if (newExpr.Members[i].Name == node.Member.Name)
                            return Visit(newExpr.Arguments[i]);
                    }
                }
                return base.VisitMember(node);
            }
        }
    }
}
