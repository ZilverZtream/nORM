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
using Microsoft.Extensions.ObjectPool;
using System.Text;
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
        private SqlBuilder _clauses = new();
        private readonly object _syncRoot = new();
        private readonly MaterializerFactory _materializerFactory = new();
        private TableMapping _mapping = null!;
        private Type? _rootType;
        private readonly ParameterManager _parameterManager = new();
        private Dictionary<string, object> _params { get => _parameterManager.Parameters; set => _parameterManager.Parameters = value; }
        private List<string> _compiledParams { get => _parameterManager.CompiledParameters; set => _parameterManager.CompiledParameters = value; }
        private Dictionary<ParameterExpression, string> _paramMap { get => _parameterManager.ParameterMap; set => _parameterManager.ParameterMap = value; }
        internal int ParameterIndex => _parameterManager.Index;
        private List<IncludePlan> _includes = new();
        private LambdaExpression? _projection;
        private bool _isAggregate;
        private string _methodName = "";
        private Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _correlatedParams = new();
        private Dictionary<string, (TableMapping Mapping, string Alias, string ColumnName)> _projectionPropertyMap = new();
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

        private const int MaxRecursionDepth = 100;
        private int _recursionDepth;

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

        private static readonly ObjectPool<QueryTranslator> _translatorPool =
            new DefaultObjectPool<QueryTranslator>(new QueryTranslatorPooledObjectPolicy());

        private static readonly ObjectPool<List<string>> _selectItemsPool =
            new DefaultObjectPool<List<string>>(new ListPooledObjectPolicy<string>());

        private static readonly AdaptiveQueryComplexityAnalyzer _complexityAnalyzer =
            new AdaptiveQueryComplexityAnalyzer(new SystemMemoryMonitor());

        private QueryTranslator()
        {
        }

        public QueryTranslator(DbContext ctx)
        {
            Reset(ctx);
        }

        private QueryTranslator(
            DbContext ctx,
            TableMapping mapping,
            Dictionary<string, object> parameters,
            int pIndex,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlated,
            HashSet<string> tables,
            List<string> compiledParams,
            Dictionary<ParameterExpression, string> paramMap,
            int joinStart = 0,
            int recursionDepth = 0)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _mapping = mapping;
            _rootType = mapping.Type;
            _params = parameters;
            _parameterManager.Index = pIndex;
            _correlatedParams = correlated;
            _tables = tables;
            _compiledParams = compiledParams;
            _paramMap = paramMap;
            _tables.Add(mapping.TableName);
            _joinCounter = joinStart;
            _recursionDepth = recursionDepth;
        }

        internal static QueryTranslator Create(
            DbContext ctx,
            TableMapping mapping,
            Dictionary<string, object> parameters,
            int pIndex,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlated,
            HashSet<string> tables,
            List<string> compiledParams,
            Dictionary<ParameterExpression, string> paramMap,
            int joinStart = 0,
            int recursionDepth = 0)
            => new QueryTranslator(ctx, mapping, parameters, pIndex, correlated, tables, compiledParams, paramMap, joinStart, recursionDepth);

        internal static QueryTranslator Rent(DbContext ctx)
        {
            var t = _translatorPool.Get();
            t.Reset(ctx);
            return t;
        }

        private void Reset(DbContext ctx)
        {
            lock (_syncRoot)
            {
                _ctx = ctx;
                _provider = ctx.Provider;
                _mapping = null!;
                _rootType = null;
                _parameterManager.Reset();
                _includes = new List<IncludePlan>();
                _projection = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
                _projectionPropertyMap = new Dictionary<string, (TableMapping Mapping, string Alias, string ColumnName)>();
                _groupJoinInfo = null;
                _joinCounter = 0;
                _recursionDepth = 0;
                _singleResult = false;
                _noTracking = false;
                _splitQuery = false;
                _tables = new HashSet<string>();
                _clauses?.Dispose();
                _clauses = new SqlBuilder();
                _estimatedTimeout = ctx.Options.TimeoutConfiguration.BaseTimeout;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
            }
        }

        private void Clear()
        {
            lock (_syncRoot)
            {
                _clauses?.Dispose();
                _clauses = new SqlBuilder();

                _ctx = null!;
                _provider = null!;
                _mapping = null!;
                _rootType = null;
                _parameterManager.Reset();
                _includes = new List<IncludePlan>();
                _projection = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
                _projectionPropertyMap = new Dictionary<string, (TableMapping Mapping, string Alias, string ColumnName)>();
                _groupJoinInfo = null;
                _joinCounter = 0;
                _recursionDepth = 0;
                _singleResult = false;
                _noTracking = false;
                _splitQuery = false;
                _tables = new HashSet<string>();
                _estimatedTimeout = default;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
            }
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
            lock (_syncRoot)
            {
                return new TranslationBuilder(this, e)
                    .Validate()
                    .Setup()
                    .Generate();
            }
        }

        private sealed class TranslationBuilder
        {
            private readonly QueryTranslator _t;
            private readonly Expression _expression;

            public TranslationBuilder(QueryTranslator translator, Expression expression)
            {
                _t = translator;
                _expression = expression;
            }

            public TranslationBuilder Validate()
            {
                if (_expression == null) throw new ArgumentNullException(nameof(_expression));
                if (_t._ctx == null) throw new InvalidOperationException("QueryTranslator not properly initialized");
                if (_t._provider == null) throw new InvalidOperationException("Provider not set");

                var complexityInfo = _complexityAnalyzer.AnalyzeQuery(_expression, _t._ctx.Options);
                if (complexityInfo.WarningMessages.Any())
                {
                    var warnings = string.Join("; ", complexityInfo.WarningMessages);
                    _t._ctx.Options.Logger?.LogQuery($"-- WARN: {warnings}", new Dictionary<string, object>(), TimeSpan.Zero, 0);
                }

                var timeoutMultiplier = Math.Max(1.0, complexityInfo.EstimatedCost / 1000.0);
                var adjustedTimeout = TimeSpan.FromMilliseconds(_t._ctx.Options.TimeoutConfiguration.BaseTimeout.TotalMilliseconds * timeoutMultiplier);
                _t._estimatedTimeout = adjustedTimeout;

                return this;
            }

            public TranslationBuilder Setup()
            {
                var rootExpr = UnwrapQueryExpression(_expression);
                _t._rootType = GetElementType(rootExpr);
                _t._mapping = _t.TrackMapping(_t._rootType);

                var baseType = _t._rootType.BaseType;
                while (baseType != null && baseType != typeof(object))
                {
                    var baseMap = _t.TrackMapping(baseType);
                    if (baseMap.DiscriminatorColumn != null)
                    {
                        _t._mapping = baseMap;
                        var discAttr = _t._rootType.GetCustomAttribute<DiscriminatorValueAttribute>();
                        if (discAttr != null)
                        {
                            var paramName = _t._ctx.Provider.ParamPrefix + "p" + _t._parameterManager.GetNextIndex();
                            _t._params[paramName] = discAttr.Value;
                            _t._where.Append($"({_t._mapping.DiscriminatorColumn!.EscCol} = {paramName})");
                        }
                        break;
                    }
                    baseType = baseType.BaseType;
                }

                return this;
            }

            public QueryPlan Generate()
            {
                _t.Visit(_expression);

                var materializerType = _t._projection?.Body.Type ?? _t._rootType ?? _t._mapping.Type;
                if (_t._isAggregate && _t._groupBy.Count == 0 && (_expression as MethodCallExpression)?.Method.Name is "Count" or "LongCount")
                {
                    materializerType = typeof(int);
                }

                var materializer = _t._materializerFactory.CreateMaterializer(_t._mapping, materializerType, _t._projection);
                var isScalar = _t._isAggregate && _t._groupBy.Count == 0;

                if (_t._sql.Length == 0)
                {
                    var fromClause = _t._mapping.EscTable;
                    var alias = _t._correlatedParams.Count > 0 ? _t._correlatedParams.Values.First().Alias : null;
                    if (_t._asOfTimestamp.HasValue)
                    {
                        alias ??= _t.EscapeAlias("T0");
                        var timeParamName = _t._provider.ParamPrefix + "p" + _t._parameterManager.GetNextIndex();
                        _t._params[timeParamName] = _t._asOfTimestamp.Value;
                        var historyTable = _t._provider.Escape(_t._mapping.TableName + "_History");
                        var cols = PooledStringBuilder.Join(_t._mapping.Columns.Select(c => c.EscCol));
                        var t1 = _t.EscapeAlias("T1");
                        var t2 = _t.EscapeAlias("T2");
                        var temporalQuery = $@"
(
    SELECT {cols} FROM {_t._mapping.EscTable} {t1}
    WHERE {timeParamName} >= {t1}.{_t._provider.Escape("__ValidFrom")} AND {timeParamName} < {t1}.{_t._provider.Escape("__ValidTo")}
    UNION ALL
    SELECT {cols} FROM {historyTable} {t2}
    WHERE {timeParamName} >= {t2}.{_t._provider.Escape("__ValidFrom")} AND {timeParamName} < {t2}.{_t._provider.Escape("__ValidTo")}
)";
                        fromClause = temporalQuery;
                    }

                    if (_t._isAggregate && _t._groupBy.Count == 0)
                    {
                        _t._sql.AppendFragment("SELECT COUNT(*) FROM ").Append(fromClause);
                        if (alias != null) _t._sql.Append(' ').Append(alias);
                    }
                    else
                    {
                        var windowFuncs = _t._clauses.WindowFunctions;
                        if (windowFuncs.Count > 0 && _t._projection == null)
                            _t._projection = windowFuncs[^1].ResultSelector;

                        string select;
                        if (windowFuncs.Count > 0 && _t._projection != null)
                        {
                            var orderByForOverClause = _t._orderBy.Any()
                                ? $"ORDER BY {PooledStringBuilder.JoinOrderBy(_t._orderBy)}"
                                : "ORDER BY (SELECT NULL)";
                            select = _t.BuildSelectWithWindowFunctions(_t._projection, windowFuncs, orderByForOverClause);
                        }
                        else if (_t._projection != null)
                        {
                            var selectVisitor = new SelectClauseVisitor(_t._mapping, _t._groupBy, _t._provider);
                            select = selectVisitor.Translate(_t._projection.Body);
                        }
                        else
                        {
                            select = PooledStringBuilder.Join(_t._mapping.Columns.Select(c => c.EscCol));
                        }

                        var distinct = _t._isDistinct ? "DISTINCT " : string.Empty;
                        using var prefix = new OptimizedSqlBuilder(select.Length + _t._mapping.EscTable.Length + 32);
                        prefix.AppendFragment("SELECT ").Append(distinct).Append(select).AppendFragment(" FROM ").Append(fromClause);
                        if (alias != null) prefix.Append(' ').Append(alias);
                        _t._sql.Insert(0, prefix.ToSqlString());
                    }
                }

                if (_t._where.Length > 0)
                {
                    _t._sql.AppendFragment(" WHERE ").Append(_t._where.ToSqlString());
                }

                if (_t._groupBy.Count > 0)
                    _t._sql.AppendFragment(" GROUP BY ").Append(PooledStringBuilder.Join(_t._groupBy));
                if (_t._having.Length > 0)
                    _t._sql.AppendFragment(" HAVING ").Append(_t._having.ToSqlString());
                if (_t._orderBy.Count > 0)
                    _t._sql.AppendFragment(" ORDER BY ").Append(PooledStringBuilder.JoinOrderBy(_t._orderBy));
                _t._ctx.Provider.ApplyPaging(_t._sql.InnerBuilder, _t._take, _t._skip, _t._takeParam, _t._skipParam);

                var singleResult = _t._singleResult || _t._methodName is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault"
                    or "ElementAt" or "ElementAtOrDefault" or "Last" or "LastOrDefault" || isScalar;

                var elementType = _t._groupJoinInfo?.ResultType ?? materializerType;

                var plan = new QueryPlan(_t._sql.ToString(), _t._params, _t._compiledParams, materializer, elementType, isScalar, singleResult, _t._noTracking, _t._methodName, _t._includes, _t._groupJoinInfo, _t._tables.ToArray(), _t._splitQuery, _t._estimatedTimeout, _t._isCacheable, _t._cacheExpiration);
                QueryPlanValidator.Validate(plan, _t._provider);
                _t._projectionPropertyMap.Clear();
                return plan;
            }
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
            var sb = PooledStringBuilder.Rent();
            try
            {
                for (int i = 0; i < ne.Arguments.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    var arg = ne.Arguments[i];
                    var alias = ne.Members![i].Name;
                    if (arg is MemberExpression me)
                    {
                        var col = _mapping.ColumnsByName[me.Member.Name];
                        sb.Append(col.EscCol).Append(" AS ").Append(_provider.Escape(alias));
                    }
                    else if (arg is ParameterExpression p && paramMap.TryGetValue(p, out var wf))
                    {
                        var wfSql = BuildWindowFunctionSql(wf, overClause);
                        sb.Append(wfSql).Append(" AS ").Append(_provider.Escape(alias));
                    }
                    else
                    {
                        var param = projection.Parameters[0];
                        if (!_correlatedParams.TryGetValue(param, out var info))
                        {
                            info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
                            _correlatedParams[param] = info;
                        }
                        var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap);
                        var visitor = ExpressionVisitorPool.Get(in vctx);
                        var sql = visitor.Translate(arg);
                        foreach (var kvp in visitor.GetParameters())
                            _params[kvp.Key] = kvp.Value;
                        ExpressionVisitorPool.Return(visitor);
                        sb.Append(sql).Append(" AS ").Append(_provider.Escape(alias));
                    }
                }
                return sb.ToString();
            }
            finally
            {
                PooledStringBuilder.Return(sb);
            }
        }

        private string BuildWindowFunctionSql(WindowFunctionInfo wf, string overClause)
        {
            if (wf.ValueSelector != null)
            {
                var param = wf.ValueSelector.Parameters[0];
                if (!_correlatedParams.TryGetValue(param, out var info))
                {
                    info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
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
                        info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
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

                string offsetParam;
                do
                {
                    offsetParam = _provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                }
                while (_params.ContainsKey(offsetParam));
                _params[offsetParam] = wf.Offset;
                return $"{wf.FunctionName}({valueSql}, {offsetParam}{defaultSql}) OVER ({overClause})";
            }
            return $"{wf.FunctionName}() OVER ({overClause})";
        }

        private string TranslateSubExpression(Expression e)
        {
            if (_recursionDepth >= MaxRecursionDepth)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Query exceeds maximum translation depth of {MaxRecursionDepth}"));

            using var subTranslator = QueryTranslator.Create(_ctx, _mapping, _params, _parameterManager.Index, _correlatedParams, _tables, _compiledParams, _paramMap, _joinCounter, _recursionDepth + 1);
            var subPlan = subTranslator.Translate(e);
            _parameterManager.Index = subTranslator.ParameterIndex;
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
            lock (_syncRoot)
            {
                _translatorPool.Return(this);
            }
        }

        private sealed class QueryTranslatorPooledObjectPolicy : PooledObjectPolicy<QueryTranslator>
        {
            public override QueryTranslator Create() => new QueryTranslator();

            public override bool Return(QueryTranslator obj)
            {
                obj.Clear();
                return true;
            }
        }

        private sealed class ListPooledObjectPolicy<T> : PooledObjectPolicy<List<T>>
        {
            public override List<T> Create() => new List<T>();

            public override bool Return(List<T> obj)
            {
                obj.Clear();
                return true;
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

        private Expression HandleInnerJoin(MethodCallExpression node)
        {
            if (node.Arguments.Count < 5)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join operation requires 5 arguments"));

            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;

            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join selectors must be lambda expressions"));

            Visit(outerQuery);

            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);

            var outerAlias = EscapeAlias("T0");
            var innerAlias = EscapeAlias("T" + (++_joinCounter));

            var correlationBackup = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)?>(2);

            void Correlate(ParameterExpression param, TableMapping mapping, string alias)
            {
                if (_correlatedParams.TryGetValue(param, out var existing))
                    correlationBackup[param] = existing;
                else
                    correlationBackup[param] = null;
                _correlatedParams[param] = (mapping, alias);
            }

            Correlate(outerKeySelector.Parameters[0], _mapping, outerAlias);
            Correlate(innerKeySelector.Parameters[0], innerMapping, innerAlias);

            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap);
            var outerKeyVisitor = ExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);

            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap);
            var innerKeyVisitor = ExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);

            foreach (var kvp in outerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(outerKeyVisitor);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            ExpressionVisitorPool.Return(innerKeyVisitor);

            _correlatedParams[resultSelector.Parameters[0]] = (_mapping, outerAlias);
            _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);

            try
            {
                JoinBuilder.SetupJoinProjection(resultSelector, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
                if (_projection != null)
                    SetupProjectionPropertyMapping(_projection, _mapping, outerAlias, innerMapping, innerAlias);
                else
                    _projectionPropertyMap.Clear();

                var sql = JoinBuilder.BuildJoinClause(_projection, _mapping, outerAlias, innerMapping, innerAlias, "INNER JOIN", outerKeySql, innerKeySql, _correlatedParams);
                _sql.Clear();
                _sql.Append(sql);

                return node;
            }
            finally
            {
                foreach (var kvp in correlationBackup)
                {
                    if (kvp.Value.HasValue)
                        _correlatedParams[kvp.Key] = kvp.Value.Value;
                    else
                        _correlatedParams.Remove(kvp.Key);
                }
            }
        }

        private Expression HandleGroupJoin(MethodCallExpression node)
        {
            if (node.Arguments.Count < 5)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join operation requires 5 arguments"));

            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;

            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join selectors must be lambda expressions"));

            Visit(outerQuery);

            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);

            var outerAlias = EscapeAlias("T0");
            var innerAlias = EscapeAlias("T" + (++_joinCounter));

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

            JoinBuilder.SetupJoinProjection(null, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);

            var sql = JoinBuilder.BuildJoinClause(_projection, _mapping, outerAlias, innerMapping, innerAlias, "LEFT JOIN", outerKeySql, innerKeySql, _correlatedParams, outerKeySql);
            _sql.Clear();
            _sql.Append(sql);

            var outerType = outerKeySelector.Parameters[0].Type;
            var innerType = innerKeySelector.Parameters[0].Type;
            var resultType = resultSelector.Body.Type;

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
            var outerAlias = EscapeAlias("T0");

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
                var innerAlias = EscapeAlias("T" + (++_joinCounter));

                if (resultSelector != null && resultSelector.Parameters.Count > 1 &&
                    !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                {
                    _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
                }

                using var joinSql = new OptimizedSqlBuilder(256);

                if (_projection?.Body is NewExpression newExpr)
                {
                    var neededColumns = JoinBuilder.ExtractNeededColumns(newExpr, outerMapping, innerMapping, outerAlias, innerAlias, _correlatedParams);
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
                    SetupProjectionPropertyMapping(_projection!, outerMapping, outerAlias, innerMapping, innerAlias);
                }
                else
                {
                    _projectionPropertyMap.Clear();
                    _mapping = innerMapping;
                }

                return node;
            }

            // Otherwise treat as CROSS JOIN
            var innerType = GetElementType(collectionSelector.Body);
            var crossMapping = TrackMapping(innerType);
            var crossAlias = EscapeAlias("T" + (++_joinCounter));

            if (resultSelector != null && resultSelector.Parameters.Count > 1 &&
                !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
            {
                _correlatedParams[resultSelector.Parameters[1]] = (crossMapping, crossAlias);
            }

            using var crossSql = new OptimizedSqlBuilder(256);

            if (_projection?.Body is NewExpression crossNew)
            {
                var neededColumns = JoinBuilder.ExtractNeededColumns(crossNew, outerMapping, crossMapping, outerAlias, crossAlias, _correlatedParams);
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
                SetupProjectionPropertyMapping(_projection!, outerMapping, outerAlias, crossMapping, crossAlias);
            }
            else
            {
                _projectionPropertyMap.Clear();
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

            using var subTranslator = QueryTranslator.Create(_ctx, _mapping, _params, _parameterManager.Index, _correlatedParams, _tables, _compiledParams, _paramMap, _joinCounter, _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _parameterManager.Index = subTranslator.ParameterIndex;
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
            var limitParam = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
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
                var paramName = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
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

            var paramName = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
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

        private void SetupProjectionPropertyMapping(
            LambdaExpression projection,
            TableMapping outerMapping,
            string outerAlias,
            TableMapping innerMapping,
            string innerAlias)
        {
            _projectionPropertyMap.Clear();

            if (projection.Body is NewExpression newExpr && newExpr.Members != null)
            {
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var memberName = newExpr.Members[i].Name;

                    if (arg is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression param)
                    {
                        var propName = memberExpr.Member.Name;

                        if (param.Type == outerMapping.Type &&
                            outerMapping.ColumnsByName.ContainsKey(propName))
                        {
                            _projectionPropertyMap[memberName] = (outerMapping, outerAlias, propName);
                        }
                        else if (param.Type == innerMapping.Type &&
                                 innerMapping.ColumnsByName.ContainsKey(propName))
                        {
                            _projectionPropertyMap[memberName] = (innerMapping, innerAlias, propName);
                        }
                    }
                }
            }
            else if (projection.Body is MemberInitExpression initExpr)
            {
                foreach (var binding in initExpr.Bindings.OfType<MemberAssignment>())
                {
                    var memberName = binding.Member.Name;

                    if (binding.Expression is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression param)
                    {
                        var propName = memberExpr.Member.Name;

                        if (param.Type == outerMapping.Type &&
                            outerMapping.ColumnsByName.ContainsKey(propName))
                        {
                            _projectionPropertyMap[memberName] = (outerMapping, outerAlias, propName);
                        }
                        else if (param.Type == innerMapping.Type &&
                                 innerMapping.ColumnsByName.ContainsKey(propName))
                        {
                            _projectionPropertyMap[memberName] = (innerMapping, innerAlias, propName);
                        }
                    }
                }
            }
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ParameterExpression pe)
            {
                if (_correlatedParams.TryGetValue(pe, out var info))
                {
                    if (info.Mapping.ColumnsByName.TryGetValue(node.Member.Name, out var col))
                    {
                        _sql.Append($"{info.Alias}.{col.EscCol}");
                        return node;
                    }
                }

                if (_projectionPropertyMap.TryGetValue(node.Member.Name, out var mappingInfo))
                {
                    var sourceCol = mappingInfo.Mapping.ColumnsByName[mappingInfo.ColumnName];
                    _sql.Append($"{mappingInfo.Alias}.{sourceCol.EscCol}");
                    return node;
                }

                if (_mapping.ColumnsByName.TryGetValue(node.Member.Name, out var column))
                {
                    _sql.Append(column.EscCol);
                    return node;
                }
            }

            if (TryGetConstantValue(node, out var value))
            {
                var paramName = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                _params[paramName] = value ?? DBNull.Value;
                _sql.Append(paramName);
                return node;
            }

            throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, $"Member '{node.Member.Name}'"));
        }

        private string EscapeAlias(string alias)
        {
            if (string.IsNullOrWhiteSpace(alias))
                throw new NormQueryException($"Invalid table alias: {alias}");
            return _provider.Escape(alias);
        }

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
            var alias = EscapeAlias("T" + _joinCounter);
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
            var alias = EscapeAlias("T" + _joinCounter);
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
                    _projectionPropertyMap.Clear();

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
            var selectItems = _selectItemsPool.Get();
            try
            {
                var builder = PooledStringBuilder.Rent();
                builder.Append(groupBySql).Append(" AS GroupKey");
                selectItems.Add(builder.ToString());
                PooledStringBuilder.Return(builder);

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
                                builder = PooledStringBuilder.Rent();
                                builder.Append(aggregateSql).Append(" AS ").Append(memberName);
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
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

                            builder = PooledStringBuilder.Rent();
                            builder.Append(sql).Append(" AS ").Append(memberName);
                            selectItems.Add(builder.ToString());
                            PooledStringBuilder.Return(builder);

                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            ExpressionVisitorPool.Return(visitor);
                        }
                    }
                }

                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.InnerBuilder.AppendJoin(", ", selectItems);
            }
            finally
            {
                _selectItemsPool.Return(selectItems);
            }
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
                var alias = EscapeAlias("T" + _joinCounter);
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
            var alias = EscapeAlias("T" + _joinCounter);
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
            var invoker = ExpressionUtils.CompileWithFallback(lambda, cts.Token);

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
            return ExpressionUtils.CompileWithFallback(lambda, cts.Token);
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
                if (node.Expression is MemberInitExpression memberInit)
                {
                    foreach (var binding in memberInit.Bindings)
                    {
                        if (binding is MemberAssignment assignment &&
                            assignment.Member.Name == node.Member.Name)
                            return Visit(assignment.Expression);
                    }
                }
                else if (node.Expression is NewExpression newExpr && newExpr.Members != null)
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
