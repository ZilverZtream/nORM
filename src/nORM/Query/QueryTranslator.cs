using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
    /// <summary>
    /// Translates LINQ expression trees to SQL queries using the visitor pattern.
    /// </summary>
    /// <remarks>
    /// This class uses recursive instantiation for subquery translation.
    ///
    /// **Current Architecture:**
    /// - Every subquery or complex nested expression creates a new QueryTranslator instance
    /// - See TranslateSubExpression() and VisitMethodCall (Any/Contains) - both call QueryTranslator.Create()
    /// - For deeply nested queries, this creates O(depth) allocations
    /// - Object pooling (_translatorPool) only applies to top-level queries, not recursive instances
    ///
    /// **Performance Impact:**
    /// - Queries with N nested subqueries allocate N QueryTranslator instances (each ~2KB+)
    /// - GC pressure increases linearly with query complexity
    /// - Example: 10-level nested UNION creates 10+ translator instances
    /// - Each instance allocates: SqlBuilder, Dictionary collections, Lists, etc.
    ///
    /// **Recommended Refactoring:**
    /// Replace recursive instantiation with a single stateful visitor that manages context stacks:
    ///
    /// 1. **Context Stack Approach:**
    ///    - Maintain Stack&lt;TranslationContext&gt; for depth tracking
    ///    - Push/pop context when entering/exiting subqueries
    ///    - Reuse single QueryTranslator instance across entire translation
    ///
    /// 2. **State Management:**
    ///    - Replace field assignments with stack-based state
    ///    - Use ref structs or value types for context to avoid heap allocation
    ///    - Ensure proper stack unwinding on exceptions
    ///
    /// 3. **Benefits:**
    ///    - Reduces allocations from O(depth) to O(1)
    ///    - Enables true pooling for all translation work
    ///    - Improves cache locality
    ///    - Reduces GC pressure by ~90% for complex queries
    ///
    /// **Migration Complexity:**
    /// This is a major architectural change requiring:
    /// - Redesign of visitor state management (~2000 LOC)
    /// - Extensive testing to ensure query correctness
    /// - Careful handling of correlated subqueries and parameter scoping
    /// - Breaking changes to internal APIs
    ///
    /// Current recursion depth limit: configurable via <see cref="nORM.Configuration.DbContextOptions.MaxRecursionDepth"/> (default 50 levels)
    /// </remarks>
    internal sealed partial class QueryTranslator : ExpressionVisitor, IDisposable
    {
        /// <summary>Default initial capacity for cross-join SQL builders.</summary>
        private const int CrossJoinSqlInitialCapacity = 256;
        /// <summary>Divisor applied to complexity cost to compute the timeout multiplier.</summary>
        private const double ComplexityCostDivisor = 1000.0;
        /// <summary>
        /// Fraction of <see cref="_maxRecursionDepth"/> beyond which a warning is logged.
        /// The actual threshold is <c>min(DeepRecursionWarningAbsolute, maxDepth / 2)</c>.
        /// </summary>
        private const int DeepRecursionWarningAbsolute = 15;
        /// <summary>Static empty dictionary used to avoid allocations when logging without parameters.</summary>
        private static readonly IReadOnlyDictionary<string, object> EmptyParamDict =
            new Dictionary<string, object>();

        /// <summary>Maps LINQ aggregate method names to their SQL function equivalents, avoiding <c>ToUpperInvariant()</c> allocations on each call.</summary>
        private static readonly Dictionary<string, string> AggregateFunctionMap = new(StringComparer.OrdinalIgnoreCase)
        {
            { "Sum", "SUM" },
            { "Average", "AVG" },
            { "Min", "MIN" },
            { "Max", "MAX" },
            { "Count", "COUNT" },
            { "LongCount", "COUNT" },
            { "InternalSumExpression", "SUM" },
            { "InternalAverageExpression", "AVG" },
            { "InternalMinExpression", "MIN" },
            { "InternalMaxExpression", "MAX" }
        };

        private DbContext _ctx = null!;
        private SqlBuilder _clauses = new();
        private readonly object _syncRoot = new();
        private readonly MaterializerFactory _materializerFactory = new();
        private TableMapping _mapping = null!;
        private Type? _rootType;
        private readonly ParameterManager _parameterManager = new();
        private IDictionary<string, object> _params { get => _parameterManager.Parameters; set => _parameterManager.Parameters = value; }
        private List<string> _compiledParams { get => _parameterManager.CompiledParameters; set => _parameterManager.CompiledParameters = value; }
        private Dictionary<ParameterExpression, string> _paramMap { get => _parameterManager.ParameterMap; set => _parameterManager.ParameterMap = value; }
        internal int ParameterIndex => _parameterManager.Index;
        private List<IncludePlan> _includes = new();
        private List<M2MIncludePlan> _m2mIncludes = new();
        private LambdaExpression? _projection;
        private Func<object, object>? _clientProjection;
        private bool _isAggregate;
        private string _methodName = "";
        private Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _correlatedParams = new();
        private GroupJoinInfo? _groupJoinInfo;
        private int _joinCounter;
        private DatabaseProvider _provider = null!;
        private bool _singleResult;
        private bool _noTracking;
        private bool _splitQuery;
        private HashSet<string> _tables = new();
        private readonly Stack<TranslationContextSnapshot> _contextStack = new();
        private List<PropertyInfo> _detectedCollections = new();
        private TimeSpan _estimatedTimeout;
        private bool _isCacheable;
        private TimeSpan? _cacheExpiration;
        private DateTime? _asOfTimestamp;
        // Complexity metrics accumulated during expression-tree visitation.
        // These are used by GetAdaptiveTimeout instead of post-hoc SQL string scanning.
        private QueryComplexityMetrics _complexityMetrics;
        // Recursion depth limit is read from DbContextOptions.MaxRecursionDepth (default 50).
        // The effective limit is cached on the QueryTranslator instance so that options changes mid-query have no effect.
        private int _maxRecursionDepth = 50; // Updated from _ctx.Options during Reset()
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
            IDictionary<string, object> parameters,
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
            _maxRecursionDepth = ctx.Options.MaxRecursionDepth;
        }
        internal static QueryTranslator Create(
            DbContext ctx,
            TableMapping mapping,
            IDictionary<string, object> parameters,
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
                SqlBuilder? oldClauses = Interlocked.Exchange(ref _clauses, null!);
                oldClauses?.Dispose();
                _ctx = ctx;
                _provider = ctx.Provider;
                _mapping = null!;
                _rootType = null;
                _parameterManager.Reset();
                _includes = new List<IncludePlan>();
                _m2mIncludes = new List<M2MIncludePlan>();
                _projection = null;
                _clientProjection = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
                _groupJoinInfo = null;
                _joinCounter = 0;
                _recursionDepth = 0;
                _singleResult = false;
                _noTracking = false;
                _splitQuery = false;
                _tables = new HashSet<string>();
                _clauses = new SqlBuilder();
                _contextStack.Clear();
                _estimatedTimeout = ctx.Options.TimeoutConfiguration.BaseTimeout;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
                _detectedCollections = new List<PropertyInfo>();
                _complexityMetrics = default;
                // Capture the configured recursion depth limit at Reset time.
                _maxRecursionDepth = ctx.Options.MaxRecursionDepth;
            }
        }
        private void Clear()
        {
            lock (_syncRoot)
            {
                SqlBuilder? oldClauses = Interlocked.Exchange(ref _clauses, null!);
                oldClauses?.Dispose();
                _clauses = new SqlBuilder();
                _includes = new List<IncludePlan>();
                _m2mIncludes = new List<M2MIncludePlan>();
                _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
                _tables = new HashSet<string>();
                _ctx = null!;
                _provider = null!;
                _mapping = null!;
                _rootType = null;
                _parameterManager.Reset();
                _projection = null;
                _clientProjection = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _groupJoinInfo = null;
                _joinCounter = 0;
                _recursionDepth = 0;
                _contextStack.Clear();
                _singleResult = false;
                _noTracking = false;
                _splitQuery = false;
                _estimatedTimeout = default;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
                _detectedCollections = new List<PropertyInfo>();
                _complexityMetrics = default;
            }
        }
        /// <summary>
        /// Creates a delegate that materializes rows from a reader into the specified type.
        /// </summary>
        /// <param name="mapping">Mapping describing the table schema.</param>
        /// <param name="targetType">Type to materialize into.</param>
        /// <param name="projection">Optional projection expression.</param>
        /// <returns>A materializer delegate.</returns>
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
        /// <summary>
        /// Converts a LINQ <see cref="Expression"/> into an executable <see cref="QueryPlan"/>,
        /// performing validation, setup and SQL generation in a thread-safe manner.
        /// </summary>
        /// <param name="e">The query expression to translate.</param>
        /// <returns>The resulting <see cref="QueryPlan"/>.</returns>
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
                // Guard so sub-translators created by BuildExists/BuildIn throw when depth exceeded.
                if (_t._recursionDepth >= _t._maxRecursionDepth)
                    throw new NormQueryException(
                        $"Query exceeds maximum translation depth of {_t._maxRecursionDepth}. " +
                        $"This typically indicates overly complex nested subqueries. " +
                        $"Consider simplifying the query or breaking it into multiple queries. " +
                        $"You can also increase the limit via DbContextOptions.MaxRecursionDepth (current: {_t._maxRecursionDepth}, max: 200).");
                var complexityInfo = _complexityAnalyzer.AnalyzeQuery(_expression, _t._ctx.Options);
                if (complexityInfo.WarningMessages.Count > 0)
                {
                    var warnings = string.Join("; ", complexityInfo.WarningMessages);
                    _t._ctx.Options.Logger?.LogQuery($"-- WARN: {warnings}", EmptyParamDict, TimeSpan.Zero, 0);
                }
                var timeoutMultiplier = Math.Max(1.0, complexityInfo.EstimatedCost / ComplexityCostDivisor);
                var adjustedTimeout = TimeSpan.FromMilliseconds(_t._ctx.Options.TimeoutConfiguration.BaseTimeout.TotalMilliseconds * timeoutMultiplier);
                _t._estimatedTimeout = adjustedTimeout;
                // Reset complexity metrics so they accumulate cleanly during the upcoming
                // expression-tree walk in Generate().
                _t._complexityMetrics = default;
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
                            // Use direct params assignment (not _compiledParams) - discriminator is a fixed constant,
                            // not a closure capture. Adding to _compiledParams causes it to be overridden with DBNull
                            // when ParameterValueExtractor extracts lambda parameter placeholders.
                            _t._params[paramName] = discAttr.Value;
                            _t._where.Append($"({_t._mapping.DiscriminatorColumn!.EscCol} = {paramName})");
                        }
                        break;
                    }
                    baseType = baseType.BaseType;
                }
                return this;
            }
            /// <summary>
            /// Generates the final <see cref="QueryPlan"/> including SQL text, parameters and materializer.
            /// </summary>
            /// <returns>The completed query plan.</returns>
            public QueryPlan Generate()
            {
                _t.Visit(_expression);
                var materializerType = _t._projection?.Body.Type ?? _t._rootType ?? _t._mapping.Type;
                if (_t._isAggregate && _t._groupBy.Count == 0 && (_expression as MethodCallExpression)?.Method.Name is "Count" or "LongCount")
                {
                    materializerType = typeof(int);
                }

                // Create both sync and async materializers.
                Func<DbDataReader, object> syncMaterializer;
                Func<DbDataReader, CancellationToken, Task<object>> materializer;

                if (_t._projection != null)
                {
                    syncMaterializer = _t._materializerFactory.CreateSyncMaterializer(_t._mapping, materializerType, _t._projection);
                    materializer = _t._materializerFactory.CreateSchemaAwareMaterializer(_t._mapping, materializerType, _t._projection);
                }
                else
                {
                    syncMaterializer = _t._materializerFactory.CreateSyncMaterializer(_t._mapping, materializerType, null);
                    materializer = _t._materializerFactory.CreateMaterializer(_t._mapping, materializerType, null);
                }

                var isScalar = _t._isAggregate && _t._groupBy.Count == 0;
                if (isScalar)
                {
                    var aggMethod = (_expression as MethodCallExpression)?.Method;
                    var aggMethodName = aggMethod?.Name ?? string.Empty;
                    // Count/LongCount always return non-null integer — keep the fast path.
                    if (aggMethodName is "Count" or "LongCount")
                    {
                        syncMaterializer = static (DbDataReader r) =>
                        {
                            if (r.Read())
                            {
                                var v = r.GetValue(0);
                                return v is long l ? (object)l : Convert.ToInt64(v);
                            }
                            return 0L;
                        };
                        materializer = static async (DbDataReader r, CancellationToken ct) =>
                        {
                            if (await r.ReadAsync(ct).ConfigureAwait(false))
                            {
                                var v = r.GetValue(0);
                                return v is long l ? (object)l : Convert.ToInt64(v);
                            }
                            return 0L;
                        };
                        // materializerType already set to int above; leave it.
                    }
                    else
                    {
                        // Type-aware materializer for Sum/Average/Min/Max.
                        var scalarReturnType = aggMethod?.ReturnType ?? typeof(long);
                        var underlyingType = Nullable.GetUnderlyingType(scalarReturnType) ?? scalarReturnType;
                        var isNullableReturn = Nullable.GetUnderlyingType(scalarReturnType) != null;
                        var isSum = aggMethodName == "Sum";

                        object ReadScalarValue(object dbValue)
                        {
                            try { return Convert.ChangeType(dbValue, underlyingType); }
                            catch (InvalidCastException) { return dbValue; }
                            catch (FormatException) { return dbValue; }
                            catch (OverflowException) { return dbValue; }
                        }

                        object HandleNull()
                        {
                            if (isNullableReturn) return null!;
                            if (isSum) return Convert.ChangeType(0, underlyingType);
                            throw new InvalidOperationException("Sequence contains no elements");
                        }

                        syncMaterializer = (DbDataReader r) =>
                        {
                            if (r.Read())
                            {
                                var v = r.GetValue(0);
                                if (v == null || v is DBNull) return HandleNull();
                                return ReadScalarValue(v);
                            }
                            return HandleNull();
                        };
                        materializer = async (DbDataReader r, CancellationToken ct) =>
                        {
                            if (await r.ReadAsync(ct).ConfigureAwait(false))
                            {
                                var v = r.GetValue(0);
                                if (v == null || v is DBNull) return HandleNull();
                                return ReadScalarValue(v);
                            }
                            return HandleNull();
                        };
                        materializerType = scalarReturnType;
                    }
                }

                if (_t._sql.Length == 0)
                {
                    var fromClause = _t._mapping.EscTable;
                    var alias = _t._correlatedParams.Count > 0 ? _t._correlatedParams.Values.FirstOrDefault().Alias : null;
                    if (_t._asOfTimestamp.HasValue)
                    {
                        alias ??= _t.EscapeAlias("T0");
                        var timeParamName = _t._provider.ParamPrefix + "p" + _t._parameterManager.GetNextIndex();
                        _t.AddParameter(timeParamName, _t._asOfTimestamp.Value);
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
                        var prefix = PooledStringBuilder.Rent();
                        try
                        {
                            prefix.Append("SELECT COUNT(*) FROM ").Append(fromClause);
                            if (alias != null) prefix.Append(' ').Append(alias);
                            _t._sql.Insert(0, prefix.ToString());
                        }
                        finally
                        {
                            PooledStringBuilder.Return(prefix);
                        }
                    }
                    else
                    {
                        var windowFuncs = _t._clauses.WindowFunctions;
                        if (windowFuncs.Count > 0 && _t._projection == null)
                            _t._projection = windowFuncs[^1].ResultSelector;
                        string select;
                        if (windowFuncs.Count > 0 && _t._projection != null)
                        {
                            var orderByForOverClause = _t._orderBy.Count > 0
                                ? $"ORDER BY {PooledStringBuilder.JoinOrderBy(_t._orderBy)}"
                                : "ORDER BY (SELECT NULL)";
                            select = _t.BuildSelectWithWindowFunctions(_t._projection, windowFuncs, orderByForOverClause);
                        }
                        else if (_t._projection != null)
                        {
                            var selectVisitor = new SelectClauseVisitor(_t._mapping, _t._groupBy, _t._provider);
                            select = selectVisitor.Translate(_t._projection.Body);

                            // Capture detected collections for split query processing
                            _t._detectedCollections.AddRange(selectVisitor.DetectedCollections);

                            // If we detected collections, ensure primary key is included in SELECT
                            if (_t._detectedCollections.Count > 0 && _t._mapping.KeyColumns.Length > 0)
                            {
                                var keyColumns = new List<string>();
                                foreach (var keyCol in _t._mapping.KeyColumns)
                                {
                                    var escapedCol = keyCol.EscCol;
                                    // Only add if not already present
                                    if (!select.Contains(escapedCol, StringComparison.Ordinal))
                                    {
                                        keyColumns.Add($"{escapedCol} AS {_t._provider.Escape(keyCol.PropName)}");
                                    }
                                }

                                if (keyColumns.Count > 0)
                                {
                                    if (!string.IsNullOrEmpty(select))
                                        select = select + ", " + string.Join(", ", keyColumns);
                                    else
                                        select = string.Join(", ", keyColumns);
                                }
                            }
                        }
                        else
                        {
                            select = PooledStringBuilder.Join(_t._mapping.Columns.Select(c => c.EscCol));
                        }
                        var distinct = _t._isDistinct ? "DISTINCT " : string.Empty;
                        var prefix = PooledStringBuilder.Rent();
                        try
                        {
                            prefix.Append("SELECT ").Append(distinct).Append(select).Append(" FROM ").Append(fromClause);
                            if (alias != null) prefix.Append(' ').Append(alias);
                            _t._sql.Insert(0, prefix.ToString());
                        }
                        finally
                        {
                            PooledStringBuilder.Return(prefix);
                        }
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
                _t._ctx.Provider.ApplyPaging(_t._sql, _t._take, _t._skip, _t._takeParam, _t._skipParam);
                var singleResult = _t._singleResult || _t._methodName is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault"
                    or "ElementAt" or "ElementAtOrDefault" or "Last" or "LastOrDefault" || isScalar;
                var elementType = _t._groupJoinInfo?.ResultType ?? materializerType;
                var bulkCudShape = new BulkCudQueryShape(
                    _t._where.Length > 0 ? " WHERE " + _t._where.ToSqlString() : string.Empty,
                    _t._groupBy.Count > 0,
                    _t._orderBy.Count > 0,
                    _t._having.Length > 0,
                    _t._complexityMetrics.JoinCount > 0,
                    _t._isDistinct,
                    _t._take.HasValue || _t._skip.HasValue || _t._takeParam != null || _t._skipParam != null);

                // Build dependent query definitions for nested collections
                List<DependentQueryDefinition>? dependentQueries = null;
                if (_t._detectedCollections.Count > 0)
                {
                    dependentQueries = _t.BuildDependentQueryDefinitions();
                }

                var plan = new QueryPlan(
                    _t._sql.ToString(),
                    (IReadOnlyDictionary<string, object>)_t._params,
                    _t._compiledParams,
                    materializer,
                    syncMaterializer,
                    elementType,
                    isScalar,
                    singleResult,
                    _t._noTracking,
                    _t._methodName,
                    new List<IncludePlan>(_t._includes),
                    _t._groupJoinInfo,
                    _t._tables.ToArray(),
                    _t._splitQuery,
                    _t._estimatedTimeout,
                    _t._isCacheable,
                    _t._cacheExpiration,
                    Take: _t._take,
                    DependentQueries: dependentQueries,
                    ClientProjection: _t._clientProjection,
                    Complexity: _t._complexityMetrics,
                    M2MIncludes: _t._m2mIncludes.Count > 0 ? new List<M2MIncludePlan>(_t._m2mIncludes) : null,
                    BulkCudShape: bulkCudShape
                );
                QueryPlanValidator.Validate(plan, _t._provider);
                return plan;
            }
        }
        private TableMapping TrackMapping(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            var map = _ctx?.GetMapping(type) ?? throw new InvalidOperationException("Context not available");
            _tables.Add(map.TableName);
            return map;
        }

        /// <summary>
        /// Builds dependent query definitions for detected navigation collections.
        /// This enables split query execution to avoid Cartesian explosion.
        /// </summary>
        private List<DependentQueryDefinition> BuildDependentQueryDefinitions()
        {
            var dependentQueries = new List<DependentQueryDefinition>();

            foreach (var collectionProperty in _detectedCollections)
            {
                // Try to find the relation for this navigation property
                if (!_mapping.Relations.TryGetValue(collectionProperty.Name, out var relation))
                {
                    continue; // Skip if relation not found
                }

                // Get the element type of the collection
                var collectionType = collectionProperty.PropertyType;
                Type elementType;

                if (collectionType.IsGenericType)
                {
                    elementType = collectionType.GetGenericArguments()[0];
                }
                else
                {
                    // Try to find IEnumerable<T>
                    var iEnumerable = collectionType.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
                    if (iEnumerable == null)
                        continue;

                    elementType = iEnumerable.GetGenericArguments()[0];
                }

                // Get the target mapping for the dependent type
                var targetMapping = _ctx.GetMapping(relation.DependentType);

                // Create the dependent query definition
                var dependentQuery = new DependentQueryDefinition(
                    TargetMapping: targetMapping,
                    ForeignKeyColumn: relation.ForeignKey,
                    ParentKeyProperty: relation.PrincipalKey.Prop,
                    TargetCollectionProperty: collectionProperty,
                    CollectionElementType: elementType
                );

                dependentQueries.Add(dependentQuery);
            }

            return dependentQueries;
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
                    var alias = ne.Members?[i]?.Name ?? $"Item{i + 1}";
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
                        var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                        var visitor = FastExpressionVisitorPool.Get(in vctx);
                        var sql = visitor.Translate(arg);
                        foreach (var kvp in visitor.GetParameters())
                            AddParameter(kvp.Key, kvp.Value);
                        FastExpressionVisitorPool.Return(visitor);
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
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var valueSql = visitor.Translate(wf.ValueSelector.Body);
                foreach (var kvp in visitor.GetParameters())
                    AddParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(visitor);
                string defaultSql = string.Empty;
                if (wf.DefaultValueSelector != null)
                {
                    var dParam = wf.DefaultValueSelector.Parameters[0];
                    if (!_correlatedParams.TryGetValue(dParam, out info))
                    {
                        info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
                        _correlatedParams[dParam] = info;
                    }
                    var vctx2 = new VisitorContext(_ctx, _mapping, _provider, dParam, info.Alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                    var visitor2 = FastExpressionVisitorPool.Get(in vctx2);
                    var defSql = visitor2.Translate(wf.DefaultValueSelector.Body);
                    foreach (var kv in visitor2.GetParameters())
                        AddParameter(kv.Key, kv.Value);
                    FastExpressionVisitorPool.Return(visitor2);
                    defaultSql = $", {defSql}";
                }
                string offsetParam;
                do
                {
                    offsetParam = _provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                }
                while (_params.ContainsKey(offsetParam));
                AddParameter(offsetParam, wf.Offset);
                return $"{wf.FunctionName}({valueSql}, {offsetParam}{defaultSql}) OVER ({overClause})";
            }
            return $"{wf.FunctionName}() OVER ({overClause})";
        }
        /// <summary>
        /// Translates a sub-expression using a nested translation context.
        /// </summary>
        /// <remarks>
        /// Uses a stack-based context swap instead of allocating a new <see cref="QueryTranslator"/>,
        /// reducing allocations for nested subqueries such as unions or correlated WHERE clauses.
        /// </remarks>
        private string TranslateSubExpression(Expression e)
        {
            if (_recursionDepth >= _maxRecursionDepth)
                throw new NormQueryException(
                    $"Query exceeds maximum translation depth of {_maxRecursionDepth}. " +
                    $"This typically indicates overly complex nested subqueries. " +
                    $"Consider simplifying the query by breaking it into multiple queries or using CTEs. " +
                    $"You can also increase the limit via DbContextOptions.MaxRecursionDepth (current: {_maxRecursionDepth}, max: 200).");

            // Log deep recursion for monitoring.
            if (_recursionDepth > Math.Min(DeepRecursionWarningAbsolute, _maxRecursionDepth / 2))
            {
                _ctx.Options.Logger?.LogWarning(
                    "Query translation depth is {Depth} (max: {MaxDepth}). " +
                    "Deep nesting causes O(depth) allocations (~2KB per level). " +
                    "Consider query simplification or increase DbContextOptions.MaxRecursionDepth.",
                    _recursionDepth + 1, _maxRecursionDepth);
            }

            _complexityMetrics.SubqueryDepth++;
            var subPlan = TranslateInSubContext(e, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out _);
            return subPlan.Sql;
        }
        /// <summary>
        /// Retrieves the timestamp associated with a named temporal tag from the special
        /// <c>__NormTemporalTags</c> table. Tags are used to reference specific points in time
        /// for temporal queries.
        /// </summary>
        /// <param name="tagName">The name of the temporal tag.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>The timestamp stored for the specified tag.</returns>
        /// <exception cref="NormQueryException">Thrown if the tag does not exist.</exception>
        private async Task<DateTime> GetTimestampForTagAsync(string tagName, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tagName))
                throw new ArgumentException("Tag name must not be null or empty.", nameof(tagName));
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            var pName = _provider.ParamPrefix + "p0";
            // Use provider-escaped identifier SQL to avoid hardcoded unescaped names.
            cmd.CommandText = _provider.GetTagLookupSql(pName);
            cmd.AddParam(pName, tagName);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result == null || result == DBNull.Value)
                throw new NormQueryException($"Tag '{tagName}' not found.");
            return Convert.ToDateTime(result);
        }
        /// <summary>
        /// Releases resources used by the translator and, if pooled, returns it to the shared pool.
        /// </summary>
        public void Dispose()
        {
            // Clear() already performs Interlocked.Exchange + Dispose on _clauses,
            // so delegate directly to avoid a redundant SqlBuilder allocation.
            Clear();
        }
        private sealed class QueryTranslatorPooledObjectPolicy : PooledObjectPolicy<QueryTranslator>
        {
            /// <summary>
            /// Creates a new <see cref="QueryTranslator"/> for inclusion in the pool.
            /// </summary>
            /// <returns>A newly constructed translator instance.</returns>
            public override QueryTranslator Create() => new QueryTranslator();

            /// <summary>
            /// Resets a translator before returning it to the pool for reuse.
            /// </summary>
            /// <param name="obj">The translator to recycle.</param>
            /// <returns>Always <c>true</c> to indicate pooling should continue.</returns>
            public override bool Return(QueryTranslator obj)
            {
                obj.Clear();
                return true;
            }
        }

        private sealed class ListPooledObjectPolicy<T> : PooledObjectPolicy<List<T>>
        {
            /// <summary>
            /// Creates a new list instance for pooling.
            /// </summary>
            public override List<T> Create() => new List<T>();

            /// <summary>
            /// Clears the list prior to returning it to the pool.
            /// </summary>
            /// <param name="obj">The list to reset.</param>
            /// <returns>Always <c>true</c>, indicating the list can be reused.</returns>
            public override bool Return(List<T> obj)
            {
                obj.Clear();
                return true;
            }
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            _methodName = node.Method.Name;
            // Track complexity metrics from the expression tree as we walk it.
            // This replaces post-hoc SQL string scanning in GetAdaptiveTimeout.
            switch (_methodName)
            {
                case "Join":
                case "GroupJoin":
                case "SelectMany":
                    _complexityMetrics.JoinCount++;
                    break;
                case "GroupBy":
                    _complexityMetrics.HasGroupBy = true;
                    break;
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    _complexityMetrics.HasOrderBy = true;
                    break;
                case "Where":
                    _complexityMetrics.PredicateCount++;
                    break;
                case "Distinct":
                    _complexityMetrics.HasDistinct = true;
                    break;
                case "Sum":
                case "Average":
                case "Min":
                case "Max":
                case "Count":
                case "LongCount":
                case "InternalSumExpression":
                case "InternalAverageExpression":
                case "InternalMinExpression":
                case "InternalMaxExpression":
                    _complexityMetrics.HasAggregates = true;
                    break;
            }
            if (_methodTranslators.TryGetValue(_methodName, out var translator))
            {
                return translator.Translate(this, node);
            }
            return base.VisitMethodCall(node);
        }
        private Expression HandleSetOperation(MethodCallExpression node)
        {
            _isAggregate = true;
            _singleResult = true;
            var source = node.Arguments[0];
            var genericArgs = source.Type.GetGenericArguments();
            if (genericArgs.Length == 0)
                throw new NormQueryException(
                    string.Format(ErrorMessages.QueryTranslationFailed,
                    $"Expected a generic IQueryable<T> source type but found '{source.Type.Name}'."));
            var elementType = genericArgs[0];
            if (node.Method.Name == nameof(Queryable.Any) && node.Arguments.Count > 1 && StripQuotes(node.Arguments[1]) is LambdaExpression anyPred)
            {
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(anyPred));
            }
            else if (node.Method.Name == nameof(Queryable.All) && node.Arguments.Count > 1 && StripQuotes(node.Arguments[1]) is LambdaExpression allPred)
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
            var subPlan = TranslateInSubContext(source, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subMapping);
            _mapping = subMapping;
            MergeSubPlanParameters(subPlan);
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
            _ctx.Provider.ApplyPaging(subSqlBuilder, 1, null, null, null);
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
        internal static bool TryGetConstantValue(Expression e, out object? value)
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
                // MethodCallExpression handling was intentionally removed to prevent RCE.
                // Method calls are translated to SQL (e.g., string.Contains) or throw NotSupportedException.
                // Executing arbitrary user code via Invoke() would be a critical security vulnerability.
            }
            value = null;
            return false;
        }
        private void MergeSubPlanParameters(QueryPlan subPlan)
        {
            var compiledSubPlanParameters = subPlan.CompiledParameters.Count == 0
                ? null
                : new HashSet<string>(subPlan.CompiledParameters, StringComparer.Ordinal);

            foreach (var parameter in subPlan.Parameters)
            {
                _params[parameter.Key] = parameter.Value;
                if (compiledSubPlanParameters?.Contains(parameter.Key) == true && !_compiledParams.Contains(parameter.Key))
                    _compiledParams.Add(parameter.Key);

                AdvanceParameterIndexPast(parameter.Key);
            }
        }

        private void AdvanceParameterIndexPast(string parameterName)
        {
            var generatedPrefix = _ctx.Provider.ParamPrefix + "p";
            if (!parameterName.StartsWith(generatedPrefix, StringComparison.Ordinal))
                return;

            var indexText = parameterName.Substring(generatedPrefix.Length);
            if (!int.TryParse(indexText, out var index))
                return;

            var nextIndex = index + 1;
            if (_parameterManager.Index < nextIndex)
                _parameterManager.Index = nextIndex;
        }

        private void AddParameter(string name, object? value)
        {
            _params[name] = value ?? DBNull.Value;
            if (!_compiledParams.Contains(name))
            {
                _compiledParams.Add(name);
            }
        }
        private TranslationContextSnapshot CaptureContext()
        {
            return new TranslationContextSnapshot(
                _clauses,
                _includes,
                _projection,
                _isAggregate,
                _methodName,
                _groupJoinInfo,
                _joinCounter,
                _singleResult,
                _noTracking,
                _splitQuery,
                _mapping,
                _rootType,
                _estimatedTimeout,
                _isCacheable,
                _cacheExpiration,
                _asOfTimestamp,
                _recursionDepth);
        }
        private void RestoreContext(TranslationContextSnapshot snapshot)
        {
            _clauses = snapshot.Clauses;
            _includes = snapshot.Includes;
            _projection = snapshot.Projection;
            _isAggregate = snapshot.IsAggregate;
            _methodName = snapshot.MethodName;
            _groupJoinInfo = snapshot.GroupJoinInfo;
            _joinCounter = snapshot.JoinCounter;
            _singleResult = snapshot.SingleResult;
            _noTracking = snapshot.NoTracking;
            _splitQuery = snapshot.SplitQuery;
            _mapping = snapshot.Mapping;
            _rootType = snapshot.RootType;
            _estimatedTimeout = snapshot.EstimatedTimeout;
            _isCacheable = snapshot.IsCacheable;
            _cacheExpiration = snapshot.CacheExpiration;
            _asOfTimestamp = snapshot.AsOfTimestamp;
            _recursionDepth = snapshot.RecursionDepth;
        }
        private QueryPlan TranslateInSubContext(Expression e, TableMapping mapping, int parameterIndex, int joinStart, int recursionDepth, out TableMapping resultingMapping)
        {
            var snapshot = CaptureContext();
            _contextStack.Push(snapshot);
            try
            {
                _clauses = new SqlBuilder();
                _includes = new List<IncludePlan>();
                _projection = null;
                _clientProjection = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _groupJoinInfo = null;
                _joinCounter = joinStart;
                _singleResult = false;
                _noTracking = false;
                _splitQuery = false;
                _mapping = mapping;
                _rootType = mapping.Type;
                _estimatedTimeout = _ctx.Options.TimeoutConfiguration.BaseTimeout;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
                _parameterManager.Index = parameterIndex;
                _recursionDepth = recursionDepth;
                _tables.Add(mapping.TableName);
                var plan = Translate(e);
                resultingMapping = _mapping;
                return plan;
            }
            finally
            {
                var subClauses = _clauses;
                var contextToRestore = _contextStack.Pop();
                RestoreContext(contextToRestore);
                subClauses.Dispose();
            }
        }
        private readonly struct TranslationContextSnapshot
        {
            public TranslationContextSnapshot(
                SqlBuilder clauses,
                List<IncludePlan> includes,
                LambdaExpression? projection,
                bool isAggregate,
                string methodName,
                GroupJoinInfo? groupJoinInfo,
                int joinCounter,
                bool singleResult,
                bool noTracking,
                bool splitQuery,
                TableMapping mapping,
                Type? rootType,
                TimeSpan estimatedTimeout,
                bool isCacheable,
                TimeSpan? cacheExpiration,
                DateTime? asOfTimestamp,
                int recursionDepth)
            {
                Clauses = clauses;
                Includes = includes;
                Projection = projection;
                IsAggregate = isAggregate;
                MethodName = methodName;
                GroupJoinInfo = groupJoinInfo;
                JoinCounter = joinCounter;
                SingleResult = singleResult;
                NoTracking = noTracking;
                SplitQuery = splitQuery;
                Mapping = mapping;
                RootType = rootType;
                EstimatedTimeout = estimatedTimeout;
                IsCacheable = isCacheable;
                CacheExpiration = cacheExpiration;
                AsOfTimestamp = asOfTimestamp;
                RecursionDepth = recursionDepth;
            }
            public SqlBuilder Clauses { get; }
            public List<IncludePlan> Includes { get; }
            public LambdaExpression? Projection { get; }
            public bool IsAggregate { get; }
            public string MethodName { get; }
            public GroupJoinInfo? GroupJoinInfo { get; }
            public int JoinCounter { get; }
            public bool SingleResult { get; }
            public bool NoTracking { get; }
            public bool SplitQuery { get; }
            public TableMapping Mapping { get; }
            public Type? RootType { get; }
            public TimeSpan EstimatedTimeout { get; }
            public bool IsCacheable { get; }
            public TimeSpan? CacheExpiration { get; }
            public DateTime? AsOfTimestamp { get; }
            public int RecursionDepth { get; }
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

        private bool TryBindPagingParameter(Expression expression, out string parameterName)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;

            if (expression is ParameterExpression parameter)
            {
                if (!_paramMap.TryGetValue(parameter, out parameterName!))
                {
                    parameterName = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                    AddParameter(parameterName, DBNull.Value);
                    _paramMap[parameter] = parameterName;
                }
                return true;
            }

            if (expression is MemberExpression member && HasUncorrelatedParameterRoot(member))
            {
                parameterName = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                AddParameter(parameterName, DBNull.Value);
                return true;
            }

            parameterName = string.Empty;
            return false;
        }

        private bool HasUncorrelatedParameterRoot(MemberExpression member)
        {
            Expression? current = member.Expression;
            while (current is MemberExpression nested)
                current = nested.Expression;

            return current is ParameterExpression parameter && !_correlatedParams.ContainsKey(parameter);
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
                AddParameter(paramName, node.Value);
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
            AddParameter(paramName, DBNull.Value);
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
                    // Use TryGetValue to prevent KeyNotFoundException on unmapped properties.
                    if (!info.Mapping.ColumnsByName.TryGetValue(node.Member.Name, out var col))
                    {
                        // Check if it's a navigation property
                        if (info.Mapping.Relations.ContainsKey(node.Member.Name))
                        {
                            throw new NormQueryException(
                                $"Navigation property '{node.Member.Name}' cannot be used directly in queries. " +
                                "Use Include() to load related entities or project specific properties.");
                        }

                        throw new NormQueryException(
                            $"Property '{node.Member.Name}' on type '{info.Mapping.Type.Name}' is not mapped to a database column. " +
                            "Ensure the property has a [Column] attribute or is included in the entity configuration.");
                    }
                    _sql.Append($"{info.Alias}.{col.EscCol}");
                }
                else
                {
                    // Use TryGetValue to prevent KeyNotFoundException on unmapped properties.
                    if (!_mapping.ColumnsByName.TryGetValue(node.Member.Name, out var col))
                    {
                        // Check if it's a navigation property
                        if (_mapping.Relations.ContainsKey(node.Member.Name))
                        {
                            throw new NormQueryException(
                                $"Navigation property '{node.Member.Name}' cannot be used directly in queries. " +
                                "Use Include() to load related entities or project specific properties.");
                        }

                        throw new NormQueryException(
                            $"Property '{node.Member.Name}' on type '{_mapping.Type.Name}' is not mapped to a database column. " +
                            "Ensure the property has a [Column] attribute, [NotMapped] is not applied, " +
                            "or the property is included in the entity configuration.");
                    }
                    _sql.Append(col.EscCol);
                }
                return node;
            }
            if (TryGetConstantValue(node, out var value))
            {
                var paramName = _ctx.Provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                AddParameter(paramName, value ?? DBNull.Value);
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
        private static string? ExtractPropertyName(Expression expression)
        {
            return expression switch
            {
                MemberExpression member => member.Member.Name,
                UnaryExpression { Operand: MemberExpression member } => member.Member.Name,
                _ => null
            };
        }
    }
}
