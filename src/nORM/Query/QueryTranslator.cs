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
using nORM.Configuration;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;
#nullable enable
namespace nORM.Query
{
    /// <summary>
    /// Translates LINQ expression trees to SQL queries using a pooled, stateful visitor.
    /// </summary>
    /// <remarks>
    /// Top-level translators are pooled. Nested subqueries use stack-based context snapshots
    /// so parameters, aliases, projection state, and recursion depth unwind deterministically.
    /// The recursion depth limit is configured by <see cref="nORM.Configuration.DbContextOptions.MaxRecursionDepth"/>.
    /// </remarks>
    // Runtime LINQ translation is a dynamic surface: it can route to client-evaluation
    // fallbacks that instantiate generics and build delegates at runtime. The class-level
    // annotations cover every member; nested translator classes carry the same annotations
    // because nested types do not inherit them from the containing type.
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
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
        private Dictionary<string, nORM.Mapping.IValueConverter> _paramConverters => _parameterManager.CompiledParameterConverters;
        private Dictionary<ParameterExpression, string> _paramMap { get => _parameterManager.ParameterMap; set => _parameterManager.ParameterMap = value; }
        internal int ParameterIndex => _parameterManager.Index;
        private List<IncludePlan> _includes = new();
        private List<M2MIncludePlan> _m2mIncludes = new();
        private LambdaExpression? _projection;
        // Records the SelectMany result selector that builds a transparent identifier
        // (e.g. `(l, r) => new { l, r }`). Used by ExpandProjection to inline `t.l` /
        // `t.r` references in downstream Where / Select lambdas back to the join's
        // outer / inner parameters - separate from _projection so the outer Select's
        // materializer projection isn't clobbered by the TI lambda.
        private LambdaExpression? _transparentIdentifier;
        private Func<object, object>? _clientProjection;
        // When the projection is split (server-side fetch + client-side transform), the server
        // projection's body type is the intermediate row, not what the caller sees. Recording the
        // original lambda's result type here lets plan.ElementType reflect the FINAL shape so
        // CreateList<T> and tracking checks operate against the right type.
        private Type? _clientProjectionResultType;
        // For composite GroupBy keys (p => new { p.A, p.B }), each anonymous-type member's
        // SQL fragment is recorded here so the result-projection path can resolve `g.Key.A`
        // back to the right grouped column.
        private readonly Dictionary<string, string> _compositeKeyMemberSql = new(StringComparer.Ordinal);
        // Set by HandleGroupBy when the GroupBy source is a Take/Skip-windowed
        // sub-plan. BuildGroupBySelectClause uses these instead of `_mapping.EscTable`
        // to emit `FROM (windowedSubSql) AS alias` so the GROUP BY aggregates the
        // windowed rows only. Cleared in Clear().
        private string? _windowedGroupBySubSql;
        private string? _windowedGroupByAlias;
        // Set by TranslateAfterTakeSkipWindow (Where/OrderBy/etc.) to track the alias
        // of the most recently built derived-table wrap. Regular Where/OrderBy paths
        // use this when their lambda parameter isn't yet in _correlatedParams.
        private string? _outerDerivedAlias;
        private bool _isAggregate;
        private string _methodName = "";
        private Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _correlatedParams = new();
        private GroupJoinInfo? _groupJoinInfo;
        // GroupJoin's result selector lambda preserved so downstream Where/OrderBy can
        // expand `r.Member` references back to the underlying outer/group expressions.
        // _projection isn't used for GroupJoin (it has 2 params and the materializer
        // uses _groupJoinInfo.ResultSelector - a compiled Func - instead), so we need
        // a separate channel for ExpandProjection.
        private LambdaExpression? _groupJoinResultSelector;
        private LambdaExpression? _groupJoinExpansionSelector;
        private LambdaExpression? _groupByElementSelector;
        // The GroupBy key selector, retained so the projection builder can re-render the key
        // against a subquery alias when emitting a greatest-N-per-group correlated subquery.
        private LambdaExpression? _groupByKeySelector;
        // The source chain's Where predicates below GroupBy, retained so the
        // greatest-N-per-group subquery re-applies them — it re-scans the base
        // table, so a dropped filter would let excluded rows win the ordering.
        // Null when the source chain holds operators the re-scan cannot honor.
        private List<LambdaExpression>? _groupOrderedFirstSourceWheres;
        // Set when a subquery fragment baked closure VALUES into its SQL instead of
        // registering compiled parameters (a second registration for a lambda the
        // outer translation already registered would shift positional bindings).
        // Marks the plan execution-specific: translate fresh, skip both caches.
        private bool _closureFoldedIntoSql;
        // The alias THIS translator bound its root lambda parameter to. Build's FROM
        // clause previously reverse-looked-up the alias by mapping in the SHARED
        // correlated dict — ambiguous when a nested correlated subquery targets the
        // SAME entity type as an outer scope (both entries carry the same mapping),
        // which emitted the outer scope's alias and broke every inner reference.
        private string? _selfRootAlias;
        // Set when the query root is a FromSqlRaw source: the raw SQL wrapped as a derived table replaces the
        // mapped table's FROM clause (see PlanGeneration), so LINQ operators compose on top of it.
        private string? _rawSqlSource;
        private int _joinCounter;
        private DatabaseProvider _provider = null!;
        private bool _singleResult;
        private bool _noTracking;
        // AsNoTrackingWithIdentityResolution() sets this alongside _noTracking: untracked results, but the
        // root materialize loop still collapses a repeated root key to one shared instance.
        private bool _identityResolution;
        // AsTracking() sets this to force tracking ON over a NoTracking context default. _trackingDecided
        // gives AsNoTracking/AsTracking last-wins semantics: the outermost (last-written, first-visited)
        // operator locks the decision so an inner one cannot override it.
        private bool _forceTracking;
        private bool _trackingDecided;
        private bool _splitQuery;
        // Comments captured from TagWith(...) and prepended to the generated SQL as line comments.
        private List<string>? _queryTags;
        // Set by TakeLast/SkipLast translators after flipping ORDER BY direction +
        // applying Take/Skip. The materializer reverses the final list so the caller
        // sees rows in the original ORDER BY direction.
        internal bool _postReverseResult;
        // Post-materialize transform hook for explicitly in-memory terminal adapters.
        // Keyed set operators now emit provider SQL and should not use this field.
        internal System.Func<DbContext, System.Collections.IList, System.Collections.IList>? _postMaterializeTransform;
        private System.Func<DbContext, System.Collections.IList, System.Collections.IList>? _postMaterializeOrderPrefixTransform;
        private List<(System.Func<object, object> KeyReader, bool Ascending)>? _postMaterializeOrderings;
        private Type? _postMaterializeElementType;
        // Set when a scalar aggregate is computed client-side over a client-tail
        // reshaped sequence; the post-materialize transform reduces the rows to a
        // single boxed value and the executor unwraps it as the query result.
        private bool _clientScalarResult;
        // Set when a SelectMany(... DefaultIfEmpty()) flatten with no result selector
        // makes the inner ENTITY the query result: unmatched outer rows come back with
        // every inner column NULL and must materialize as null list elements (LINQ
        // DefaultIfEmpty semantics), not crash the non-null column readers.
        private bool _flattenedLeftJoinEntityResult;
        // Stored by HandleGroupBy when a 2-arg GroupBy with no downstream result selector
        // is detected. Generate() inspects this after visiting the full expression tree: if
        // no projection was set (streaming case), _groupBy is cleared and a client-side
        // grouping transform is installed via InstallGroupingTransform().
        private LambdaExpression? _streamingGroupByKeySelector;
        private HashSet<string> _tables = new();
        private readonly Stack<TranslationContextSnapshot> _contextStack = new();
        private List<PropertyInfo> _detectedCollections = new();
        // Rendered filter fragments for shaped collection projections (Lines = o.Lines.Where(pred).ToList()),
        // keyed by nav property; consumed by BuildDependentQueryDefinitions.
        private Dictionary<PropertyInfo, SelectClauseVisitor.RenderedCollectionFilter> _detectedCollectionFilters = new();
        // Element projections for shaped collection projections (Lines = o.Lines.Select(l => new Dto{...}).ToList()),
        // keyed by nav property; consumed by BuildDependentQueryDefinitions to shape the child fetch.
        private Dictionary<PropertyInfo, System.Linq.Expressions.LambdaExpression> _detectedCollectionProjections = new();
        // DTO binding member each detected collection is assigned to (keyed by nav property) — the stitch
        // target when the projection property is named differently from the navigation.
        private Dictionary<PropertyInfo, PropertyInfo> _detectedCollectionTargetMembers = new();
        // Rendered ORDER BY + row cap/skip for an ordered/top-N shaped collection projection, keyed by nav
        // property; consumed by BuildDependentQueryDefinitions to emit a ROW_NUMBER partition window.
        private Dictionary<PropertyInfo, SelectClauseVisitor.RenderedCollectionOrdering> _detectedCollectionOrderings = new();
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
        private List<string> _groupByOrdinalExtras => _clauses.GroupByOrdinalExtras;
        private int? _take { get => _clauses.Take; set => _clauses.Take = value; }
        private int? _skip { get => _clauses.Skip; set => _clauses.Skip = value; }
        private string? _takeParam { get => _clauses.TakeParam; set => _clauses.TakeParam = value; }
        internal string? _fromSuffix { get => _clauses.FromSuffix; set => _clauses.FromSuffix = value; }
        private string? _skipParam { get => _clauses.SkipParam; set => _clauses.SkipParam = value; }
        // True when _take was set by a terminal operator (First / FirstOrDefault / Single /
        // SingleOrDefault / Last / LastOrDefault / ElementAt / ElementAtOrDefault) rather
        // than by a user-facing Take()/Skip(). Used by the post-Take/Skip silent-wrongness
        // pin family (bca0523 / 47acc83 / 54c16ae / 4fcd795 / c2cce55 / 3427495 / 3716e13 /
        // f0ccf06 / b4f5ae4) to AVOID false-positives on `q.OrderBy(k).First()` -
        // ordering BEFORE a terminal LIMIT is correct, and only ordering AFTER a USER
        // Take/Skip is the silent-wrongness shape the pins guard.
        private bool _takeSetByTerminal { get => _clauses.TakeSetByTerminal; set => _clauses.TakeSetByTerminal = value; }
        private bool _isDistinct { get => _clauses.IsDistinct; set => _clauses.IsDistinct = value; }

        // Set by SetOperationTranslator before translating each UNION / INTERSECT /
        // EXCEPT arm. SQLite stores decimal as TEXT and the set-op dedup compares
        // strings, so scale variants of the same value ('10.5' vs '10.50') would
        // register as distinct rows. Each arm's decimal projections emit the
        // provider's ExactDecimalKeySql (canonical decimal text on SQLite), so the
        // dedup is scale-insensitive AND exact at full 28-digit precision - values
        // differing beyond double precision stay distinct, and the projected text
        // still materializes as the exact decimal value. See SCV.ExactDecimalProjectionKeys.
        internal bool _exactDecimalProjectionKeys;

        // Set by SetOperationTranslator around each UNION / INTERSECT / EXCEPT arm when the
        // element type is string on a CI-collation provider: string projections get the
        // value-preserving OrdinalComparableStringProjection wrap so the set-op dedups and
        // matches byte-wise like LINQ. Plain field (not part of the sub-context snapshot) so it
        // flows into TranslateSubExpression, scoped by try/finally at the set site — the exact
        // pattern _exactDecimalProjectionKeys uses.
        internal bool _forceOrdinalStringProjections;

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
                var ownsOrdinalScope = BeginClosureOrdinalScope(e);
                var ownsTableScope = BeginReferencedTableScope();
                // Only the OUTERMOST translation owns the temporal window: nested
                // sub-translations (set-operation arms, subquery sources — whether on
                // this instance or a pool-fresh one) must all see the same scope,
                // wherever in the tree the AsOf node sits. The referenced-table scope
                // already answers "is this the outermost translation on this thread"
                // for exactly that mix of nesting shapes, so its ownership doubles as
                // the temporal scope's. The pre-scan opens the window before any arm
                // translates so translation order never decides which arms get windowed.
                var ownsTemporalScope = ownsTableScope;
                try
                {
                    if (ownsTemporalScope)
                        TryOpenTemporalScopeForTranslation(e);
                    return new TranslationBuilder(this, e)
                        .Validate()
                        .Setup()
                        .Generate();
                }
                finally
                {
                    if (ownsOrdinalScope)
                        EndClosureOrdinalScope();
                    if (ownsTableScope)
                        EndReferencedTableScope();
                    if (ownsTemporalScope)
                    {
                        EndTemporalTableSourceScope();
                        _asOfTagResolution = null;
                    }
                }
            }
        }
        private TableMapping TrackMapping(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            var map = _ctx?.GetMapping(type) ?? throw new InvalidOperationException("Context not available");
            _tables.Add(map.TableName);
            RecordReferencedTable(map.TableName);
            return map;
        }

        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(node))]
        public override Expression? Visit(Expression? node)
        {
            // Translation recurses once per expression node with large frames (each
            // operator's translator plus visitor plumbing). A deeply nested tree —
            // thousands of chained operators — would otherwise end in an uncatchable
            // StackOverflowException that kills the process; fail with a catchable
            // query exception while headroom remains.
            if (!System.Runtime.CompilerServices.RuntimeHelpers.TryEnsureSufficientExecutionStack())
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    "Expression tree is nested too deeply to translate. Break the query into smaller operations."));
            return base.Visit(node);
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
            // Reject unsupported Queryable / Enumerable method calls on a nORM query with a
            // stable nORM exception. Without this guard, methods like SequenceEqual or
            // unsupported Cast/OfType variants either silently pass through (returning
            // wrong results) or leak provider-specific exceptions on execution.
            var declaring = node.Method.DeclaringType;
            if (declaring == typeof(System.Linq.Queryable) || declaring == typeof(System.Linq.Enumerable))
            {
                throw new NormUnsupportedFeatureException(
                    $"LINQ method '{_methodName}' is not supported by the nORM v1 query translator. " +
                    "See docs/linq-support.md for the supported operator matrix.");
            }
            return base.VisitMethodCall(node);
        }
        private Expression HandleSetOperation(MethodCallExpression node)
        {
            _isAggregate = true;
            _singleResult = true;
            // Any/All/Contains return a scalar bool via `SELECT 1 WHERE EXISTS(...)`.
            // If the source was a projected set-op (e.g. `q1.Select(p => new { V = p.V }).Union(q2).Any()`),
            // SetOperationTranslator would have lifted the anonymous-typed projection onto
            // _projection, and Generate() would then build a materializer against that
            // anonymous type -- which throws "No suitable constructor for AnonymousType"
            // because the actual EXISTS result is a single bool column. Clear _projection
            // so the scalar materializer path is taken (mirror of CountTranslator's
            // _projection = null behaviour for the same reason).
            _projection = null;
            var source = node.Arguments[0];
            var genericArgs = source.Type.GetGenericArguments();
            if (genericArgs.Length == 0)
                throw new NormQueryException(
                    string.Format(ErrorMessages.QueryTranslationFailed,
                    $"Expected a generic IQueryable<T> source type but found '{source.Type.Name}'."));
            var elementType = genericArgs[0];
            // Any/All/Contains over a Take/Skip-windowed source: the predicate must
            // see only the windowed rows, not the full table. SQL evaluation order
            // applies WHERE before LIMIT, so a flat rewrite `Where(Take(N, q), pred)`
            // would filter the full table first and the windowing semantics would
            // be lost. Detect the shape, translate the windowed source AS-IS, and
            // wrap it as a derived table inside the EXISTS - `EXISTS(SELECT 1 FROM
            // (windowedSource) AS t WHERE pred LIMIT 1)` - so the predicate runs
            // against the windowed rowset.
            bool sourceWindowed = node.Method.Name is nameof(Queryable.Any) or nameof(Queryable.All) or nameof(Queryable.Contains)
                && SourceHasTakeOrSkip(source);
            if (sourceWindowed)
            {
                LambdaExpression? windowedPred = null;
                var windowedSource = source;
                // Predicate-bearing forms come in two shapes depending on the call site:
                //   (a) 2-arg `Queryable.Any(source, pred)` - predicate at node.Arguments[1].
                //   (b) 1-arg `Queryable.Any(Where(source, pred))` - async-extensions
                //       pre-inject a Where wrapper. Peel it off so we translate the
                //       inner windowed source as a derived table.
                if (node.Method.Name == nameof(Queryable.Any) && node.Arguments.Count > 1
                    && StripQuotes(node.Arguments[1]) is LambdaExpression wAny)
                {
                    windowedPred = wAny;
                }
                else if (node.Method.Name == nameof(Queryable.All) && node.Arguments.Count > 1
                    && StripQuotes(node.Arguments[1]) is LambdaExpression wAll)
                {
                    var pAll = wAll.Parameters[0];
                    windowedPred = Expression.Lambda(Expression.Not(wAll.Body), pAll);
                }
                else if (node.Method.Name == nameof(Queryable.Contains) && node.Arguments.Count == 2)
                {
                    // Contains over a projected source - `Select(Take(...), p => p.V).Contains(value)` -
                    // peels the Select so we apply the predicate against the underlying entity in
                    // the derived table, where the column references resolve naturally.
                    if (source is MethodCallExpression projSel
                        && projSel.Method.Name == nameof(Queryable.Select)
                        && projSel.Arguments.Count == 2
                        && StripQuotes(projSel.Arguments[1]) is LambdaExpression projLambda)
                    {
                        var entityElem = projSel.Arguments[0].Type.GetGenericArguments()[0];
                        var pCtE = projLambda.Parameters[0];
                        var converted = Expression.Convert(node.Arguments[1], projLambda.Body.Type);
                        windowedPred = Expression.Lambda(Expression.Equal(projLambda.Body, converted), pCtE);
                        windowedSource = projSel.Arguments[0];
                        elementType = entityElem;
                    }
                    else
                    {
                        var pCt = Expression.Parameter(elementType, "x");
                        windowedPred = Expression.Lambda(Expression.Equal(pCt, Expression.Convert(node.Arguments[1], elementType)), pCt);
                    }
                }
                else if (source is MethodCallExpression wrapWhere
                         && wrapWhere.Method.Name == nameof(Queryable.Where)
                         && wrapWhere.Arguments.Count == 2
                         && StripQuotes(wrapWhere.Arguments[1]) is LambdaExpression wrapPred)
                {
                    var inverted = node.Method.Name == nameof(Queryable.All)
                        ? Expression.Lambda(Expression.Not(wrapPred.Body), wrapPred.Parameters[0])
                        : wrapPred;
                    windowedPred = inverted;
                    windowedSource = wrapWhere.Arguments[0];
                }
                var subPlanWin = TranslateInSubContext(windowedSource, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subMappingWin);
                _mapping = subMappingWin;
                MergeSubPlanParameters(subPlanWin);
                using var winBuilder = new OptimizedSqlBuilder();
                var subAlias = "__win" + _joinCounter++;
                winBuilder.Append("SELECT 1 FROM (");
                winBuilder.Append(subPlanWin.Sql);
                winBuilder.Append(") AS ").Append(_provider.Escape(subAlias));
                if (windowedPred != null)
                {
                    var vctx = new VisitorContext(_ctx, subMappingWin, _provider, windowedPred.Parameters[0], _provider.Escape(subAlias), _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth + 1, _params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx);
                    var predSql = visitor.Translate(windowedPred.Body);
                    foreach (var kvp in visitor.GetParameters())
                        _params[kvp.Key] = kvp.Value;
                    FastExpressionVisitorPool.Return(visitor);
                    winBuilder.Append(" WHERE ").Append(predSql);
                }
                _provider.ApplyPaging(winBuilder, 1, null, null, null);
                _sql.Append(node.Method.Name == nameof(Queryable.All)
                    ? "SELECT 1 WHERE NOT EXISTS("
                    : "SELECT 1 WHERE EXISTS(");
                _sql.Append(winBuilder.ToSqlString());
                _sql.Append(")");
                return node;
            }
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
            _ctx.RawProvider.ApplyPaging(subSqlBuilder, 1, null, null, null);
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
        internal static bool SourceHasTakeOrSkip(Expression source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                if (mce.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
                    return true;
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
            return false;
        }

        /// <summary>
        /// True when the source spine contains a set operation (Union/Concat/Intersect/Except) or a Distinct.
        /// A GROUP BY cannot sit directly on top of a compound or DISTINCT SELECT, so such a source must be
        /// wrapped as a derived table (<c>FROM (&lt;compound&gt;) AS alias</c>) before the group key can resolve.
        /// Walks the source chain only (arg 0), never lambda arguments, so a set-op buried inside a predicate
        /// subquery does not falsely trigger the wrap. Catches wrapper-over-set-op shapes such as
        /// <c>Concat(...).Distinct().GroupBy(...)</c> and <c>Concat(...).Where(...).GroupBy(...)</c> that a
        /// bare immediate-source check misses.
        /// </summary>
        internal static bool SourceHasSetOpOrDistinct(Expression source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                if (mce.Method.Name is "Union" or "Concat" or "Intersect" or "Except" or nameof(Queryable.Distinct))
                    return true;
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
            return false;
        }

        /// <summary>
        /// Strips a trailing ORDER BY so the SQL can embed in a derived table —
        /// unless a paging clause follows it. Every provider emits its paging
        /// tokens (LIMIT/OFFSET/FETCH) after the ORDER BY they depend on, so
        /// cutting from the ORDER BY would amputate the paging clause and
        /// silently widen the row set (TakeLast/SkipLast and Take/Skip windows
        /// page this way).
        /// </summary>
        internal static string RemoveTrailingOrderByUnlessPaged(string sql)
        {
            var idx = sql.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (idx < 0) return sql;
            var tail = sql.AsSpan(idx);
            if (tail.Contains(" LIMIT ", StringComparison.OrdinalIgnoreCase)
                || tail.Contains(" OFFSET ", StringComparison.OrdinalIgnoreCase)
                || tail.Contains(" FETCH ", StringComparison.OrdinalIgnoreCase))
                return sql;
            return sql[..idx];
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
                var paramName = _ctx.RawProvider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
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
            // __qv marks a whole-query-parameter slot so the compiled-query pipeline
            // pairs it by name instead of document-order position.
            var paramName = _ctx.RawProvider.ParamPrefix + "p" + _parameterManager.GetNextIndex() + "__qv";
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
            if (TableMapping.TryGetMemberAccessRoot(node, out var rootParameter))
            {
                if (_correlatedParams.TryGetValue(rootParameter, out var info))
                {
                    // Use TryGetValue to prevent KeyNotFoundException on unmapped properties.
                    if (!info.Mapping.TryGetColumnForMemberAccess(node, out var col))
                    {
                        // Check if it's a navigation property
                        if (node.Expression is ParameterExpression && info.Mapping.Relations.ContainsKey(node.Member.Name))
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
                    if (!_mapping.TryGetColumnForMemberAccess(node, out var col))
                    {
                        // Check if it's a navigation property
                        if (node.Expression is ParameterExpression && _mapping.Relations.ContainsKey(node.Member.Name))
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
                var paramName = _ctx.RawProvider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
                AddParameter(paramName, value ?? DBNull.Value);
                _sql.Append(paramName);
                return node;
            }
            throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, $"Member '{node.Member.Name}'"));
        }
    }
}
