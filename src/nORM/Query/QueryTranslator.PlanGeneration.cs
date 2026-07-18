using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
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
                            var paramName = _t._ctx.RawProvider.ParamPrefix + "p" + _t._parameterManager.GetNextIndex();
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

                // Streaming GroupBy: GroupBy(source, key) with no downstream projection,
                // no aggregate terminal (Count/Sum/etc.), and no HAVING filter means the
                // caller wants IGrouping<K, V> elements. Discard the SQL GROUP BY (which
                // the entity materializer can't handle) and install a client-side grouping
                // transform that groups the plain entity rows in memory.
                if (_t._streamingGroupByKeySelector != null
                    && _t._sql.Length == 0
                    && _t._groupBy.Count > 0
                    && _t._projection == null
                    && _t._windowedGroupBySubSql == null
                    && !_t._isAggregate
                    && _t._having.Length == 0)
                {
                    _t._groupBy.Clear();
                    _t._groupByOrdinalExtras.Clear();
                    _t.InstallGroupingTransform(_t._streamingGroupByKeySelector, _t._groupByElementSelector);
                }

                if (_t._clauses.WindowFunctions.Count > 0 && _t._projection == null)
                    _t._projection = _t._clauses.WindowFunctions[^1].ResultSelector;

                _t.RewritePrebuiltJoinProjectionIfNeeded();

                var materializerType = _t._groupJoinInfo?.OuterType ?? _t._projection?.Body.Type ?? _t._rootType ?? _t._mapping.Type;
                var topLevelMethodName = (_expression as MethodCallExpression)?.Method.Name;
                if (_t._isAggregate && _t._groupBy.Count == 0 && topLevelMethodName is "Count" or "LongCount")
                {
                    materializerType = typeof(int);
                }
                // Any/All/Contains over a projected source (e.g. `q.Select(p => new {...}).Union(q2).Any()`)
                // would otherwise drag the projection's anonymous type into the materializer factory
                // even though the scalar path overwrites the materializer below with a bool reader.
                // Use a safe scalar type for the pre-build so CreateSyncMaterializer doesn't try
                // to look up an anonymous-type constructor for the EXISTS scalar result.
                else if (_t._isAggregate && _t._groupBy.Count == 0 && topLevelMethodName is "Any" or "All" or "Contains")
                {
                    materializerType = typeof(bool);
                }

                // Create both sync and async materializers.
                Func<DbDataReader, object> syncMaterializer;
                Func<DbDataReader, CancellationToken, Task<object>> materializer;

                if (_t._projection != null && _t._groupJoinInfo == null)
                {
                    // Resolve converters for projection members whose value comes from a correlated
                    // First/Last/Min/Max subquery over a converter column, so the materializer maps
                    // the provider value back (null/empty for ordinary projections — no change).
                    var subqueryConverters = QueryTranslator.ComputeProjectionSubqueryConverters(_t._projection, _t._ctx);
                    syncMaterializer = _t._materializerFactory.CreateSyncMaterializer(_t._mapping, materializerType, _t._projection, projectionSubqueryConverters: subqueryConverters);
                    materializer = _t._materializerFactory.CreateSchemaAwareMaterializer(_t._mapping, materializerType, _t._projection, projectionSubqueryConverters: subqueryConverters);
                }
                else
                {
                    syncMaterializer = _t._materializerFactory.CreateSyncMaterializer(_t._mapping, materializerType, null);
                    materializer = _t._materializerFactory.CreateMaterializer(_t._mapping, materializerType, null);
                }

                // LEFT JOIN flatten (SelectMany ... DefaultIfEmpty, no result selector)
                // with the inner entity as the result: unmatched outer rows carry NULL
                // in every inner column. A real row never has NULL key columns, so an
                // all-NULL key marks "no match" and the element materializes as null
                // (DefaultIfEmpty semantics). Wraps here rather than inside the factory
                // so cached materializers for the same mapping stay guard-free.
                if (_t._flattenedLeftJoinEntityResult && _t._projection == null && _t._groupJoinInfo == null
                    && materializerType == _t._mapping.Type && _t._mapping.KeyColumns.Length > 0)
                {
                    var keyOrdinals = _t._mapping.KeyColumns
                        .Select(k => Array.IndexOf(_t._mapping.Columns, k))
                        .Where(o => o >= 0)
                        .ToArray();
                    if (keyOrdinals.Length > 0)
                    {
                        var baseSync = syncMaterializer;
                        var baseAsync = materializer;
                        syncMaterializer = reader =>
                        {
                            foreach (var ordinal in keyOrdinals)
                                if (!reader.IsDBNull(ordinal))
                                    return baseSync(reader);
                            return null!;
                        };
                        materializer = (reader, ct) =>
                        {
                            foreach (var ordinal in keyOrdinals)
                                if (!reader.IsDBNull(ordinal))
                                    return baseAsync(reader, ct);
                            return Task.FromResult<object>(null!);
                        };
                    }
                }

                var isScalar = _t._isAggregate && _t._groupBy.Count == 0;
                if (isScalar)
                {
                    var aggMethod = (_expression as MethodCallExpression)?.Method;
                    var aggMethodName = aggMethod?.Name ?? string.Empty;
                    // Count/LongCount always return non-null integer - keep the fast path.
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
                            if (isSum) return Convert.ChangeType(0, underlyingType);
                            if (isNullableReturn) return null!;
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

                // Aggregate COUNT(*) on a pre-populated _sql (SelectMany / GroupJoin path).
                // HandleSelectMany emits `SELECT <cols> FROM Parent JOIN Child ON ...` into
                // _sql directly, then the `if (_sql.Length == 0)` branch below is skipped
                // entirely -- including the aggregate COUNT(*) prefix. Result was
                // ExecuteScalar reading the first row's first column (e.g. T1.Id == 11)
                // instead of an aggregate. Discovered in 8c12072. Rewrite the SELECT-clause
                // column list to COUNT(*) before WHERE/etc append; ORDER BY and paging are
                // meaningless on a scalar COUNT so drop them silently.
                if (_t._isAggregate && _t._groupBy.Count == 0 && _t._sql.Length > 0
                    && _t._methodName is "Count" or "LongCount")
                {
                    var sqlStr = _t._sql.ToSqlString();
                    // If the source produced a set-op SELECT (Union/Intersect/Except/
                    // Concat -> UNION ALL), the existing _sql is already a full statement
                    // like `SELECT V FROM L UNION SELECT V FROM R`. Rewriting the first
                    // SELECT to COUNT(*) breaks the UNION (arms must have the same column
                    // count and SQLite returns the COUNT of the first arm only). Wrap the
                    // whole thing in a subquery instead so we count rows of the unioned
                    // set: `SELECT COUNT(*) FROM (<set-op>) AS T0`.
                    bool isSetOpSql = sqlStr.Contains(" UNION ", StringComparison.OrdinalIgnoreCase)
                                   || sqlStr.Contains(" INTERSECT ", StringComparison.OrdinalIgnoreCase)
                                   || sqlStr.Contains(" EXCEPT ", StringComparison.OrdinalIgnoreCase);
                    if (isSetOpSql)
                    {
                        var subqueryAlias = _t.EscapeAlias("T0");
                        _t._sql.Clear();
                        _t._sql.Append("SELECT COUNT(*) FROM (").Append(sqlStr).Append(") AS ").Append(subqueryAlias);
                        _t._orderBy.Clear();
                        _t._take = null;
                        _t._takeParam = null;
                        _t._skip = null;
                        _t._skipParam = null;
                    }
                    else
                    {
                        var fromIdx = sqlStr.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
                        if (fromIdx > 0)
                        {
                            var afterFrom = sqlStr.Substring(fromIdx);
                            _t._sql.Clear();
                            _t._sql.Append("SELECT COUNT(*)").Append(afterFrom);
                            _t._orderBy.Clear();
                            _t._take = null;
                            _t._takeParam = null;
                            _t._skip = null;
                            _t._skipParam = null;
                        }
                    }
                }
                if (_t._sql.Length == 0)
                {
                    var fromClause = _t._mapping.EscTable;
                    // Prefer the alias THIS translator bound its root parameter to. The
                    // mapping-based reverse lookup below is ambiguous when a nested
                    // correlated subquery targets the SAME entity type as an outer scope:
                    // the shared dict lists the outer entry first, so the inner FROM got
                    // the outer alias and every inner reference broke. The lookup remains
                    // as the fallback for translators that never bind a root lambda.
                    var alias = _t._selfRootAlias
                        ?? (_t._correlatedParams.Count > 0
                            ? _t._correlatedParams.FirstOrDefault(kvp => kvp.Value.Mapping == _t._mapping).Value.Alias
                              ?? _t._correlatedParams.Values.FirstOrDefault().Alias
                            : null);
                    // Gate on the AMBIENT scope, not the local _asOfTimestamp: a nested
                    // sub-translation (a set-operation side, a Contains inner source, a
                    // windowed sub-plan) carries no AsOf node of its own but must still
                    // read through the top-level translation's history window.
                    if (HasActiveTemporalScope)
                    {
                        alias ??= _t.EscapeAlias("T0");
                        fromClause = TemporalTableSource(_t._mapping);
                    }
                    else if (_t._asOfTimestamp.HasValue)
                    {
                        throw new NormQueryException(
                            "Internal error: an AsOf timestamp is set but no temporal window scope is active.");
                    }
                    if (_t._isAggregate && _t._groupBy.Count == 0)
                    {
                        var prefix = PooledStringBuilder.Rent();
                        try
                        {
                            // DISTINCT over a projected shape (e.g. `Select(anon).Distinct().Count()`)
                            // must count rows of the distinct set - wrap as `SELECT COUNT(*) FROM
                            // (SELECT DISTINCT <proj> FROM ...) AS T0`. Plain `SELECT COUNT(*) FROM
                            // table` would return the full row count and ignore the distinct.
                            if (_t._isDistinct && _t._projection != null)
                            {
                                var selectVisitor = new SelectClauseVisitor(_t._mapping, _t._groupBy, _t._provider, alias, ctx: _t._ctx) { ExactDecimalProjectionKeys = true | _t._exactDecimalProjectionKeys, ForceOrdinalStringProjections = _t._forceOrdinalStringProjections || (_t._provider.DefaultStringEqualityIsCaseInsensitive && _t._projection.Body.Type != typeof(string)), SharedParams = _t._params, SharedCompiledParams = _t._compiledParams, SharedParamConverters = _t._paramConverters, OuterRowParameters = _t._projection.Parameters };
                                var projSelect = selectVisitor.Translate(_t._projection.Body);
                                var subqueryAlias = _t.EscapeAlias("T0");
                                prefix.Append("SELECT COUNT(*) FROM (SELECT DISTINCT ").Append(projSelect).Append(" FROM ").Append(fromClause);
                                if (alias != null) prefix.Append(' ').Append(alias);
                                _t._sql.Insert(0, prefix.ToString());
                                _t._sql.Append(") AS ").Append(subqueryAlias);
                            }
                            else
                            {
                                prefix.Append("SELECT COUNT(*) FROM ").Append(fromClause);
                                if (alias != null) prefix.Append(' ').Append(alias);
                                _t._sql.Insert(0, prefix.ToString());
                            }
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
                            var selectVisitor = new SelectClauseVisitor(_t._mapping, _t._groupBy, _t._provider, alias, ctx: _t._ctx) { ExactDecimalProjectionKeys = _t._isDistinct || _t._exactDecimalProjectionKeys, ForceOrdinalStringProjections = _t._forceOrdinalStringProjections || (_t._isDistinct && _t._provider.DefaultStringEqualityIsCaseInsensitive && _t._projection.Body.Type != typeof(string)), SharedParams = _t._params, SharedCompiledParams = _t._compiledParams, SharedParamConverters = _t._paramConverters, OuterRowParameters = _t._projection.Parameters };
                            select = selectVisitor.Translate(_t._projection.Body);

                            // Capture detected collections for split query processing
                            _t._detectedCollections.AddRange(selectVisitor.DetectedCollections);
                            foreach (var kvp in selectVisitor.DetectedCollectionFilters)
                                _t._detectedCollectionFilters[kvp.Key] = kvp.Value;

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
                        var emitDistinctKeyword = _t._isDistinct;
                        // C# Distinct() on strings is ordinal, but DISTINCT on CI-collation
                        // providers (MySQL, SQL Server) folds case and silently collapses "abc"
                        // and "ABC" into one value. For a scalar string projection, rewrite as
                        // GROUP BY (value, binary-value): grouping by the composite is exactly
                        // byte-wise distinctness, while the SELECT keeps returning the plain
                        // string (BINARY value alone would materialize as raw bytes on MySQL).
                        if (emitDistinctKeyword
                            && _t._provider.DefaultStringEqualityIsCaseInsensitive
                            && _t._groupBy.Count == 0
                            && _t._projection != null
                            && _t._projection.Body.Type == typeof(string)
                            && !select.Contains(" AS ", StringComparison.Ordinal))
                        {
                            _t._groupBy.Add(select);
                            _t._groupByOrdinalExtras.Add(_t._provider.ForceCaseSensitiveStringComparison(select));
                            emitDistinctKeyword = false;
                        }
                        // Scalar DateTimeOffset Distinct: .NET dedups by INSTANT, but SQLite's
                        // offset-suffixed TEXT dedups by representation and keeps same-instant
                        // values distinct. Select the canonical UTC text and group by it — the
                        // canonical form both dedups by instant and materializes as a
                        // DateTimeOffset (identical mechanism to GROUP BY key canonicalization).
                        if (emitDistinctKeyword
                            && _t._groupBy.Count == 0
                            && _t._projection != null
                            && (Nullable.GetUnderlyingType(_t._projection.Body.Type) ?? _t._projection.Body.Type) == typeof(DateTimeOffset)
                            && !select.Contains(" AS ", StringComparison.Ordinal)
                            && _t._provider.CanonicalizeDateTimeOffsetGroupKey(select) is { } canonicalDistinct)
                        {
                            select = canonicalDistinct;
                            _t._groupBy.Add(canonicalDistinct);
                            emitDistinctKeyword = false;
                        }
                        var distinct = emitDistinctKeyword ? "DISTINCT " : string.Empty;
                        var prefix = PooledStringBuilder.Rent();
                        try
                        {
                            prefix.Append("SELECT ").Append(distinct).Append(select).Append(" FROM ").Append(fromClause);
                            if (alias != null) prefix.Append(' ').Append(alias);
                            if (_t._fromSuffix != null) prefix.Append(_t._fromSuffix);
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
                {
                    _t._sql.AppendFragment(" GROUP BY ").Append(PooledStringBuilder.Join(_t._groupBy));
                    // Ordinal string grouping on CI-collation providers: the binary key forms are
                    // appended to the CLAUSE only, so the SELECT-side key resolution (which also
                    // reads _groupBy) keeps returning plain strings.
                    if (_t._groupByOrdinalExtras.Count > 0)
                        _t._sql.AppendFragment(", ").Append(PooledStringBuilder.Join(_t._groupByOrdinalExtras));
                }
                if (_t._having.Length > 0)
                    _t._sql.AppendFragment(" HAVING ").Append(_t._having.ToSqlString());
                if (_t._orderBy.Count > 0)
                    _t._sql.AppendFragment(" ORDER BY ").Append(PooledStringBuilder.JoinOrderBy(_t._orderBy));
                _t._ctx.RawProvider.ApplyPaging(_t._sql, _t._take, _t._skip, _t._takeParam, _t._skipParam);
                var singleResult = _t._singleResult || _t._methodName is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault"
                    or "ElementAt" or "ElementAtOrDefault" or "Last" or "LastOrDefault" || isScalar;
                // When the projection was split for client-side evaluation, the FINAL row shape
                // is the original projection's body type, not the server-side intermediate.
                var elementType = _t._postMaterializeElementType ?? _t._groupJoinInfo?.ResultType ?? _t._clientProjectionResultType ?? materializerType;
                // A window wrap (Where/OrderBy after Take/Skip) consumes the paging
                // fields into its derived-table SQL, so the query is still paged even
                // when no take/skip field survives — mark it via the wrap alias.
                var bulkCudShape = new BulkCudQueryShape(
                    _t._where.Length > 0 ? " WHERE " + _t._where.ToSqlString() : string.Empty,
                    _t._groupBy.Count > 0,
                    _t._orderBy.Count > 0,
                    _t._having.Length > 0,
                    _t._complexityMetrics.JoinCount > 0,
                    _t._isDistinct,
                    _t._take.HasValue || _t._skip.HasValue || _t._takeParam != null || _t._skipParam != null || _t._outerDerivedAlias != null);

                // Build dependent query definitions for nested collections
                List<DependentQueryDefinition>? dependentQueries = null;
                if (_t._detectedCollections.Count > 0)
                {
                    dependentQueries = _t.BuildDependentQueryDefinitions();
                }

                // Tables the query READS beyond the semantic set (correlated-subquery and
                // navigation-aggregate tables, whose sub-translators' own _tables are discarded).
                // These go into CacheTables for result-cache invalidation but MUST NOT enter
                // _tables — ExecuteUpdate/ExecuteDelete branch on Tables.Count to pick their
                // single- vs multi-table form, and a subquery-only table there would misgenerate.
                IReadOnlyCollection<string>? cacheTables = null;
                if (CurrentReferencedTables is { Count: > 0 } referencedTables
                    && !referencedTables.All(_t._tables.Contains))
                {
                    var union = new HashSet<string>(_t._tables, StringComparer.Ordinal);
                    union.UnionWith(referencedTables);
                    cacheTables = union;
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
                    // Result-cache keys derive from SQL + parameters, but client-tail
                    // transforms bake captured values (an appended element, a compiled
                    // client filter, a chunk size) that never reach either — two queries
                    // differing only in those values would share a key and replay each
                    // other's results. Transform-carrying plans are never result-cached.
                    _t._isCacheable && _t._postMaterializeTransform == null,
                    _t._cacheExpiration,
                    Take: _t._take,
                    DependentQueries: dependentQueries,
                    ClientProjection: _t._clientProjection,
                    Complexity: _t._complexityMetrics,
                    M2MIncludes: _t._m2mIncludes.Count > 0 ? new List<M2MIncludePlan>(_t._m2mIncludes) : null,
                    BulkCudShape: bulkCudShape,
                    PostReverse: _t._postReverseResult,
                    PostMaterializeTransform: _t._postMaterializeTransform,
                    ClientScalar: _t._clientScalarResult,
                    ParameterConverters: _t._paramConverters.Count > 0
                        ? new Dictionary<string, nORM.Mapping.IValueConverter>(_t._paramConverters)
                        : null,
                    ClosureFoldedIntoSql: _t._closureFoldedIntoSql,
                    CompiledParameterOrdinals: SnapshotSlotOrdinals(_t._compiledParams),
                    CacheTables: cacheTables,
                    AsOfTimestamp: _t._asOfTimestamp
                );
                // A many-to-many include reads the association table, which is a raw
                // user-owned table with no history — there is no data to reconstruct the
                // association at the AsOf timestamp from, and silently joining LIVE
                // associations onto historical rows would mix eras. Fail loud instead.
                if (_t._asOfTimestamp.HasValue && plan.M2MIncludes is { Count: > 0 })
                    throw new NormUnsupportedFeatureException(
                        "Include of a many-to-many navigation cannot be combined with AsOf: the " +
                        "association table is not versioned, so the association membership at the " +
                        "requested timestamp is unknown. Query the historical entities without the " +
                        "many-to-many Include.");
                QueryPlanValidator.Validate(plan, _t._provider);
                return plan;
            }
        }
    }
}
