using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using nORM.Core;
using nORM.Internal;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
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
            _methodName = functionName;

            var param = selectorLambda.Parameters[0];
            var alias = EscapeAlias("T" + _joinCounter);
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            var columnSql = visitor.Translate(selectorLambda.Body);
            // See HandleGroupBy / OrderByTranslator / Join family: literal constants in
            // an aggregate selector (e.g. `Sum(x => x.Col ?? 0)`) must merge as
            // AddLiteralParameter so they don't get mis-flagged as compiled-runtime
            // placeholders and skipped at execution time.
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);
            _isAggregate = true;
            _sql.Clear();

            if (!AggregateFunctionMap.TryGetValue(functionName, out var sqlFunction))
                sqlFunction = functionName.ToUpperInvariant();

            // Decimal aggregates route through the provider's full-precision hook (registered
            // decimal aggregates / collation-ordered MIN-MAX on SQLite; the plain native aggregate
            // on providers with exact DECIMAL). AverageAggregateOperand is identity for decimal.
            var aggBodyType = Nullable.GetUnderlyingType(selectorLambda.Body.Type) ?? selectorLambda.Body.Type;
            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            if (aggBodyType == typeof(decimal))
            {
                _sql.Append(_provider.DecimalAggregateSql(sqlFunction, columnSql));
            }
            else
            {
                // C# Average over ints is a double; SQL Server's AVG(int) truncates to int, so its
                // provider hook casts integral operands to FLOAT (identity elsewhere).
                if (sqlFunction == "AVG")
                    columnSql = _provider.AverageAggregateOperand(columnSql, selectorLambda.Body.Type);
                _sql.AppendAggregateFunction(sqlFunction, columnSql);
            }

            return node;
        }
        private Expression HandleGroupBy(MethodCallExpression node)
        {
            // GroupBy(source, keySelector) or GroupBy(source, keySelector, resultSelector)
            var sourceQuery = node.Arguments[0];
            var keySelectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
            LambdaExpression? elementSelectorLambda = null;
            LambdaExpression? resultSelectorLambda = null;

            if (keySelectorLambda == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "GroupBy key selector must be a lambda expression"));

            // GroupBy after a Take/Skip-windowed source - wrap the windowed source as a
            // derived table so GROUP BY aggregates only the LIMITed window, not the full
            // table. Sister of the post-Take/Skip fixes in 3040f49 / e0f1397 / 99a02ce /
            // a1eb69e / bfc8180, extended to any Take/Skip in the source spine: the
            // sub-translation carries intervening operators (a post-window Where wraps
            // again inside the sub-plan), while the flat path would translate the key
            // against a fresh alias that the wrapped FROM never defines. Post-materialize
            // tails keep their client grouping route below.
            string alias;
            bool sourceIsWindowed = SourceHasTakeOrSkip(sourceQuery)
                && !SourceHasClientTailReshape(sourceQuery)
                && !SourceHasGroupJoinResultTail(sourceQuery)
                && !SourceHasRawGroupByResultTail(sourceQuery);
            if (sourceIsWindowed)
            {
                var subPlanG = TranslateInSubContext(sourceQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subMapG);
                _mapping = subMapG;
                MergeSubPlanParameters(subPlanG);
                alias = EscapeAlias("__wgb" + _joinCounter++);
                // Store the windowed sub-SQL + alias so BuildGroupBySelectClause emits
                // `FROM (subSql) AS alias` instead of the bare `FROM tableName alias`.
                _windowedGroupBySubSql = subPlanG.Sql;
                _windowedGroupByAlias = alias;
            }
            else
            {
                Visit(sourceQuery);
                alias = EscapeAlias("T" + _joinCounter);
                // Ordering before GroupBy is not preserved by LINQ, and a surviving
                // ORDER BY over ungrouped source columns is invalid SQL on
                // Postgres/MySQL (42803 / ONLY_FULL_GROUP_BY). Ordering applied to
                // the grouped projection is visited later and re-populates the list.
                _orderBy.Clear();
            }
            if (sourceQuery is MethodCallExpression selectSource
                && selectSource.Method.Name == nameof(Queryable.Select)
                && StripQuotes(selectSource.Arguments[1]) is LambdaExpression sourceProjection
                && IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == sourceProjection.Parameters[0].Type)
            {
                AppendPostMaterializeSelect(sourceProjection);
            }

            if (node.Arguments.Count == 3)
            {
                var third = StripQuotes(node.Arguments[2]) as LambdaExpression;
                if (third?.Parameters.Count == 1)
                    elementSelectorLambda = third;
                else
                    resultSelectorLambda = third;
            }
            else if (node.Arguments.Count > 3)
            {
                elementSelectorLambda = StripQuotes(node.Arguments[2]) as LambdaExpression;
                resultSelectorLambda = StripQuotes(node.Arguments[3]) as LambdaExpression;
            }

            var sourceContainsGroupBy = SourceContainsGroupBy(sourceQuery);
            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == keySelectorLambda.Parameters[0].Type)
            {
                AppendPostMaterializeGroupBy(keySelectorLambda, elementSelectorLambda);
                return node;
            }
            if (sourceContainsGroupBy
                && _projection != null
                && keySelectorLambda.Parameters.Count == 1
                && keySelectorLambda.Parameters[0].Type == _projection.Body.Type)
            {
                AppendPostMaterializeGroupBy(keySelectorLambda, elementSelectorLambda);
                return node;
            }
            if (sourceContainsGroupBy && TryCreateProjectedTailSelector(keySelectorLambda, out var projectedTailSelector))
            {
                AppendPostMaterializeGroupBy(projectedTailSelector, elementSelectorLambda);
                return node;
            }

            keySelectorLambda = ExpandProjection(keySelectorLambda);
            if (elementSelectorLambda != null)
                elementSelectorLambda = ExpandProjection(elementSelectorLambda);
            _groupByElementSelector = elementSelectorLambda;
            _groupByKeySelector = keySelectorLambda;
            // A windowed source's rows live in the derived table, which the
            // greatest-N-per-group re-scan cannot see — disable the rewrite there.
            _groupOrderedFirstSourceWheres = sourceIsWindowed ? null : ExtractGroupSourceWheres(sourceQuery);
            var param = keySelectorLambda.Parameters[0];
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);

            string groupBySql;
            // Composite key (p => new { p.A, p.B }) - translate each arg individually so the
            // emitted GROUP BY clause is comma-separated, and remember the per-member SQL so
            // the result-projection path can resolve `g.Key.A` back to its column.
            if (keySelectorLambda.Body is NewExpression compositeKey)
            {
                var parts = new List<string>(compositeKey.Arguments.Count);
                for (int i = 0; i < compositeKey.Arguments.Count; i++)
                {
                    var vctxPart = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                    var partVisitor = FastExpressionVisitorPool.Get(in vctxPart);
                    var partSql = partVisitor.Translate(compositeKey.Arguments[i]);
                    foreach (var kvp in partVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(partVisitor);
                    // Decimal columns store as TEXT; grouping on raw text treats
                    // '10.5' / '10.50' as distinct keys. The canonical decimal
                    // text keys numerically-equal values together at full
                    // precision (REAL would merge values differing beyond
                    // double's ~15-17 significant digits). Same emit applied to
                    // both the GROUP BY and the SELECT-side key-member so
                    // SQLite matches the projection back to the grouped
                    // expression, and the canonical text still parses as the
                    // materialized decimal key.
                    var partType = Nullable.GetUnderlyingType(compositeKey.Arguments[i].Type) ?? compositeKey.Arguments[i].Type;
                    if (partType == typeof(decimal))
                        partSql = _provider.ExactDecimalKeySql(partSql);
                    // .NET DateTimeOffset equality is instant-based; canonicalize the key so
                    // same-instant values at different offsets group together (SQLite TEXT
                    // storage would otherwise split them). The canonical text doubles as the
                    // selected key, so assigning BEFORE the adds covers clause and select alike.
                    if (partType == typeof(DateTimeOffset)
                        && _provider.CanonicalizeDateTimeOffsetGroupKey(partSql) is { } canonicalPart)
                        partSql = canonicalPart;
                    // Navigation-member composite parts translate to correlated scalar
                    // subqueries; providers that reject those in GROUP BY expose each
                    // part as its own applied lateral column (see the single-key path).
                    if (compositeKey.Arguments[i] is MemberExpression { Expression: MemberExpression }
                        && _provider.AppliedScalarColumnClause(partSql, EscapeAlias("GK" + i), _provider.Escape("GK")) is { } appliedPartClause)
                    {
                        _fromSuffix = (_fromSuffix ?? string.Empty) + appliedPartClause;
                        partSql = $"{EscapeAlias("GK" + i)}.{_provider.Escape("GK")}";
                    }
                    parts.Add(partSql);
                    _groupBy.Add(partSql);
                    // C# groups string keys ordinally; on CI-collation providers (MySQL,
                    // SQL Server) add the binary form as an EXTRA grouping key — the composite
                    // (key, binary-key) is exactly as fine as byte-wise grouping, the SELECT side
                    // keeps returning the plain string, and ONLY_FULL_GROUP_BY stays satisfied.
                    // Goes into GroupByOrdinalExtras, NOT _groupBy: _groupBy also feeds the
                    // SELECT-side key resolution, where the binary form would materialize as
                    // raw bytes and shift the positional materializer.
                    if (partType == typeof(string) && _provider.DefaultStringEqualityIsCaseInsensitive)
                        _groupByOrdinalExtras.Add(_provider.ForceCaseSensitiveStringComparison(partSql));
                    var memberName = compositeKey.Members?[i]?.Name ?? $"Item{i + 1}";
                    _compositeKeyMemberSql[memberName] = partSql;
                }
                groupBySql = string.Join(", ", parts);
            }
            else
            {
                // Constant key (e.g. `_ => 1` from the string-concat aggregate rewrite) means
                // "group all rows into a single group". SQL Server rejects GROUP BY <constant>
                // and MySQL ONLY_FULL_GROUP_BY rejects it too. Omit GROUP BY entirely - a plain
                // aggregate SELECT without GROUP BY gives the same single-group semantics.
                if (keySelectorLambda.Body is ConstantExpression)
                {
                    groupBySql = string.Empty;
                    // Do NOT add to _groupBy - no GROUP BY clause will be emitted.
                }
                else
                {
                    var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx2);
                    groupBySql = visitor.Translate(keySelectorLambda.Body);
                    // Use AddLiteralParameter (not AddParameter) so the inline-constant `@p0` from
                    // a COALESCE-fallback / literal-bearing key selector isn't also flagged in
                    // `_compiledParams`. Flagging it makes BindPlanParameters treat the slot as a
                    // compiled-query closure binding and skip the literal value at execution time -
                    // SQLite then throws `Must add values for the following parameters`. Matches the
                    // composite-key path above which already uses AddLiteralParameter.
                    foreach (var kvp in visitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(visitor);
                    // Decimal key: canonical exact text (see composite-key path above).
                    var keyType = Nullable.GetUnderlyingType(keySelectorLambda.Body.Type) ?? keySelectorLambda.Body.Type;
                    if (keyType == typeof(decimal))
                        groupBySql = _provider.ExactDecimalKeySql(groupBySql);
                    // Instant-based DateTimeOffset grouping (see the composite-key path above).
                    if (keyType == typeof(DateTimeOffset)
                        && _provider.CanonicalizeDateTimeOffsetGroupKey(groupBySql) is { } canonicalKey)
                        groupBySql = canonicalKey;
                    // A navigation-member key translates to a correlated scalar subquery,
                    // which SQL Server rejects inside GROUP BY and MySQL's
                    // only_full_group_by rejects on the SELECT side. Those providers
                    // expose the key as an applied lateral column and group by THAT —
                    // the dual-purpose _groupBy then serves both the clause and the
                    // SELECT-side key resolution with the applied column.
                    if (keySelectorLambda.Body is MemberExpression { Expression: MemberExpression }
                        && _provider.AppliedScalarColumnClause(groupBySql, EscapeAlias("GK0"), _provider.Escape("GK")) is { } appliedClause)
                    {
                        _fromSuffix = (_fromSuffix ?? string.Empty) + appliedClause;
                        groupBySql = $"{EscapeAlias("GK0")}.{_provider.Escape("GK")}";
                    }
                    _groupBy.Add(groupBySql);
                    // Ordinal string grouping on CI-collation providers: extra binary key (see
                    // the composite-key path above for the rationale and the extras-list note).
                    if (keyType == typeof(string) && _provider.DefaultStringEqualityIsCaseInsensitive)
                        _groupByOrdinalExtras.Add(_provider.ForceCaseSensitiveStringComparison(groupBySql));
                }
            }

            // If there's a result selector, handle the projection
            if (resultSelectorLambda != null)
            {
                var resultSelector = resultSelectorLambda;
                _projection = resultSelector;
                var sourceFromSql = ExtractSourceFromClause(_sql.ToString());

                // Clear the default select and let the projection handling rebuild it
                _sql.Clear();

                // Analyze the result selector to build appropriate SELECT clause
                BuildGroupBySelectClause(resultSelector, groupBySql, alias, sourceFromSql);
            }
            else
            {
                // 2-arg GroupBy with no result selector: the caller may want to enumerate
                // IGrouping<K, V> directly. Record the key lambda so Generate() can detect
                // this streaming case and install a client-side grouping transform instead
                // of emitting a SQL GROUP BY that the entity materializer can't handle.
                _streamingGroupByKeySelector = keySelectorLambda;
            }

            return node;
        }
        private Expression HandleDirectAggregate(MethodCallExpression node)
        {
            // Handle direct aggregate calls like query.Sum(x => x.Amount)
            var sourceQuery = node.Arguments[0];

            // A reshape anywhere in the spine (including below a Take/Skip window)
            // must divert to in-memory evaluation before the windowed derived-table
            // wrap builds aggregate SQL against pre-reshape rows.
            if (TryTranslateReshapedScalarTerminal(this, node) is { } reshapedAgg)
                return reshapedAgg;

            // Sum/Min/Max/Average over a Take/Skip-windowed source - wrap the source
            // as a derived table and emit the aggregate against the wrap alias.
            // Sister of the post-Take/Skip family, extended to any Take/Skip in the
            // source spine: the sub-translation carries intervening operators (a Where
            // after the window wraps again inside the sub-plan), while the flat path
            // below would rebuild the SELECT against the raw table and dangle the
            // derived alias. Post-materialize tails already diverted above.
            if (SourceHasTakeOrSkip(sourceQuery))
            {
                var subPlanA = TranslateInSubContext(sourceQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subMapA);
                _mapping = subMapA;
                MergeSubPlanParameters(subPlanA);
                var winAliasA = EscapeAlias("__wagg" + _joinCounter++);
                if (!AggregateFunctionMap.TryGetValue(node.Method.Name, out var aggUpper))
                    aggUpper = node.Method.Name.ToUpperInvariant();
                _isAggregate = true;
                _methodName = node.Method.Name;
                _sql.Clear();
                if (node.Arguments.Count > 1
                    && StripQuotes(node.Arguments[1]) is LambdaExpression selA)
                {
                    var sp = selA.Parameters[0];
                    _correlatedParams[sp] = (subMapA, winAliasA);
                    var vctxA = new VisitorContext(_ctx, subMapA, _provider, sp, winAliasA, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth + 1, _params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctxA);
                    var colSql = visitor.Translate(selA.Body);
                    foreach (var kvp in visitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(visitor);
                    // See the flat path below: decimal aggregates route through the provider's
                    // full-precision hook; AverageAggregateOperand is identity for decimal.
                    var selABodyType = Nullable.GetUnderlyingType(selA.Body.Type) ?? selA.Body.Type;
                    string aggCallA;
                    if (selABodyType == typeof(decimal))
                    {
                        aggCallA = _provider.DecimalAggregateSql(aggUpper, colSql);
                    }
                    else
                    {
                        // See HandleAggregate: SQL Server AVG(int) truncates; cast integral operands.
                        if (aggUpper == "AVG")
                            colSql = _provider.AverageAggregateOperand(colSql, selA.Body.Type);
                        aggCallA = $"{aggUpper}({colSql})";
                    }
                    _sql.Append("SELECT ").Append(aggCallA).Append(" FROM (")
                        .Append(subPlanA.Sql).AppendFragment(") AS ").Append(winAliasA);
                }
                return node;
            }

            var visitedSource = Visit(sourceQuery);
            // A pending client-tail reshape means the server aggregate would ignore
            // the reshaped sequence: evaluate the aggregate in memory over the
            // reshaped rows, and fail closed only when no Enumerable equivalent exists.
            if (TryAppendClientScalarAggregate(node))
                return visitedSource;
            ThrowIfClientTailReshapePending(this, node.Method.Name);
            _methodName = node.Method.Name;
            // An aggregate collapses to a single row, so the source ordering is
            // meaningless — and a surviving ORDER BY over source columns is invalid
            // SQL on Postgres (42803: column must appear in GROUP BY or aggregate).
            _orderBy.Clear();

            if (node.Arguments.Count > 1 && StripQuotes(node.Arguments[1]) is LambdaExpression selector)
            {
                var param = selector.Parameters[0];
                var alias = EscapeAlias("T" + _joinCounter);
                if (!_correlatedParams.ContainsKey(param))
                    _correlatedParams[param] = (_mapping, alias);
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var columnSql = visitor.Translate(selector.Body);
                // AddLiteralParameter - see HandleAggregateExpression above.
                foreach (var kvp in visitor.GetParameters())
                    AddLiteralParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(visitor);
                _isAggregate = true;
                _sql.Clear();

                if (!AggregateFunctionMap.TryGetValue(node.Method.Name, out var sqlFunction))
                    sqlFunction = node.Method.Name.ToUpperInvariant();

                // Decimal aggregates route through the provider's full-precision hook: SQLite
                // (TEXT-stored decimal) uses registered decimal aggregates for SUM/AVG and the
                // NORM_DECIMAL collation for MIN/MAX, so results are exact at full 28-digit
                // precision and MIN/MAX always return an actual stored value; providers with
                // native DECIMAL emit the plain aggregate. AverageAggregateOperand is identity
                // for decimal.
                var selBodyType = Nullable.GetUnderlyingType(selector.Body.Type) ?? selector.Body.Type;

                // Build complete SELECT ... FROM ... so the sql-length guard in Generate()
                // correctly skips the default SELECT/FROM assembly block.
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                if (selBodyType == typeof(decimal))
                {
                    _sql.Append(_provider.DecimalAggregateSql(sqlFunction, columnSql));
                }
                else
                {
                    // C# Average over ints is a double; SQL Server's AVG(int) truncates to int, so
                    // its provider hook casts integral operands to FLOAT (identity elsewhere).
                    if (sqlFunction == "AVG")
                        columnSql = _provider.AverageAggregateOperand(columnSql, selector.Body.Type);
                    _sql.AppendAggregateFunction(sqlFunction, columnSql);
                }
                _sql.AppendFragment(" FROM ").Append(TemporalTableSource(_mapping)).Append(' ').Append(alias);
            }

            return node;
        }
        private Expression HandleAllOperation(MethodCallExpression node)
        {
            // ALL is translated as:
            // SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM table alias WHERE NOT (pred) [AND outer_where]) THEN 1 ELSE 0 END
            // The predicate MUST be embedded inside the subquery -- do NOT use _where, which
            // Build() would append AFTER the closing parenthesis, against an outer SELECT
            // that has no FROM clause.
            var sourceQuery = node.Arguments[0];
            var predicate = StripQuotes(node.Arguments[1]) as LambdaExpression;

            if (predicate == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "All operation requires a predicate"));
            Visit(sourceQuery);

            // If the source had an outer Where, its predicates are now sitting in _where
            // and would otherwise be appended by Build() against an outer SELECT with no
            // FROM clause -- producing "no such column: T0.x". Snapshot them so we can
            // AND them into the EXISTS subquery's own WHERE clause, then clear _where so
            // Build() doesn't double-emit them.
            string? outerWhereSql = null;
            if (_where.Length > 0)
            {
                outerWhereSql = _where.ToSqlString();
                _where.Clear();
            }

            var param = predicate.Parameters[0];
            var alias = EscapeAlias("T" + _joinCounter);
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            // paramIndexStart=_params.Count so the All predicate's parameters do not
            // collide with @p0/@p1 already allocated by the outer Where. Pooled visitors
            // reset _paramIndex to ParamIndexStart, so without this offset both predicates
            // would emit @p0 and the inner value would overwrite the outer in _params.
            var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx2);
            var predicateSql = visitor.Translate(predicate.Body);
            // All() predicate parameters are fixed constants (not closure captures),
            // so add them only to _params (NOT _compiledParams) to ensure they are
            // bound directly rather than relying on ParameterValueExtractor.
            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value ?? DBNull.Value;
            FastExpressionVisitorPool.Return(visitor);

            // Build the complete SQL inline so the predicate is inside the EXISTS subquery.
            // When an outer Where was present, AND it into the subquery's WHERE so the
            // semantics match Where(outer).All(p) -> NOT EXISTS(rows matching outer that
            // violate p).
            var subqueryWhere = outerWhereSql is null
                ? $"NOT ({predicateSql})"
                : $"NOT ({predicateSql}) AND ({outerWhereSql})";
            _sql.Insert(0,
                $"SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {TemporalTableSource(_mapping)} {alias} WHERE {subqueryWhere}) THEN 1 ELSE 0 END");

            // Signal to Build() that this is a scalar query so it uses the scalar materializer
            // path (reads position 0, converts via Convert.ChangeType to bool).
            _isAggregate = true;

            return node;
        }
    }
}
