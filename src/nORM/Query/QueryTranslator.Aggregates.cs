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
            // GroupBy over a set-op (Union/Concat/Intersect/Except) or a Distinct has the same shape problem as a
            // windowed source: the group key `g => g.A` cannot resolve against a bare compound or DISTINCT SELECT.
            // Wrap it as a derived table (`FROM (<compound>) AS alias`) through the same sub-context path so
            // `GROUP BY alias.A` and the aggregate SELECT are valid, instead of the member failing to resolve. The
            // spine walk also catches wrapper-over-set-op shapes (`Concat(...).Distinct()/.Where(...)` then GroupBy)
            // that a bare immediate-source check misses.
            bool sourceIsSetOp = SourceHasSetOpOrDistinct(sourceQuery);
            if (sourceIsWindowed || sourceIsSetOp)
            {
                var subPlanG = TranslateInSubContext(sourceQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subMapG);
                _mapping = subMapG;
                MergeSubPlanParameters(subPlanG);
                alias = EscapeAlias("__wgb" + _joinCounter++);
                // Store the sub-SQL + alias so BuildGroupBySelectClause emits
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
            _groupOrderedFirstSourceWheres = (sourceIsWindowed || sourceIsSetOp) ? null : ExtractGroupSourceWheres(sourceQuery);
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
                    // Same correlated-mapping preference as the single-key path below: a join key
                    // member (p.A / c.B) must resolve against its own join alias, not the forced one.
                    var (partMapping, partAlias) = _correlatedParams.TryGetValue(param, out var partCorr) ? partCorr : (_mapping, alias);
                    var vctxPart = new VisitorContext(_ctx, partMapping, _provider, param, partAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
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
                    // Exact key canonicalization (decimal text, TimeOnly fraction-strip); DateTimeOffset is
                    // handled by its instant hook just below.
                    partSql = _provider.ExactKeySql(partSql, partType);
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
                    // For a join/projection source the (expanded) key param is already correctly
                    // mapped in _correlatedParams (e.g. p -> (Parent, T0)). The VisitorContext
                    // unconditionally sets _parameterMappings[param] = (Mapping, TableAlias), so
                    // forcing (_mapping, alias) here would override that with the inner join alias,
                    // mis-qualifying the key column (GROUP BY T1."PVal" for a Parent column). Prefer
                    // the correlated mapping when present; single-table groupings aren't in it and
                    // keep (_mapping, alias).
                    var (keyMapping, keyAlias) = _correlatedParams.TryGetValue(param, out var keyCorr) ? keyCorr : (_mapping, alias);
                    var vctx2 = new VisitorContext(_ctx, keyMapping, _provider, param, keyAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
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
                    // Exact key canonicalization (decimal text, TimeOnly fraction-strip; see composite-key
                    // path above). DateTimeOffset is handled by its instant hook just below.
                    var keyType = Nullable.GetUnderlyingType(keySelectorLambda.Body.Type) ?? keySelectorLambda.Body.Type;
                    groupBySql = _provider.ExactKeySql(groupBySql, keyType);
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
        /// <summary>
        /// Peels a scalar <c>Select(x =&gt; scalarProj)</c> into its inner source + selector, so a
        /// following parameterless aggregate can fold the projection into the aggregate operand.
        /// Only a genuinely scalar projection (value type or string) is peeled — an entity or
        /// anonymous Select (which a parameterless aggregate can't compile against) is left alone.
        /// </summary>
        private bool TryPeelScalarSelect(Expression expr, out Expression innerSource, out LambdaExpression? selector)
        {
            innerSource = expr;
            selector = null;
            if (expr is MethodCallExpression sc
                && sc.Method.DeclaringType == typeof(Queryable)
                && sc.Method.Name == nameof(Queryable.Select)
                && sc.Arguments.Count == 2
                && StripQuotes(sc.Arguments[1]) is LambdaExpression sel
                && sel.Parameters.Count == 1)
            {
                var bt = Nullable.GetUnderlyingType(sel.Body.Type) ?? sel.Body.Type;
                if (bt.IsValueType || bt == typeof(string))
                {
                    innerSource = sc.Arguments[0];
                    selector = sel;
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Forces <c>AS aliasName</c> onto the single-column select list of the first arm of a compound
        /// SELECT (the arm whose column name the compound adopts), so a derived-table wrap can aggregate
        /// a known column name. Returns null when the shape isn't a single unaliased scalar column
        /// (multi-column, already aliased, or <c>SELECT *</c>) — the caller then leaves the aggregate to
        /// its existing path rather than emit wrong SQL. Mirrors AliasNamelessScalarSelect's paren-aware
        /// top-level FROM scan but always aliases (that helper deliberately skips bare identifiers).
        /// </summary>
        private string? ForceFirstArmColumnAlias(string sql, string aliasName)
        {
            if (!sql.StartsWith("SELECT ", StringComparison.Ordinal)) return null;
            var depth = 0;
            var fromIdx = -1;
            for (var i = 7; i < sql.Length - 6; i++)
            {
                var ch = sql[i];
                if (ch == '(') depth++;
                else if (ch == ')') depth--;
                else if (depth == 0 && ch == ' ' && string.CompareOrdinal(sql, i, " FROM ", 0, 6) == 0)
                {
                    fromIdx = i;
                    break;
                }
            }
            if (fromIdx < 0) return null;
            var list = sql[7..fromIdx];
            if (list.Trim() == "*") return null; // SELECT * (e.g. windowed derived-table arm) — can't alias
            var listDepth = 0;
            foreach (var ch in list)
            {
                if (ch == '(') listDepth++;
                else if (ch == ')') listDepth--;
                else if (ch == ',' && listDepth == 0) return null; // multi-column: not a scalar set-op
            }
            if (list.Contains(" AS ", StringComparison.OrdinalIgnoreCase)) return null; // already aliased (name unknown)
            return string.Concat(sql.AsSpan(0, fromIdx), " AS ", _provider.Escape(aliasName), sql.AsSpan(fromIdx));
        }

        private Expression HandleDirectAggregate(MethodCallExpression node)
        {
            // Handle direct aggregate calls like query.Sum(x => x.Amount)
            var sourceQuery = node.Arguments[0];

            // Fold `source.Select(x => scalarProj).Sum()/Max()/Min()/Average()` into the selector
            // form `source.Sum(x => scalarProj)`. Without this, the parameterless terminal over a
            // scalar Select skips the aggregate-emit block below (which is gated on a selector arg),
            // leaving `SELECT scalarProj FROM ...` with no aggregate — the query then materialises a
            // List<T> and the scalar-result path casts List<T> -> T and throws InvalidCastException.
            // Only a genuinely scalar projection (value type or string) is peeled, so an entity /
            // anonymous Select (which .Max() couldn't compile against anyway) is left untouched.
            LambdaExpression? directSelector = node.Arguments.Count > 1
                ? StripQuotes(node.Arguments[1]) as LambdaExpression
                : null;
            // aggregateDistinct: `Select(x => scalar).Distinct().Sum()/Max()/Min()/Average()` sums the
            // DISTINCT projected values -> AGG(DISTINCT proj). Peel an optional Distinct wrapping a scalar
            // Select, then the Select itself. (Distinct over an entity/anonymous shape is left alone; only
            // a scalar projection can be a distinct aggregate operand.)
            bool aggregateDistinct = false;
            if (directSelector == null)
            {
                var peelTarget = StripQuotes(sourceQuery);
                if (peelTarget is MethodCallExpression distinctCall
                    && distinctCall.Method.DeclaringType == typeof(Queryable)
                    && distinctCall.Method.Name == nameof(Queryable.Distinct)
                    && distinctCall.Arguments.Count == 1
                    && TryPeelScalarSelect(StripQuotes(distinctCall.Arguments[0]), out var distinctInner, out var distinctSel))
                {
                    aggregateDistinct = true;
                    sourceQuery = distinctInner;
                    directSelector = distinctSel;
                }
                else if (TryPeelScalarSelect(peelTarget, out var selInner, out var selSel))
                {
                    sourceQuery = selInner;
                    directSelector = selSel;
                }
            }

            // Parameterless aggregate over a SET OPERATION (Union/Concat/Except/Intersect):
            // `q1.Union(q2).Sum()/Max()/Min()/Average()`. The aggregate is over the compound's single
            // scalar column, not a table, so wrap it: SELECT AGG(col) FROM (<set-op>) AS sub. Without
            // this the aggregate is dropped, the compound materialises a List<T>, and the scalar-result
            // path casts List<T> -> T and throws InvalidCastException.
            if (directSelector == null
                && StripQuotes(sourceQuery) is MethodCallExpression setOpAggSource
                && setOpAggSource.Method.Name is "Union" or "Concat" or "Intersect" or "Except")
            {
                var subPlanSet = TranslateInSubContext(sourceQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subMapSet);
                _mapping = subMapSet;
                MergeSubPlanParameters(subPlanSet);
                // The compound takes its first arm's column name; force a known alias on that arm so
                // the wrap can aggregate it uniformly (bare-member arms are otherwise self-named,
                // computed arms unnamed). Null => not a single-column scalar set-op; fall through.
                var aliasedSetSql = ForceFirstArmColumnAlias(subPlanSet.Sql, "__set_val");
                if (aliasedSetSql != null)
                {
                    if (!AggregateFunctionMap.TryGetValue(node.Method.Name, out var setAggFn))
                        setAggFn = node.Method.Name.ToUpperInvariant();
                    var setElemType = sourceQuery.Type.IsGenericType ? sourceQuery.Type.GetGenericArguments()[0] : node.Type;
                    var setElemUnderlying = Nullable.GetUnderlyingType(setElemType) ?? setElemType;
                    var setCol = _provider.Escape("__set_val");
                    string setAggExpr;
                    if (setElemUnderlying == typeof(decimal))
                    {
                        setAggExpr = _provider.DecimalAggregateSql(setAggFn, setCol);
                    }
                    else
                    {
                        if (setAggFn == "AVG") setCol = _provider.AverageAggregateOperand(setCol, setElemType);
                        setAggExpr = $"{setAggFn}({setCol})";
                    }
                    var setAggAlias = EscapeAlias("__saggu" + _joinCounter++);
                    _isAggregate = true;
                    _methodName = node.Method.Name;
                    _projection = null;
                    _orderBy.Clear();
                    _sql.Clear();
                    _sql.Append("SELECT ").Append(setAggExpr).Append(" FROM (")
                        .Append(aliasedSetSql).AppendFragment(") AS ").Append(setAggAlias);
                    return node;
                }
            }

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
                if (directSelector is LambdaExpression selA)
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

            if (directSelector is LambdaExpression selector)
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
                    // DISTINCT (if any) goes inside the aggregate: SUM(DISTINCT expr). SQLite's
                    // registered decimal aggregates and the other providers all accept it.
                    _sql.Append(_provider.DecimalAggregateSql(sqlFunction, aggregateDistinct ? "DISTINCT " + columnSql : columnSql));
                }
                else
                {
                    // C# Average over ints is a double; SQL Server's AVG(int) truncates to int, so
                    // its provider hook casts integral operands to FLOAT (identity elsewhere). The
                    // DISTINCT modifier must wrap the already-cast operand (AVG(DISTINCT CAST(..))),
                    // not sit inside the CAST, so apply it after AverageAggregateOperand.
                    if (sqlFunction == "AVG")
                        columnSql = _provider.AverageAggregateOperand(columnSql, selector.Body.Type);
                    _sql.AppendAggregateFunction(sqlFunction, aggregateDistinct ? "DISTINCT " + columnSql : columnSql);
                }
                _sql.AppendFragment(" FROM ").Append(RootTableSource()).Append(' ').Append(alias);
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
            // Over a scalar projection (Select(x => proj).All(a => a OP ...)), the predicate's
            // parameter is the projected value, not the entity — expand it through the projection so
            // `a` resolves to `proj` (else `a` renders as an empty operand: `WHERE NOT (( < @p0))`).
            // No-op when there is no matching projection (plain All(entity predicate)).
            predicate = ExpandProjection(predicate);

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
                $"SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {RootTableSource()} {alias} WHERE {subqueryWhere}) THEN 1 ELSE 0 END");

            // Signal to Build() that this is a scalar query so it uses the scalar materializer
            // path (reads position 0, converts via Convert.ChangeType to bool).
            _isAggregate = true;

            return node;
        }
    }
}
