using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private void BuildGroupBySelectClause(LambdaExpression resultSelector, string groupBySql, string alias, string? sourceFromSql)
        {
            var selectItems = _selectItemsPool.Get();
            try
            {
                // Direct Key projection. Covers:
                //  - g => g.Key             (inline 3-arg GroupBy with IGrouping projection)
                //  - (k, g) => k            (rewritten from 2-arg GroupBy + Select(g => g.Key))
                bool bodyIsKey = (resultSelector.Body is MemberExpression me && me.Member.Name == "Key")
                    || (resultSelector.Body is ParameterExpression pe
                        && resultSelector.Parameters.Count >= 1
                        && pe == resultSelector.Parameters[0]);
                if (bodyIsKey)
                {
                    var keyBuilder = PooledStringBuilder.Rent();
                    keyBuilder.Append(groupBySql).Append(" AS ").Append(_provider.Escape("Key"));
                    selectItems.Add(keyBuilder.ToString());
                    PooledStringBuilder.Return(keyBuilder);
                }
                else if (resultSelector.Body is MethodCallExpression methodCall
                         && TranslateGroupAggregateMethod(methodCall, alias, groupBySql) is { } aggregateSql)
                {
                    var groupBuilder = PooledStringBuilder.Rent();
                    groupBuilder.Append(groupBySql).Append(" AS GroupKey");
                    selectItems.Add(groupBuilder.ToString());
                    PooledStringBuilder.Return(groupBuilder);

                    var valueBuilder = PooledStringBuilder.Rent();
                    valueBuilder.Append(aggregateSql).Append(" AS ").Append(_provider.Escape("Value"));
                    selectItems.Add(valueBuilder.ToString());
                    PooledStringBuilder.Return(valueBuilder);
                }
                else
                {
                    var builder = (System.Text.StringBuilder?)null;
                    var projectionItems = GetGroupProjectionItems(resultSelector.Body);
                    if (projectionItems != null)
                    {
                        foreach (var (arg, memberName) in projectionItems)
                        {
                            // Greatest-N-per-group: g.OrderByDescending(x => x.Date).First().Amount
                            // emits a correlated single-row subquery correlated on the group key.
                            if (TryTranslateGroupOrderedFirst(arg, alias, groupBySql) is { } orderedFirstSql)
                            {
                                var orderedFirstBuilder = PooledStringBuilder.Rent();
                                orderedFirstBuilder.Append(orderedFirstSql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(orderedFirstBuilder.ToString());
                                PooledStringBuilder.Return(orderedFirstBuilder);
                                continue;
                            }
                            // g.Key inside a projection projects the group key column.
                            if (arg is MemberExpression keyMember && keyMember.Member.Name == "Key")
                            {
                                if (_compositeKeyMemberSql.Count > 0)
                                {
                                    foreach (var kv in _compositeKeyMemberSql)
                                    {
                                        var nestedBuilder = PooledStringBuilder.Rent();
                                        nestedBuilder.Append(kv.Value).Append(" AS ").Append(_provider.Escape(memberName + "__" + kv.Key));
                                        selectItems.Add(nestedBuilder.ToString());
                                        PooledStringBuilder.Return(nestedBuilder);
                                    }
                                    continue;
                                }
                                builder = PooledStringBuilder.Rent();
                                builder.Append(groupBySql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            // Bare key parameter inside a 3-arg result selector: (k, g) => new { k, ... }.
                            if (arg is ParameterExpression keyParam
                                && resultSelector.Parameters.Count >= 1
                                && keyParam == resultSelector.Parameters[0])
                            {
                                if (_compositeKeyMemberSql.Count > 0)
                                {
                                    foreach (var kv in _compositeKeyMemberSql)
                                    {
                                        var nestedBuilder = PooledStringBuilder.Rent();
                                        nestedBuilder.Append(kv.Value).Append(" AS ").Append(_provider.Escape(memberName + "__" + kv.Key));
                                        selectItems.Add(nestedBuilder.ToString());
                                        PooledStringBuilder.Return(nestedBuilder);
                                    }
                                    continue;
                                }
                                builder = PooledStringBuilder.Rent();
                                builder.Append(groupBySql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            // Composite-key member access: `key.A` where key is the result selector's
                            // first parameter and A is one of the anonymous-type members on the
                            // original GroupBy key. Resolve to the per-member GROUP BY column SQL.
                            if (arg is MemberExpression compositeKeyAccess
                                && compositeKeyAccess.Expression is ParameterExpression maybeKey
                                && resultSelector.Parameters.Count >= 1
                                && maybeKey == resultSelector.Parameters[0]
                                && _compositeKeyMemberSql.TryGetValue(compositeKeyAccess.Member.Name, out var memberSql))
                            {
                                builder = PooledStringBuilder.Rent();
                                builder.Append(memberSql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            // Grouping-key member access: `g.Key.A` from the 2-arg
                            // GroupBy + Select rewrite, where `g` is the grouping
                            // parameter and A is one of the original composite key
                            // members.
                            if (arg is MemberExpression groupingKeyAccess
                                && groupingKeyAccess.Expression is MemberExpression groupingKey
                                && groupingKey.Member.Name == "Key"
                                && groupingKey.Expression is ParameterExpression maybeGroup
                                && resultSelector.Parameters.Contains(maybeGroup)
                                && _compositeKeyMemberSql.TryGetValue(groupingKeyAccess.Member.Name, out var groupingMemberSql))
                            {
                                builder = PooledStringBuilder.Rent();
                                builder.Append(groupingMemberSql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            // Nested anonymous payload: e.g. `Stats = new { Total = g.Sum(...), Count = g.Count() }`.
                            // Emit one flat column per inner arg with a prefixed alias (`Stats__Total`,
                            // `Stats__Count`) so the materialiser can reconstruct the nested anon type
                            // from N sequential columns.
                            if (arg is NewExpression nestedNew)
                            {
                                for (int j = 0; j < nestedNew.Arguments.Count; j++)
                                {
                                    var subArg = nestedNew.Arguments[j];
                                    var subName = nestedNew.Members?[j]?.Name ?? $"Item{j + 1}";
                                    var aliasName = memberName + "__" + subName;
                                    if (TryTranslateGroupOrderedFirst(subArg, alias, groupBySql) is { } nestedOrderedFirst)
                                    {
                                        var b0 = PooledStringBuilder.Rent();
                                        b0.Append(nestedOrderedFirst).Append(" AS ").Append(_provider.Escape(aliasName));
                                        selectItems.Add(b0.ToString());
                                        PooledStringBuilder.Return(b0);
                                    }
                                    else if (subArg is MethodCallExpression subAgg
                                        && TranslateGroupAggregateMethod(subAgg, alias, groupBySql) is { } subAggSql)
                                    {
                                        var b = PooledStringBuilder.Rent();
                                        b.Append(subAggSql).Append(" AS ").Append(_provider.Escape(aliasName));
                                        selectItems.Add(b.ToString());
                                        PooledStringBuilder.Return(b);
                                    }
                                    else if (subArg is MemberExpression subKeyMember
                                             && subKeyMember.Member.Name == "Key"
                                             && _compositeKeyMemberSql.Count == 0)
                                    {
                                        var b = PooledStringBuilder.Rent();
                                        b.Append(groupBySql).Append(" AS ").Append(_provider.Escape(aliasName));
                                        selectItems.Add(b.ToString());
                                        PooledStringBuilder.Return(b);
                                    }
                                    else if (subArg is MemberExpression subKeyAccess
                                             && subKeyAccess.Expression is ParameterExpression subMaybeKey
                                             && resultSelector.Parameters.Count >= 1
                                             && subMaybeKey == resultSelector.Parameters[0]
                                             && _compositeKeyMemberSql.TryGetValue(subKeyAccess.Member.Name, out var subMemberSql))
                                    {
                                        var b = PooledStringBuilder.Rent();
                                        b.Append(subMemberSql).Append(" AS ").Append(_provider.Escape(aliasName));
                                        selectItems.Add(b.ToString());
                                        PooledStringBuilder.Return(b);
                                    }
                                    else if (subArg is MemberExpression subGroupingKeyAccess
                                             && subGroupingKeyAccess.Expression is MemberExpression subGroupingKey
                                             && subGroupingKey.Member.Name == "Key"
                                             && subGroupingKey.Expression is ParameterExpression subMaybeGroup
                                             && resultSelector.Parameters.Contains(subMaybeGroup)
                                             && _compositeKeyMemberSql.TryGetValue(subGroupingKeyAccess.Member.Name, out var subGroupingMemberSql))
                                    {
                                        var b = PooledStringBuilder.Rent();
                                        b.Append(subGroupingMemberSql).Append(" AS ").Append(_provider.Escape(aliasName));
                                        selectItems.Add(b.ToString());
                                        PooledStringBuilder.Return(b);
                                    }
                                    else
                                    {
                                        if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                                            _correlatedParams[resultSelector.Parameters[0]] = (_mapping, alias);
                                        var subVctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                                        var subVisitor = FastExpressionVisitorPool.Get(in subVctx);
                                        var subSql = subVisitor.Translate(subArg);
                                        var b = PooledStringBuilder.Rent();
                                        b.Append(subSql).Append(" AS ").Append(_provider.Escape(aliasName));
                                        selectItems.Add(b.ToString());
                                        PooledStringBuilder.Return(b);
                                        foreach (var kvp in subVisitor.GetParameters())
                                            AddLiteralParameter(kvp.Key, kvp.Value);
                                        FastExpressionVisitorPool.Return(subVisitor);
                                    }
                                }
                                continue;
                            }
                            if (arg is MethodCallExpression aggregateCall)
                            {
                                var innerAggregate = TranslateGroupAggregateMethod(aggregateCall, alias, groupBySql);
                                if (innerAggregate != null)
                                {
                                    builder = PooledStringBuilder.Rent();
                                    builder.Append(innerAggregate).Append(" AS ").Append(_provider.Escape(memberName));
                                    selectItems.Add(builder.ToString());
                                    PooledStringBuilder.Return(builder);
                                }
                            }
                            else if (arg is ParameterExpression groupCarry
                                     && resultSelector.Parameters.Contains(groupCarry)
                                     && IsGroupingSequenceType(groupCarry.Type))
                            {
                                // Query syntax can carry the grouping range variable
                                // through a `let` continuation even when downstream
                                // operators only consume the derived aggregate payload.
                                // There is no SQL column that represents an IGrouping;
                                // omit the continuation-only payload from the SELECT.
                                continue;
                            }
                            else
                            {
                                if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                                    _correlatedParams[resultSelector.Parameters[0]] = (_mapping, alias);
                                var vctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                string sql;
                                try
                                {
                                    sql = visitor.Translate(arg);
                                }
                                catch (NormUnsupportedFeatureException ex)
                                {
                                    throw new NormUnsupportedFeatureException($"Grouped projection member '{memberName}' could not be translated from expression '{arg}'. {ex.Message}", ex);
                                }
                                builder = PooledStringBuilder.Rent();
                                builder.Append(sql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                // See HandleGroupBy simple-key path: AddLiteralParameter so the
                                // visitor's inline constants don't get mis-flagged as compiled.
                                foreach (var kvp in visitor.GetParameters())
                                    AddLiteralParameter(kvp.Key, kvp.Value);
                                FastExpressionVisitorPool.Return(visitor);
                            }
                        }
                    }
                }

                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.AppendJoin(", ", selectItems);
                // Build() skips its SELECT/FROM-prefix block when _sql.Length > 0, so we must
                // emit FROM ourselves. Subsequent clauses (WHERE/GROUP BY/ORDER BY) are appended
                // by the Build() pipeline as usual. When the source was Take/Skip-windowed
                // (see HandleGroupBy), emit `FROM (windowedSubSql) AS alias` so GROUP BY
                // aggregates the windowed rows only.
                if (_windowedGroupBySubSql != null && _windowedGroupByAlias != null)
                {
                    _sql.AppendFragment(" FROM (").Append(_windowedGroupBySubSql).AppendFragment(") AS ").Append(_windowedGroupByAlias);
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(sourceFromSql))
                        _sql.AppendFragment(sourceFromSql);
                    else
                        _sql.AppendFragment(" FROM ").Append(TemporalTableSource(_mapping)).Append(' ').Append(alias);
                    // Applied lateral columns (a navigation-member GROUP BY key exposed
                    // as a groupable column on providers that reject subqueries in
                    // GROUP BY) attach directly after the source table.
                    if (_fromSuffix != null)
                        _sql.AppendFragment(_fromSuffix);
                }
            }
            finally
            {
                _selectItemsPool.Return(selectItems);
            }
        }

        /// <summary>
        /// Translates a greatest-N-per-group projection member — <c>g.OrderByDescending(x =&gt; x.Date).First().Amount</c>
        /// or <c>g.OrderBy(x =&gt; x.Date).Select(x =&gt; x.Amount).First()</c> — into a correlated
        /// single-row subquery. Returns <c>null</c> when <paramref name="arg"/> is not that shape. The
        /// subquery is correlated on the group key (single- or composite-column); supports First/FirstOrDefault
        /// and Last/LastOrDefault (the latter reverses the ordering); the subquery's LIMIT/TOP form is chosen
        /// by the provider.
        /// </summary>
        private string? TryTranslateGroupOrderedFirst(Expression arg, string alias, string groupBySql)
        {
            // Re-projecting through a group element selector is not yet plumbed for this
            // shape, and a windowed or otherwise-reshaped source keeps its rows in a
            // derived table the re-scan cannot see (_groupOrderedFirstSourceWheres is
            // null for such sources).
            if (_groupByKeySelector == null
                || _groupByElementSelector != null
                || _groupOrderedFirstSourceWheres == null
                || _windowedGroupBySubSql != null
                || string.IsNullOrEmpty(groupBySql))
                return null;

            // Peel an optional trailing member access: `...First().Amount`.
            System.Reflection.MemberInfo? tailMember = null;
            if (arg is MemberExpression outerMember && outerMember.Expression is MethodCallExpression)
            {
                tailMember = outerMember.Member;
                arg = outerMember.Expression;
            }

            if (arg is not MethodCallExpression terminalCall)
                return null;

            var lastSemantics = false;
            Expression? indexExpr = null;
            LambdaExpression? terminalPredicate = null;
            switch (terminalCall.Method.Name)
            {
                case "First":
                case "FirstOrDefault":
                    break;
                case "Last":
                case "LastOrDefault":
                    lastSemantics = true;
                    break;
                case "ElementAt":
                case "ElementAtOrDefault":
                    if (terminalCall.Arguments.Count != 2)
                        return null;
                    indexExpr = terminalCall.Arguments[1];
                    // ElementAt with a constant negative index throws in LINQ; SQL
                    // OFFSET would silently clamp — keep that shape fail-closed.
                    if (TryGetIntValue(indexExpr, out var constIndex) && constIndex < 0)
                        return null;
                    break;
                default:
                    return null;
            }
            if (indexExpr == null)
            {
                if (terminalCall.Arguments.Count == 2)
                {
                    if (StripQuotes(terminalCall.Arguments[1]) is not LambdaExpression termPred
                        || termPred.Parameters.Count != 1)
                        return null;
                    terminalPredicate = termPred;
                }
                else if (terminalCall.Arguments.Count != 1)
                {
                    return null;
                }
            }

            var src = terminalCall.Arguments[0];

            // Optional `.Select(x => scalar)` directly under the terminal.
            LambdaExpression? resultSelector = null;
            if (src is MethodCallExpression selectCall
                && selectCall.Method.Name == "Select"
                && selectCall.Arguments.Count == 2
                && StripQuotes(selectCall.Arguments[1]) is LambdaExpression resLambda
                && resLambda.Parameters.Count == 1)
            {
                resultSelector = resLambda;
                src = selectCall.Arguments[0];
            }

            // `Where` filters between the ordering chain and the Select/terminal.
            // LINQ preserves order through Where, and SQL applies WHERE before
            // ORDER BY anyway, so position relative to the ordering is immaterial.
            var predicates = new List<LambdaExpression>();
            while (src is MethodCallExpression whereAbove
                   && whereAbove.Method.Name == "Where"
                   && whereAbove.Arguments.Count == 2
                   && StripQuotes(whereAbove.Arguments[1]) is LambdaExpression whereAboveLambda
                   && whereAboveLambda.Parameters.Count == 1)
            {
                predicates.Add(whereAboveLambda);
                src = whereAbove.Arguments[0];
            }

            // ThenBy* chain ending at the primary OrderBy. The expression tree nests
            // outermost-last, so collect then reverse to put the primary key first.
            var orderKeys = new List<(LambdaExpression Selector, bool Descending)>();
            while (src is MethodCallExpression thenByCall
                   && thenByCall.Method.Name is "ThenBy" or "ThenByDescending"
                   && thenByCall.Arguments.Count == 2
                   && StripQuotes(thenByCall.Arguments[1]) is LambdaExpression thenByLambda)
            {
                orderKeys.Add((thenByLambda, thenByCall.Method.Name == "ThenByDescending"));
                src = thenByCall.Arguments[0];
            }
            if (src is not MethodCallExpression orderCall
                || orderCall.Method.Name is not ("OrderBy" or "OrderByDescending")
                || orderCall.Arguments.Count != 2
                || StripQuotes(orderCall.Arguments[1]) is not LambdaExpression orderLambda)
                return null;
            orderKeys.Add((orderLambda, orderCall.Method.Name == "OrderByDescending"));
            orderKeys.Reverse();
            src = orderCall.Arguments[0];

            // `Where` filters below the ordering.
            while (src is MethodCallExpression whereBelow
                   && whereBelow.Method.Name == "Where"
                   && whereBelow.Arguments.Count == 2
                   && StripQuotes(whereBelow.Arguments[1]) is LambdaExpression whereBelowLambda
                   && whereBelowLambda.Parameters.Count == 1)
            {
                predicates.Add(whereBelowLambda);
                src = whereBelow.Arguments[0];
            }

            // The chain must bottom out at the grouping range variable.
            if (src is not ParameterExpression groupParam || !IsGroupingSequenceType(groupParam.Type))
                return null;

            // A terminal predicate filters like a Where. After a Select it binds to
            // the projected scalar — substitute the selector body so it references
            // source columns.
            if (terminalPredicate != null)
            {
                if (resultSelector != null)
                {
                    var substituted = new ParameterReplacer(terminalPredicate.Parameters[0], resultSelector.Body)
                        .Visit(terminalPredicate.Body)!;
                    predicates.Add(Expression.Lambda(substituted, resultSelector.Parameters[0]));
                }
                else
                {
                    predicates.Add(terminalPredicate);
                }
            }

            // Resolve the scalar to project from the single row.
            Expression resultBody;
            ParameterExpression resultParam;
            if (resultSelector != null)
            {
                resultParam = resultSelector.Parameters[0];
                resultBody = tailMember == null
                    ? resultSelector.Body
                    : Expression.MakeMemberAccess(resultSelector.Body, tailMember);
            }
            else if (tailMember != null)
            {
                resultParam = Expression.Parameter(orderKeys[0].Selector.Parameters[0].Type, "e");
                resultBody = Expression.MakeMemberAccess(resultParam, tailMember);
            }
            else
            {
                return null; // `.First()` with no scalar member/selector to project.
            }

            const string subAlias = "g0";
            var whereSql = BuildGroupKeyCorrelation(subAlias, groupBySql);
            if (whereSql == null)
                return null;

            // The subquery re-scans the grouped table, so the entity's global filters (soft-delete,
            // tenant) must be repeated inside it — the outer GROUP BY is filtered by ApplyGlobalFilters
            // but this correlated subquery is not, so a soft-deleted "latest" row would otherwise leak.
            var globalFilter = GlobalFilterFragment.Combine(_ctx, _mapping.Type);
            if (globalFilter != null)
            {
                var filterSql = TranslateAgainstSubAlias(globalFilter.Body, globalFilter.Parameters[0], subAlias);
                whereSql = whereSql + " AND (" + filterSql + ")";
            }

            // The query's own Where filters define which rows belong to the groups —
            // the re-scan must repeat them or an excluded row could win the ordering.
            foreach (var sourceWhere in _groupOrderedFirstSourceWheres)
                whereSql += " AND (" + TranslateAgainstSubAlias(sourceWhere.Body, sourceWhere.Parameters[0], subAlias) + ")";

            // Group-local Where filters and terminal predicates.
            foreach (var predicate in predicates)
                whereSql += " AND (" + TranslateAgainstSubAlias(predicate.Body, predicate.Parameters[0], subAlias) + ")";

            var selectSql = TranslateAgainstSubAlias(resultBody, resultParam, subAlias);

            // Last/LastOrDefault picks the opposite end of the ordering: flip EVERY key.
            var orderParts = new List<string>(orderKeys.Count);
            foreach (var (selector, descending) in orderKeys)
            {
                var keySql = TranslateAgainstSubAlias(selector.Body, selector.Parameters[0], subAlias);
                keySql = CoerceOrderKeySql(keySql, selector.Body.Type);
                orderParts.Add(descending ^ lastSemantics ? keySql + " DESC" : keySql);
            }
            var orderByFull = string.Join(", ", orderParts);

            string? offsetSql = null;
            if (indexExpr != null)
                offsetSql = TranslateAgainstSubAlias(indexExpr, Expression.Parameter(typeof(int), "__idx"), subAlias);

            return _provider.BuildCorrelatedSingleRowSubquery(selectSql, TemporalTableSource(_mapping), subAlias, whereSql, orderByFull, offsetSql);
        }

        /// <summary>
        /// Replaces closure-captured member accesses (and evaluable static members)
        /// with their current values so a re-translated subquery fragment mints no
        /// compiled-parameter slots. Mirrors ParameterValueExtractor's walk: one
        /// substitution per top-level constant-resolvable member, no descent below it.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure lifting evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure lifting reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class CapturedValueInliner : ExpressionVisitor
        {
            private readonly QueryTranslator _t;
            public CapturedValueInliner(QueryTranslator t) => _t = t;

            protected override Expression VisitMember(MemberExpression node)
            {
                if (TryGetConstantValue(node, out var value))
                {
                    _t._closureFoldedIntoSql = true;
                    return Expression.Constant(value, node.Type);
                }
                return base.VisitMember(node);
            }
        }

        /// <summary>
        /// Collects the source chain's Where predicates for re-application inside a
        /// greatest-N-per-group correlated subquery. Returns null when the chain holds
        /// anything beyond Where/ordering over the root queryable — the subquery
        /// re-scans the base table, so any other operator (projection, window, join,
        /// set op) would make the re-scan see different rows than the groups did.
        /// </summary>
        private static List<LambdaExpression>? ExtractGroupSourceWheres(Expression source)
        {
            var wheres = new List<LambdaExpression>();
            var current = source;
            while (current is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case "Where" when mce.Arguments.Count == 2
                        && StripQuotes(mce.Arguments[1]) is LambdaExpression whereLambda
                        && whereLambda.Parameters.Count == 1:
                        wheres.Add(whereLambda);
                        break;
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                    case "AsNoTracking":
                    case "AsSplitQuery":
                        break;
                    default:
                        return null;
                }
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
            return current is ConstantExpression ? wheres : null;
        }

        /// <summary>
        /// Builds the correlation predicate tying a greatest-N-per-group subquery to the outer group,
        /// equating every group-key column rendered against <paramref name="subAlias"/> to its outer
        /// counterpart. Single-column keys compare against <paramref name="outerKeySql"/>; composite keys
        /// (<c>GroupBy(x =&gt; new { x.A, x.B })</c>) compare each member against the per-member outer SQL
        /// captured during GroupBy setup. Returns <c>null</c> if a key member cannot be resolved.
        /// </summary>
        private string? BuildGroupKeyCorrelation(string subAlias, string outerKeySql)
        {
            var keyBody = _groupByKeySelector!.Body;
            var keyParam = _groupByKeySelector.Parameters[0];

            if (keyBody is NewExpression composite && _compositeKeyMemberSql.Count > 0)
            {
                var conds = new List<string>(composite.Arguments.Count);
                for (var i = 0; i < composite.Arguments.Count; i++)
                {
                    var memberName = composite.Members?[i]?.Name ?? $"Item{i + 1}";
                    if (!_compositeKeyMemberSql.TryGetValue(memberName, out var outerMemberSql))
                        return null;
                    conds.Add(CorrelateKeyPart(composite.Arguments[i], keyParam, subAlias) + " = " + outerMemberSql);
                }
                return string.Join(" AND ", conds);
            }

            return CorrelateKeyPart(keyBody, keyParam, subAlias) + " = " + outerKeySql;
        }

        /// <summary>
        /// Renders one group-key expression against the subquery alias, applying the same decimal
        /// normalisation the GROUP BY side uses so numerically-equal decimal keys correlate.
        /// </summary>
        private string CorrelateKeyPart(Expression keyPart, ParameterExpression keyParam, string subAlias)
        {
            var sql = TranslateAgainstSubAlias(keyPart, keyParam, subAlias);
            var partType = Nullable.GetUnderlyingType(keyPart.Type) ?? keyPart.Type;
            if (partType == typeof(decimal))
                sql = _provider.ExactDecimalKeySql(sql);
            return sql;
        }

        /// <summary>
        /// Translates an expression whose root parameter represents a source row, rendering its mapped
        /// columns against <paramref name="subAlias"/>. Used to build the SELECT / ORDER BY / correlation
        /// fragments of a greatest-N-per-group subquery without disturbing the outer group aliasing.
        /// </summary>
        private string TranslateAgainstSubAlias(Expression body, ParameterExpression param, string subAlias)
        {
            // Inline closure captures as constants before translating: the outer
            // translation already registered ONE compiled slot per closure occurrence
            // in the expression tree (the extractor supplies one value per occurrence),
            // and a second registration here would leave the new slot valueless and
            // shift every later positional binding. The baked values make the SQL
            // execution-specific, so the inliner flags the plan to translate fresh
            // and skip both caches.
            body = new CapturedValueInliner(this).Visit(body)!;
            var local = new Dictionary<ParameterExpression, (nORM.Mapping.TableMapping Mapping, string Alias)> { [param] = (_mapping, subAlias) };
            var vctx = new VisitorContext(_ctx, _mapping, _provider, param, subAlias, local, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            var sql = visitor.Translate(body);
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);
            return sql;
        }

        private static string? ExtractSourceFromClause(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return null;

            var fromIndex = sql.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            return fromIndex >= 0 ? sql[fromIndex..] : null;
        }

        private static IReadOnlyList<(Expression Expression, string MemberName)>? GetGroupProjectionItems(Expression body)
        {
            if (body is NewExpression newExpr)
            {
                var items = new List<(Expression, string)>(newExpr.Arguments.Count);
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                    items.Add((newExpr.Arguments[i], newExpr.Members?[i]?.Name ?? $"Item{i + 1}"));
                return items;
            }

            if (body is MemberInitExpression memberInit)
            {
                var items = new List<(Expression, string)>(memberInit.Bindings.Count);
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                        items.Add((assignment.Expression, assignment.Member.Name));
                }
                return items;
            }

            return null;
        }

        /// <summary>
        /// Extracts the predicate lambda from an Enumerable / IGrouping aggregate call -
        /// the extension-method form `Enumerable.Method(source, predicate)` puts the lambda
        /// at Arguments[1], the instance form `g.Method(predicate)` at Arguments[0].
        /// Returns null when no predicate argument is present (the no-arg overload).
        /// </summary>
        private LambdaExpression? ExtractAggregatePredicate(MethodCallExpression methodCall)
        {
            var arg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                ? methodCall.Arguments[1]
                : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
            return arg != null ? StripQuotes(arg) as LambdaExpression : null;
        }

        /// <summary>
        /// When an aggregate's source is a chain of `Enumerable.Where(...)` calls
        /// (`g.Where(p1).Where(p2).Sum(s)`), strip the wrappers and return the combined
        /// AND-of-predicates. Returns null when the source is just the IGrouping parameter.
        /// </summary>
        private LambdaExpression? ExtractAggregateSourceFilter(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 0) return null;
            var source = methodCall.Arguments[0];
            LambdaExpression? combined = null;
            // Peel each Where(...) wrapper, AND-combining their predicates against a
            // single parameter so the emitted SQL gets one CASE-WHEN guard expression.
            while (source is MethodCallExpression mce
                && mce.Method.Name == nameof(Queryable.Where)
                && mce.Method.DeclaringType is { } dt
                && (dt == typeof(Queryable) || dt == typeof(Enumerable))
                && mce.Arguments.Count == 2
                && StripQuotes(mce.Arguments[1]) is LambdaExpression pred)
            {
                if (combined == null)
                {
                    combined = pred;
                }
                else
                {
                    var rebound = new nORM.Internal.ParameterReplacer(pred.Parameters[0], combined.Parameters[0]).Visit(pred.Body)!;
                    combined = Expression.Lambda(Expression.AndAlso(combined.Body, rebound), combined.Parameters[0]);
                }
                source = mce.Arguments[0];
            }
            return combined;
        }

        /// <summary>
        /// Translates a predicate / selector lambda body against the group's element alias,
        /// merging any literal constants as compiled-literal parameters. Shared by every
        /// IGrouping aggregate path that needs the CASE-WHEN-emitter pattern.
        /// </summary>
        private string TranslateGroupPredicateBody(LambdaExpression predicate, string alias)
        {
            if (!_correlatedParams.ContainsKey(predicate.Parameters[0]))
                _correlatedParams[predicate.Parameters[0]] = (_mapping, alias);
            var vctx = new VisitorContext(_ctx, _mapping, _provider, predicate.Parameters[0], alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            var sql = visitor.Translate(predicate.Body);
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);
            return sql;
        }

        private string? TranslateGroupAggregateMethod(MethodCallExpression methodCall, string alias, string? groupBySql = null)
        {
            var methodName = methodCall.Method.Name;

            // string.Join(separator, group.Select(x => member)) and the
            // empty-separator companion string.Concat(group.Select(x => member))
            // both lower to the provider's STRING_AGG / GROUP_CONCAT aggregate.
            // Detect either method shape:
            //  * Join: arg[0] is the literal separator, arg[1] is the Select call
            //  * Concat(IEnumerable<string>): arg[0] is the Select call,
            //    separator is implicitly empty.
            MethodCallExpression? aggInner = null;
            string? aggSep = null;
            if (methodCall.Method.DeclaringType == typeof(string))
            {
                if (methodName == nameof(string.Join)
                    && methodCall.Arguments.Count == 2
                    && TryGetConstantValue(methodCall.Arguments[0], out var aggSepVal)
                    && aggSepVal is string aggSepS
                    && methodCall.Arguments[1] is MethodCallExpression aggInnerJ)
                {
                    aggInner = aggInnerJ;
                    aggSep = aggSepS;
                }
                else if (methodName == nameof(string.Concat)
                         && methodCall.Arguments.Count == 1
                         && methodCall.Arguments[0] is MethodCallExpression aggInnerC
                         // string.Concat has many overloads; only the
                         // IEnumerable<string> form takes a single sequence arg.
                         && methodCall.Method.GetParameters().Length == 1
                         && methodCall.Method.GetParameters()[0].ParameterType.IsGenericType
                         && methodCall.Method.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    aggInner = aggInnerC;
                    aggSep = string.Empty;
                }
            }
            if (aggInner != null
                && aggSep != null
                && aggInner.Method.DeclaringType == typeof(System.Linq.Enumerable)
                && aggInner.Method.Name == nameof(System.Linq.Enumerable.Select)
                && aggInner.Arguments.Count == 2
                && aggInner.Arguments[0] is ParameterExpression aggGroupParam
                && aggGroupParam.Type.IsGenericType
                && (aggGroupParam.Type.GetGenericTypeDefinition() == typeof(System.Linq.IGrouping<,>)
                    || aggGroupParam.Type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                && StripQuotes(aggInner.Arguments[1]) is LambdaExpression aggLambda)
            {
                var vctxAgg = new VisitorContext(_ctx, _mapping, _provider, aggLambda.Parameters[0], alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                var aggVisitor = FastExpressionVisitorPool.Get(in vctxAgg);
                var memberSql = aggVisitor.Translate(aggLambda.Body);
                foreach (var kvp in aggVisitor.GetParameters())
                    AddLiteralParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(aggVisitor);
                var sepLit = $"'{aggSep.Replace("'", "''")}'";
                // For constant-key GroupBy (single-group aggregate, no GROUP BY emitted),
                // route source OrderBy into the aggregate function when the provider supports
                // native ordered-aggregate syntax (SQL Server WITHIN GROUP, Postgres / MySQL
                // inline ORDER BY). Consuming _orderBy here prevents it from appearing as a
                // meaningless outer ORDER BY on the single-row result set.
                // For SQLite (SupportsNativeOrderedStringAggregate = false), leave _orderBy
                // on the outer SELECT so SQLite's query planner uses an index scan in ORDER BY
                // order, which GROUP_CONCAT then observes as its input order.
                if (_groupBy.Count == 0 && _orderBy.Count > 0 && _provider.SupportsNativeOrderedStringAggregate)
                {
                    var orderSql = PooledStringBuilder.JoinOrderBy(_orderBy);
                    _orderBy.Clear();
                    return _provider.GetStringAggregateSql(memberSql, sepLit, orderSql);
                }
                return _provider.GetStringAggregateSql(memberSql, sepLit);
            }

            // Handle IGrouping<TKey, TElement> methods
            switch (methodName)
            {
                case "Count":
                case "LongCount":
                    {
                        // 1-arg `g.Count()` - COUNT(*). 2-arg `g.Count(predicate)` (extension
                        // method form Enumerable.Count(source, predicate) puts the lambda at [1])
                        // must apply the predicate: COUNT(CASE WHEN <pred> THEN 1 ELSE NULL END)
                        // - NULL excluded by SQL count semantics, matching .NET behaviour.
                        var predicate = ExtractAggregatePredicate(methodCall);
                        if (predicate == null) return "COUNT(*)";
                        var predSql = TranslateGroupPredicateBody(predicate, alias);
                        return $"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)";
                    }
                case "Any":
                    {
                        // 1-arg `g.Any()` - CAST(COUNT(*) > 0 AS INT) but in a grouped projection
                        // the group is guaranteed non-empty so it's always true - emit literal `1`.
                        // 2-arg `g.Any(predicate)` - `MAX(CASE WHEN pred THEN 1 ELSE 0 END) = 1`.
                        // SQLite returns 0/1; the materializer coerces to bool. Matches .NET
                        // Enumerable.Any semantics (true when -1 row satisfies the predicate).
                        var predicate = ExtractAggregatePredicate(methodCall);
                        if (predicate == null) return "1";
                        var predSql = TranslateGroupPredicateBody(predicate, alias);
                        return $"(MAX(CASE WHEN {predSql} THEN 1 ELSE 0 END) = 1)";
                    }
                case "All":
                    {
                        // `g.All(predicate)` - `MIN(CASE WHEN pred THEN 1 ELSE 0 END) = 1`. True
                        // only when every row in the group satisfies the predicate (one false
                        // row pulls the MIN to 0). An empty group would yield NULL but groups
                        // by definition are non-empty, so the comparison stays well-defined.
                        var predicate = ExtractAggregatePredicate(methodCall);
                        if (predicate == null) return "1";
                        var predSql = TranslateGroupPredicateBody(predicate, alias);
                        return $"(MIN(CASE WHEN {predSql} THEN 1 ELSE 0 END) = 1)";
                    }
                case "Sum":
                    return EmitGroupAggregateWithOptionalFilter(methodCall, alias, "SUM", "0",
                        "SUM requires a selector expression (e.g., g.Sum(x => x.Amount)). SUM(*) is not valid SQL.");
                case "Average":
                    // For AVG with a Where filter the SQL idiom is `AVG(CASE WHEN p THEN sel END)`
                    // (no ELSE - NULL excludes the row from the average). Use `NULL` as the
                    // unmatched-row branch so the average only counts matching rows.
                    return EmitGroupAggregateWithOptionalFilter(methodCall, alias, "AVG", "NULL",
                        "AVG requires a selector expression (e.g., g.Average(x => x.Amount)). AVG(*) is not valid SQL.");
                case "Min":
                    // Like AVG, MIN with a filter uses NULL for unmatched rows so the MIN only
                    // considers matching rows. Returning null when no selector lets the existing
                    // null-fallback path continue (matching pre-fix behaviour).
                    return EmitGroupAggregateWithOptionalFilter(methodCall, alias, "MIN", "NULL", errorMessage: null);
                case "Max":
                    return EmitGroupAggregateWithOptionalFilter(methodCall, alias, "MAX", "NULL", errorMessage: null);
                case "First":
                case "FirstOrDefault":
                case "Last":
                case "LastOrDefault":
                case "ElementAt":
                case "ElementAtOrDefault":
                    // `g.OrderBy(...).Select(...).First()` and friends lower to a correlated
                    // single-row subquery when the caller supplies the outer group-key SQL
                    // and the chain is an ordered scalar shape.
                    if (groupBySql != null
                        && TryTranslateGroupOrderedFirst(methodCall, alias, groupBySql) is { } orderedFirstSql)
                        return orderedFirstSql;
                    goto case "Single";
                case "Single":
                case "SingleOrDefault":
                    // Single/SingleOrDefault assert the group holds exactly one matching
                    // element — SQL cannot honor the throw-on-multiple contract, so they
                    // stay fail-closed. The ordered positional operators reach here only
                    // when the chain is not an ordered scalar shape.
                    throw new NormUnsupportedFeatureException(
                        $"`g.{methodName}(...)` inside a grouping projection translates only as an ordered " +
                        "scalar chain: optional `Where(...)` filters, `OrderBy/OrderByDescending` with optional " +
                        "`ThenBy/ThenByDescending`, an optional `Select(x => scalar)`, then " +
                        "First/FirstOrDefault/Last/LastOrDefault/ElementAt projecting a scalar member. " +
                        "`Single`/`SingleOrDefault` cannot honor their throw-on-multiple contract in SQL. " +
                        "For whole-entity results or other shapes, project the group elements with " +
                        "`g.ToList()` (when streaming is acceptable) and apply the operation client-side.");
                default:
                    return null;
            }
        }

        /// <summary>
        /// Emits a SQL aggregate (SUM / AVG / MIN / MAX) over a group, honouring any
        /// `Where(...)` filters that wrap the IGrouping source. The unmatched-row branch
        /// is the caller's choice: `0` for SUM (additive identity) or `NULL` for
        /// AVG / MIN / MAX (excluded by the aggregate semantics).
        /// </summary>
        private string? EmitGroupAggregateWithOptionalFilter(
            MethodCallExpression methodCall,
            string alias,
            string sqlAgg,
            string unmatchedBranchSql,
            string? errorMessage)
        {
            var selectorArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                ? methodCall.Arguments[1]
                : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
            var selector = selectorArg != null ? StripQuotes(selectorArg) as LambdaExpression : null;
            if (selector == null)
            {
                if (errorMessage != null)
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, errorMessage));
                return null;
            }
            selector = ExpandGroupElementSelector(selector);

            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);

            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
            var columnSql = visitor.Translate(selector.Body);
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);

            // Decimal aggregate selectors route through the provider's full-precision hook
            // (mirrors the top-level paths in QueryTranslator.Aggregates.cs): SQLite uses
            // registered decimal aggregates / collation-ordered MIN-MAX over the raw TEXT
            // operand, exact at full precision; providers with native DECIMAL emit the plain
            // aggregate. AverageAggregateOperand is identity for decimal.
            var selBodyType = Nullable.GetUnderlyingType(selector.Body.Type) ?? selector.Body.Type;
            bool decimalOperand = selBodyType == typeof(decimal);

            // C# Average over ints is a double; SQL Server's AVG(int) truncates to int, so its
            // provider hook casts integral operands to FLOAT (identity elsewhere). Mirrors the
            // top-level aggregate paths in QueryTranslator.Aggregates.cs.
            if (!decimalOperand && sqlAgg == "AVG")
                columnSql = _provider.AverageAggregateOperand(columnSql, selector.Body.Type);

            var whereFilter = ExtractAggregateSourceFilter(methodCall);
            if (whereFilter == null)
                return decimalOperand
                    ? _provider.DecimalAggregateSql(sqlAgg, columnSql)
                    : $"{sqlAgg}({columnSql})";

            // Rebind the filter lambda's parameter onto the selector's parameter so both
            // expressions reference the same group-element alias, then translate the
            // predicate against that alias and emit `AGG(CASE WHEN pred THEN sel ELSE
            // <unmatched> END)`.
            var reboundBody = new nORM.Internal.ParameterReplacer(whereFilter.Parameters[0], selector.Parameters[0]).Visit(whereFilter.Body)!;
            var reboundFilter = Expression.Lambda(reboundBody, selector.Parameters[0]);
            var predSql = TranslateGroupPredicateBody(reboundFilter, alias);
            var caseSql = $"CASE WHEN {predSql} THEN {columnSql} ELSE {unmatchedBranchSql} END";
            return decimalOperand
                ? _provider.DecimalAggregateSql(sqlAgg, caseSql)
                : $"{sqlAgg}({caseSql})";
        }

        private LambdaExpression ExpandGroupElementSelector(LambdaExpression selector)
        {
            if (_groupByElementSelector == null
                || selector.Parameters.Count != 1
                || selector.Parameters[0].Type != _groupByElementSelector.Body.Type)
            {
                return selector;
            }

            var body = new ParameterReplacer(selector.Parameters[0], _groupByElementSelector.Body).Visit(selector.Body)!;
            body = new ProjectionMemberReplacer().Visit(body)!;
            return Expression.Lambda(body, _groupByElementSelector.Parameters);
        }
    }
}
