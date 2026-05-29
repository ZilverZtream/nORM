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
            var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
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

            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            _sql.AppendAggregateFunction(sqlFunction, columnSql);

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

            // GroupBy after a Take/Skip-windowed source — wrap the windowed source as a
            // derived table so GROUP BY aggregates only the LIMITed window, not the full
            // table. Sister of the post-Take/Skip fixes in 3040f49 / e0f1397 / 99a02ce /
            // a1eb69e / bfc8180. Restricted to the immediate-source-is-Take/Skip case
            // (same recursion-avoidance rationale as the WhereTranslator branch).
            string alias;
            bool sourceIsWindowed = sourceQuery is MethodCallExpression directGbSrc
                && directGbSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip);
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
            var param = keySelectorLambda.Parameters[0];
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);

            string groupBySql;
            // Composite key (p => new { p.A, p.B }) → translate each arg individually so the
            // emitted GROUP BY clause is comma-separated, and remember the per-member SQL so
            // the result-projection path can resolve `g.Key.A` back to its column.
            if (keySelectorLambda.Body is NewExpression compositeKey)
            {
                var parts = new List<string>(compositeKey.Arguments.Count);
                for (int i = 0; i < compositeKey.Arguments.Count; i++)
                {
                    var vctxPart = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                    var partVisitor = FastExpressionVisitorPool.Get(in vctxPart);
                    var partSql = partVisitor.Translate(compositeKey.Arguments[i]);
                    foreach (var kvp in partVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(partVisitor);
                    // Decimal columns store as TEXT; grouping on raw text treats
                    // '10.5' / '10.50' as distinct keys. Coerce to REAL so
                    // numerically-equal values land in the same group. Sister
                    // to OrderBy fix (c8b8c6b). Same emit applied to both the
                    // GROUP BY and the SELECT-side key-member so SQLite
                    // matches the projection back to the grouped expression.
                    var partType = Nullable.GetUnderlyingType(compositeKey.Arguments[i].Type) ?? compositeKey.Arguments[i].Type;
                    if (partType == typeof(decimal))
                        partSql = _provider.NormalizeDecimalForCompare(partSql);
                    parts.Add(partSql);
                    _groupBy.Add(partSql);
                    var memberName = compositeKey.Members?[i]?.Name ?? $"Item{i + 1}";
                    _compositeKeyMemberSql[memberName] = partSql;
                }
                groupBySql = string.Join(", ", parts);
            }
            else
            {
                // Constant key (e.g. `_ => 1` from the string-concat aggregate rewrite) means
                // "group all rows into a single group". SQL Server rejects GROUP BY <constant>
                // and MySQL ONLY_FULL_GROUP_BY rejects it too. Omit GROUP BY entirely — a plain
                // aggregate SELECT without GROUP BY gives the same single-group semantics.
                if (keySelectorLambda.Body is ConstantExpression)
                {
                    groupBySql = string.Empty;
                    // Do NOT add to _groupBy — no GROUP BY clause will be emitted.
                }
                else
                {
                    var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx2);
                    groupBySql = visitor.Translate(keySelectorLambda.Body);
                    // Use AddLiteralParameter (not AddParameter) so the inline-constant `@p0` from
                    // a COALESCE-fallback / literal-bearing key selector isn't also flagged in
                    // `_compiledParams`. Flagging it makes BindPlanParameters treat the slot as a
                    // compiled-query closure binding and skip the literal value at execution time —
                    // SQLite then throws `Must add values for the following parameters`. Matches the
                    // composite-key path above which already uses AddLiteralParameter.
                    foreach (var kvp in visitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(visitor);
                    // Decimal key: coerce to REAL (see composite-key path above).
                    var keyType = Nullable.GetUnderlyingType(keySelectorLambda.Body.Type) ?? keySelectorLambda.Body.Type;
                    if (keyType == typeof(decimal))
                        groupBySql = _provider.NormalizeDecimalForCompare(groupBySql);
                    _groupBy.Add(groupBySql);
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
                         && TranslateGroupAggregateMethod(methodCall, alias) is { } aggregateSql)
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
                                    if (subArg is MethodCallExpression subAgg
                                        && TranslateGroupAggregateMethod(subAgg, alias) is { } subAggSql)
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
                                        var subVctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
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
                                var innerAggregate = TranslateGroupAggregateMethod(aggregateCall, alias);
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
                                var vctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
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
                        _sql.AppendFragment(" FROM ").Append(_mapping.EscTable).Append(' ').Append(alias);
                }
            }
            finally
            {
                _selectItemsPool.Return(selectItems);
            }
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
        /// Extracts the predicate lambda from an Enumerable / IGrouping aggregate call —
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
            var vctx = new VisitorContext(_ctx, _mapping, _provider, predicate.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            var sql = visitor.Translate(predicate.Body);
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);
            return sql;
        }

        private string? TranslateGroupAggregateMethod(MethodCallExpression methodCall, string alias)
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
                var vctxAgg = new VisitorContext(_ctx, _mapping, _provider, aggLambda.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
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
                        // 1-arg `g.Count()` → COUNT(*). 2-arg `g.Count(predicate)` (extension
                        // method form Enumerable.Count(source, predicate) puts the lambda at [1])
                        // must apply the predicate: COUNT(CASE WHEN <pred> THEN 1 ELSE NULL END)
                        // — NULL excluded by SQL count semantics, matching .NET behaviour.
                        var predicate = ExtractAggregatePredicate(methodCall);
                        if (predicate == null) return "COUNT(*)";
                        var predSql = TranslateGroupPredicateBody(predicate, alias);
                        return $"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)";
                    }
                case "Any":
                    {
                        // 1-arg `g.Any()` → CAST(COUNT(*) > 0 AS INT) but in a grouped projection
                        // the group is guaranteed non-empty so it's always true — emit literal `1`.
                        // 2-arg `g.Any(predicate)` → `MAX(CASE WHEN pred THEN 1 ELSE 0 END) = 1`.
                        // SQLite returns 0/1; the materializer coerces to bool. Matches .NET
                        // Enumerable.Any semantics (true when ≥1 row satisfies the predicate).
                        var predicate = ExtractAggregatePredicate(methodCall);
                        if (predicate == null) return "1";
                        var predSql = TranslateGroupPredicateBody(predicate, alias);
                        return $"(MAX(CASE WHEN {predSql} THEN 1 ELSE 0 END) = 1)";
                    }
                case "All":
                    {
                        // `g.All(predicate)` → `MIN(CASE WHEN pred THEN 1 ELSE 0 END) = 1`. True
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
                    // (no ELSE — NULL excludes the row from the average). Use `NULL` as the
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
                case "Single":
                case "SingleOrDefault":
                case "ElementAt":
                case "ElementAtOrDefault":
                    // `g.OrderBy(...).Select(...).First()` and friends require a correlated
                    // subquery emit (SELECT sel FROM tbl WHERE groupKey = outer.groupKey ORDER
                    // BY sortKey LIMIT 1). nORM's group-aggregate path doesn't yet plumb that
                    // shape, so surface a clear exception that points at the supported
                    // `g.Min(selector)` / `g.Max(selector)` equivalents rather than silently
                    // emitting nothing for the column (which crashes the materializer).
                    throw new NormUnsupportedFeatureException(
                        $"`g.{methodName}(...)` inside a grouping projection requires a correlated subquery " +
                        "that nORM does not yet emit. For `g.OrderBy(sel).First()` / `OrderByDescending(sel).First()` " +
                        "use `g.Min(sel)` / `g.Max(sel)` instead. For more complex shapes, project the group " +
                        "elements into a list with `g.ToList()` (when streaming is acceptable) and apply the " +
                        "operation client-side.");
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

            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
            var columnSql = visitor.Translate(selector.Body);
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);

            // Mirror the HandleDirectAggregate REAL coercion for decimal
            // aggregate selectors. SQLite decimal columns are stored as TEXT,
            // so SUM/AVG/MIN/MAX inherit text storage class and lex-compare
            // ('10.5' < '2.0'). The same precision tradeoff applies — REAL is
            // IEEE-754 binary double so aggregate results are approximate;
            // SqlServer/Postgres/MySQL use native DECIMAL and are unaffected.
            var selBodyType = Nullable.GetUnderlyingType(selector.Body.Type) ?? selector.Body.Type;
            if (selBodyType == typeof(decimal))
            {
                columnSql = _provider.NormalizeDecimalForCompare(columnSql);
            }

            var whereFilter = ExtractAggregateSourceFilter(methodCall);
            if (whereFilter == null) return $"{sqlAgg}({columnSql})";

            // Rebind the filter lambda's parameter onto the selector's parameter so both
            // expressions reference the same group-element alias, then translate the
            // predicate against that alias and emit `AGG(CASE WHEN pred THEN sel ELSE
            // <unmatched> END)`.
            var reboundBody = new nORM.Internal.ParameterReplacer(whereFilter.Parameters[0], selector.Parameters[0]).Visit(whereFilter.Body)!;
            var reboundFilter = Expression.Lambda(reboundBody, selector.Parameters[0]);
            var predSql = TranslateGroupPredicateBody(reboundFilter, alias);
            return $"{sqlAgg}(CASE WHEN {predSql} THEN {columnSql} ELSE {unmatchedBranchSql} END)";
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

        private Expression HandleDirectAggregate(MethodCallExpression node)
        {
            // Handle direct aggregate calls like query.Sum(x => x.Amount)
            var sourceQuery = node.Arguments[0];

            // Sum/Min/Max/Average over a Take/Skip-windowed source — wrap the source
            // as a derived table and emit the aggregate against the wrap alias.
            // Sister of the post-Take/Skip family.
            if (sourceQuery is MethodCallExpression aggWinSrc
                && aggWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
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
                    var vctxA = new VisitorContext(_ctx, subMapA, _provider, sp, winAliasA, _correlatedParams, _compiledParams, _paramMap, _recursionDepth + 1, _params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctxA);
                    var colSql = visitor.Translate(selA.Body);
                    foreach (var kvp in visitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(visitor);
                    _sql.Append("SELECT ").Append(aggUpper).Append('(').Append(colSql).Append(") FROM (")
                        .Append(subPlanA.Sql).AppendFragment(") AS ").Append(winAliasA);
                }
                return node;
            }

            Visit(sourceQuery);
            _methodName = node.Method.Name;

            if (node.Arguments.Count > 1 && StripQuotes(node.Arguments[1]) is LambdaExpression selector)
            {
                var param = selector.Parameters[0];
                var alias = EscapeAlias("T" + _joinCounter);
                if (!_correlatedParams.ContainsKey(param))
                    _correlatedParams[param] = (_mapping, alias);
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var columnSql = visitor.Translate(selector.Body);
                // AddLiteralParameter — see HandleAggregateExpression above.
                foreach (var kvp in visitor.GetParameters())
                    AddLiteralParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(visitor);
                _isAggregate = true;
                _sql.Clear();

                if (!AggregateFunctionMap.TryGetValue(node.Method.Name, out var sqlFunction))
                    sqlFunction = node.Method.Name.ToUpperInvariant();

                // Decimal columns store as TEXT in SQLite; SQL aggregates
                // inherit storage class so MIN/MAX/SUM/AVG over a decimal
                // column lex-compares mixed-magnitude values ('10.5' lex >
                // '2.0'). Coerce to REAL before aggregating so the numeric
                // semantics apply. CAST(int AS REAL) is identity-with-.0;
                // non-decimal columns unaffected.
                //
                // PRECISION TRADEOFF: REAL is IEEE-754 binary double, so
                // aggregate results are approximate -- numeric ordering and
                // comparison are correct for practical magnitudes, but the
                // returned sum/avg is subject to floating-point rounding
                // (e.g. 0.01m is not exactly representable in binary, so a
                // SUM of cents accumulates tiny rounding error). Tests should
                // assert with tolerance. Exact decimal aggregate semantics on
                // SQLite require a different storage/aggregate strategy --
                // the SqlServer/Postgres/MySQL providers use native DECIMAL
                // and preserve exact semantics without this caveat.
                var selBodyType = Nullable.GetUnderlyingType(selector.Body.Type) ?? selector.Body.Type;
                if (selBodyType == typeof(decimal))
                {
                    columnSql = _provider.NormalizeDecimalForCompare(columnSql);
                }

                // Build complete SELECT ... FROM ... so the sql-length guard in Generate()
                // correctly skips the default SELECT/FROM assembly block.
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.AppendAggregateFunction(sqlFunction, columnSql);
                _sql.AppendFragment(" FROM ").Append(_mapping.EscTable).Append(' ').Append(alias);
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
            var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
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
                $"SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {_mapping.EscTable} {alias} WHERE {subqueryWhere}) THEN 1 ELSE 0 END");

            // Signal to Build() that this is a scalar query so it uses the scalar materializer
            // path (reads position 0, converts via Convert.ChangeType to bool).
            _isAggregate = true;

            return node;
        }
        private static Func<object, object> CreateObjectKeySelector(LambdaExpression keySelector)
        {
            var parameterType = keySelector.Parameters[0].Type;
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
                    return result ?? (object)DBNull.Value;
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
            var innerParamType = resultSelector.Parameters[1].Type;
            var innerGenericArgs = innerParamType.GetGenericArguments();
            if (innerGenericArgs.Length == 0)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    $"GroupJoin result selector parameter '{resultSelector.Parameters[1].Name}' must be a generic IEnumerable<T>, but was '{innerParamType.Name}'."));
            var innerElementType = innerGenericArgs[0];
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
        /// <summary>
        /// Compiles <paramref name="keySelectorLambda"/> into a client-side grouping transform
        /// that groups a materialized entity list by key, returning a typed
        /// <c>List&lt;IGrouping&lt;K, V&gt;&gt;</c>.  Stored as
        /// <see cref="_postMaterializeTransform"/> so the normal entity materializer handles
        /// row reading; the transform runs once after all rows are in memory.
        /// </summary>
        private void InstallGroupingTransform(LambdaExpression keySelectorLambda, LambdaExpression? elementSelectorLambda = null)
        {
            var entityType = keySelectorLambda.Parameters[0].Type;
            var keyType = keySelectorLambda.Body.Type;
            var elementType = elementSelectorLambda?.Body.Type ?? entityType;

            // Compile key selector: entity → boxed key (object?)
            var objParam = Expression.Parameter(typeof(object), "e");
            var castEntity = Expression.Convert(objParam, entityType);
            var reboundKey = new ParameterReplacer(keySelectorLambda.Parameters[0], castEntity).Visit(keySelectorLambda.Body)!;
            var boxedKey = keyType.IsValueType ? (Expression)Expression.Convert(reboundKey, typeof(object)) : reboundKey;
            var keyFuncExpr = Expression.Lambda<Func<object, object?>>(boxedKey, objParam);
            ExpressionUtils.ValidateExpression(keyFuncExpr);
            var timeout = ExpressionUtils.GetCompilationTimeout(keyFuncExpr);
            using var cts = new CancellationTokenSource(timeout);
            var keyFunc = ExpressionUtils.CompileWithFallback(keyFuncExpr, cts.Token);

            Func<object, object?> elementFunc;
            if (elementSelectorLambda != null)
            {
                var elementObjParam = Expression.Parameter(typeof(object), "e");
                var castElementEntity = Expression.Convert(elementObjParam, entityType);
                var reboundElement = new ParameterReplacer(elementSelectorLambda.Parameters[0], castElementEntity).Visit(elementSelectorLambda.Body)!;
                var boxedElement = elementType.IsValueType ? (Expression)Expression.Convert(reboundElement, typeof(object)) : reboundElement;
                var elementFuncExpr = Expression.Lambda<Func<object, object?>>(boxedElement, elementObjParam);
                ExpressionUtils.ValidateExpression(elementFuncExpr);
                var elementTimeout = ExpressionUtils.GetCompilationTimeout(elementFuncExpr);
                using var elementCts = new CancellationTokenSource(elementTimeout);
                elementFunc = ExpressionUtils.CompileWithFallback(elementFuncExpr, elementCts.Token);
            }
            else
            {
                elementFunc = static row => row;
            }

            // Reflection pieces needed inside the transform closure
            var groupingType = typeof(IGrouping<,>).MakeGenericType(keyType, elementType);
            var concreteType = typeof(ClientGrouping<,>).MakeGenericType(keyType, elementType);
            var groupingCtor = concreteType.GetConstructor(
                new[] { keyType, typeof(IEnumerable<>).MakeGenericType(elementType) })!;
            var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast))!.MakeGenericMethod(elementType);
            var resultListType = typeof(List<>).MakeGenericType(groupingType);

            // Sentinel used as dictionary key when the actual group key is null,
            // since Dictionary<object,…> does not permit null keys.
            var nullSentinel = new object();

            System.Collections.IList Transform(DbContext ctx, System.Collections.IList rows)
            {
                // Preserve first-seen key order (LINQ GroupBy semantics)
                var keyOrder = new List<object?>();
                var buckets = new Dictionary<object, List<object>>(EqualityComparer<object>.Default);
                foreach (var row in rows)
                {
                    var key = row == null ? null : keyFunc(row);
                    var dictKey = (object?)key ?? nullSentinel;
                    if (!buckets.TryGetValue(dictKey, out var bucket))
                    {
                        bucket = new List<object>();
                        buckets[dictKey] = bucket;
                        keyOrder.Add(key);   // actual key (may be null)
                    }
                    bucket.Add(elementFunc(row!)!);
                }

                var result = (System.Collections.IList)Activator.CreateInstance(resultListType, keyOrder.Count)!;
                foreach (var keyObj in keyOrder)
                {
                    var dictKey = (object?)keyObj ?? nullSentinel;
                    var items = castMethod.Invoke(null, new object?[] { buckets[dictKey] })!;
                    var grouping = groupingCtor.Invoke(new object?[] { keyObj, items })!;
                    result.Add(grouping);
                }
                return result;
            }

            _postMaterializeTransform = Transform;
        }

        private bool TryCreateProjectedTailSelector(LambdaExpression selector, out LambdaExpression projectedSelector)
        {
            projectedSelector = selector;
            if (_projection == null || selector.Parameters.Count != 1)
                return false;

            var projectedType = _projection.Body.Type;
            var projectedParam = Expression.Parameter(projectedType, "__projected");
            var rewriter = new ProjectedTailSelectorRewriter(selector.Parameters[0], projectedParam, projectedType);
            var body = rewriter.Visit(selector.Body)!;
            if (!rewriter.Replaced)
                return false;

            projectedSelector = Expression.Lambda(body, projectedParam);
            return true;
        }

        private static bool SourceContainsGroupBy(Expression source)
        {
            if (source is MethodCallExpression call)
            {
                if (call.Method.Name == nameof(Queryable.GroupBy))
                    return true;
                return call.Arguments.Count > 0 && SourceContainsGroupBy(call.Arguments[0]);
            }

            return false;
        }

        private sealed class ProjectedTailSelectorRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _source;
            private readonly ParameterExpression _projected;
            private readonly Type _projectedType;

            public ProjectedTailSelectorRewriter(ParameterExpression source, ParameterExpression projected, Type projectedType)
            {
                _source = source;
                _projected = projected;
                _projectedType = projectedType;
            }

            public bool Replaced { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression == _source && node.Type == _projectedType)
                {
                    Replaced = true;
                    return _projected;
                }

                return base.VisitMember(node);
            }
        }

        private sealed class ProjectionMemberReplacer : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                var visitedExpression = node.Expression == null ? null : Visit(node.Expression);
                if (visitedExpression is NewExpression newExpr)
                {
                    if (newExpr.Members != null)
                    {
                        for (int i = 0; i < newExpr.Members.Count; i++)
                        {
                            if (newExpr.Members[i].Name == node.Member.Name)
                                return Visit(newExpr.Arguments[i]);
                        }
                    }

                    var parameters = newExpr.Constructor?.GetParameters();
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length && i < newExpr.Arguments.Count; i++)
                        {
                            if (string.Equals(parameters[i].Name, node.Member.Name, StringComparison.OrdinalIgnoreCase))
                                return Visit(newExpr.Arguments[i]);
                        }
                    }
                }
                if (visitedExpression is MemberInitExpression memberInit)
                {
                    foreach (var binding in memberInit.Bindings)
                    {
                        if (binding is MemberAssignment ma && ma.Member.Name == node.Member.Name)
                            return Visit(ma.Expression);
                    }
                }
                if (visitedExpression != node.Expression)
                    return node.Update(visitedExpression);

                return base.VisitMember(node);
            }
        }

        private static bool IsGroupingSequenceType(Type type)
        {
            if (!type.IsGenericType)
                return false;

            var def = type.GetGenericTypeDefinition();
            return def == typeof(IGrouping<,>) || def == typeof(IEnumerable<>);
        }
    }

    /// <summary>
    /// Concrete IGrouping implementation returned by the streaming GroupBy transform.
    /// </summary>
    internal sealed class ClientGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private readonly List<TElement> _elements;
        public TKey Key { get; }
        public ClientGrouping(TKey key, IEnumerable<TElement> elements)
        {
            Key = key;
            _elements = new List<TElement>(elements);
        }
        public IEnumerator<TElement> GetEnumerator() => _elements.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _elements.GetEnumerator();
    }
}
