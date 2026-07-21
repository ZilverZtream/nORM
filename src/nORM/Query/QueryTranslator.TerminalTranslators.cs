using System;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ElementAtTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Windowed-source branch — wraps the windowed source as a derived
            /// table and applies the index-skip + LIMIT 1 on the outer SELECT,
            /// so ElementAt(k) over Take(N) bounds k to the windowed set rather
            /// than indexing into the full table.
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanEa = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapEa);
                t._mapping = subMapEa;
                t.MergeSubPlanParameters(subPlanEa);
                var winAliasEa = t.EscapeAlias("__wea" + t._joinCounter++);
                t._sql.AppendFragment("SELECT * FROM (").Append(subPlanEa.Sql).AppendFragment(") AS ").Append(winAliasEa);
                // Apply the index-skip on the outer wrap. Bind from constant or
                // compiled-param the same way the unwindowed path does.
                if (t.TryBindPagingParameter(node.Arguments[1], out var eaName))
                {
                    t._skipParam = eaName;
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int eaIdx))
                {
                    t._skip = eaIdx;
                }
                else
                {
                    throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, "ElementAt without constant integer index"));
                }
                t._take = 1;
                var pNameEa = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
                t._params[pNameEa] = 1;
                t._takeParam = pNameEa;
                t._takeSetByTerminal = true;
                t._singleResult = node.Method.Name == "ElementAt";
                // The outer SELECT preserves the inner ordering naturally
                // (derived tables retain row order on SQLite and most providers),
                // so no outer ORDER BY is required for the LIMIT/OFFSET to be
                // deterministic — the inner sub-plan already has its ORDER BY.
                return node;
            }

            /// <summary>
            /// Applies <c>ElementAt</c> or <c>ElementAtOrDefault</c> semantics by adjusting the skip/take parameters.
            /// </summary>
            /// <param name="t">The active <see cref="QueryTranslator"/>.</param>
            /// <param name="node">The method call expression for the element access.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A reshaped source shifts positions (Prepend moves every index by one),
                // so the server-side OFFSET/LIMIT must not run — index in memory instead.
                if (TryTranslateReshapedScalarTerminal(t, node) is { } reshapedEa)
                    return reshapedEa;
                if (node.Arguments[0] is MethodCallExpression eaWinSrc
                    && eaWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }
                var terminalMethodEa = t._methodName;
                var elementSource = t.Visit(node.Arguments[0]);
                t._methodName = terminalMethodEa;
                if (t.TryBindPagingParameter(node.Arguments[1], out var eName))
                {
                    if (t._skipParam != null)
                        t._skipParam = $"({t._skipParam} + {eName})";
                    else if (t._skip != null)
                    {
                        t._skipParam = $"({t._skip} + {eName})";
                        t._skip = null;
                    }
                    else
                        t._skipParam = eName;
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int index))
                {
                    if (t._skipParam != null)
                        t._skipParam = $"({t._skipParam} + {index})";
                    else
                        t._skip = (t._skip ?? 0) + index;
                }
                else
                {
                    throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, "ElementAt without constant integer index"));
                }

                t._take = 1;
                var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
                t._params[pName] = 1;
                t._takeParam = pName;
                t._takeSetByTerminal = true;
                t._singleResult = t._methodName == "ElementAt";
                if (t._orderBy.Count == 0)
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, true));
                return elementSource;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class FirstSingleTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates <c>First</c>, <c>Single</c> and their <c>OrDefault</c> variants, applying optional predicates
            /// and limiting the result set accordingly.
            /// </summary>
            /// <param name="t">The translator in use.</param>
            /// <param name="node">The method call expression for the terminal operator.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A reshaped source invalidates the server-side LIMIT shortcut (Chunk
                // regroups rows, a predicate must also test the appended element), so
                // evaluate the terminal in memory over the reshaped rows.
                if (TryTranslateReshapedScalarTerminal(t, node) is { } reshapedFs)
                    return reshapedFs;
                // First/Single with predicate over a Take/Skip-windowed source — the
                // predicate must evaluate only against the windowed rows. The default
                // path appends WHERE pred to _where and the SQL pipeline emits
                // `WHERE pred ORDER BY … LIMIT N` against the full table (WHERE
                // evaluates before LIMIT), silently returning rows OUTSIDE the
                // window. Wrap the windowed source as a derived table so the
                // predicate runs inside it. Sister of HandleSetOperation's
                // Any/All/Contains-after-Take windowed branch. Two predicate
                // shapes: 2-arg `First(source, pred)` and 1-arg
                // `First(Where(source, pred))` (the latter emitted by
                // NormAsyncExtensions.FirstAsync/FirstOrDefaultAsync(predicate)).
                LambdaExpression? winPred = null;
                Expression? winSource = null;
                if (node.Arguments.Count > 1
                    && StripQuotes(node.Arguments[1]) is LambdaExpression wp2
                    && QueryTranslator.SourceHasTakeOrSkip(node.Arguments[0]))
                {
                    winPred = wp2;
                    winSource = node.Arguments[0];
                }
                else if (node.Arguments.Count == 1
                    && node.Arguments[0] is MethodCallExpression wrapWhere
                    && wrapWhere.Method.Name == nameof(Queryable.Where)
                    && wrapWhere.Arguments.Count == 2
                    && StripQuotes(wrapWhere.Arguments[1]) is LambdaExpression wp1
                    && QueryTranslator.SourceHasTakeOrSkip(wrapWhere.Arguments[0]))
                {
                    winPred = wp1;
                    winSource = wrapWhere.Arguments[0];
                }
                if (winPred != null && winSource != null)
                {
                    var subPlan = t.TranslateInSubContext(winSource, t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMappingFs);
                    t._mapping = subMappingFs;
                    t.MergeSubPlanParameters(subPlan);
                    var winParam = winPred.Parameters[0];
                    var winAlias = t.EscapeAlias("__wfs" + t._joinCounter++);
                    if (!t._correlatedParams.ContainsKey(winParam))
                        t._correlatedParams[winParam] = (subMappingFs, winAlias);
                    var vctxWin = new VisitorContext(t._ctx, subMappingFs, t._provider, winParam, winAlias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                    var winVisitor = FastExpressionVisitorPool.Get(in vctxWin);
                    var winPredSql = winVisitor.Translate(winPred.Body);
                    foreach (var kvp in winVisitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(winVisitor);
                    t._sql.Append("SELECT * FROM (").Append(subPlan.Sql)
                          .Append(") AS ").Append(winAlias);
                    if (t._where.Length > 0) t._where.Append(" AND ");
                    t._where.Append('(').Append(winPredSql).Append(')');
                    var isSingleW = t._methodName == "Single" || t._methodName == "SingleOrDefault";
                    t._take = isSingleW ? 2 : 1;
                    var pNameW = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.Index++;
                    t._params[pNameW] = t._take;
                    t._takeParam = pNameW;
                    t._takeSetByTerminal = true;
                    t._singleResult = t._methodName == "First" || t._methodName == "Single";
                    return node;
                }
                // Visit the source FIRST (preserving the terminal method name, which visiting child
                // operators overwrites with e.g. "Where"), so _projection / _mapping are established
                // before the predicate is translated — mirrors HandleAllOperation. Translating the
                // predicate before the source was visited left a predicate over a scalar projection
                // (Select(x => proj).First(a => a OP ...)) with an unmapped parameter, rendering `a` as
                // an empty operand (`WHERE (( >= @p0))`).
                var terminalMethod = t._methodName;
                var result = t.Visit(node.Arguments[0]);
                t._methodName = terminalMethod;

                if (node.Arguments.Count > 1 && StripQuotes(node.Arguments[1]) is LambdaExpression predicate)
                {
                    // Resolve a predicate over a scalar projection through the projection (no-op for a
                    // plain entity predicate), then translate it against the now-established context.
                    predicate = t.ExpandProjection(predicate);
                    var param = predicate.Parameters[0];
                    var alias = t.EscapeAlias("T" + t._joinCounter);
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var vctxFS = new VisitorContext(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctxFS);
                    var sql = visitor.Translate(predicate.Body);
                    if (t._where.Length > 0) t._where.Append(" AND ");
                    t._where.Append($"({sql})");
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                }
                // Single/SingleOrDefault must fetch 2 rows so the caller can detect duplicates.
                // First/FirstOrDefault only need 1 row.
                var isSingle = t._methodName == "Single" || t._methodName == "SingleOrDefault";
                t._take = isSingle ? 2 : 1;
                var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.Index++;
                t._params[pName] = t._take;
                t._takeParam = pName;
                // Mark this _take as set by a terminal so the post-Take/Skip pin family
                // doesn't false-positive on `q.OrderBy(k).First()`-style chains (the
                // pins fire on `_take.HasValue && !_takeSetByTerminal`).
                t._takeSetByTerminal = true;
                t._singleResult = t._methodName == "First" || t._methodName == "Single";
                return result;
            }
        }

        /// <summary>
        /// Walks back through a source expression chain collecting OrderBy /
        /// OrderByDescending / ThenBy / ThenByDescending lambdas in primary-first
        /// order. Used by <see cref="LastTranslator"/> to mirror the source's
        /// ordering on the outer derived-table SELECT (reversed for "last"
        /// semantics).
        /// </summary>
        private static System.Collections.Generic.List<(LambdaExpression KeySelector, bool Ascending)> ExtractOrderByKeys(Expression source)
        {
            var result = new System.Collections.Generic.List<(LambdaExpression, bool)>();
            var current = source;
            while (current is MethodCallExpression mce)
            {
                bool? asc = mce.Method.Name switch
                {
                    nameof(Queryable.OrderBy) or nameof(Queryable.ThenBy) => true,
                    nameof(Queryable.OrderByDescending) or nameof(Queryable.ThenByDescending) => false,
                    _ => null
                };
                if (asc.HasValue
                    && mce.Arguments.Count == 2
                    && StripQuotes(mce.Arguments[1]) is LambdaExpression k)
                {
                    result.Insert(0, (k, asc.Value));
                }
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
            return result;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class LastTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Handles <c>Last</c> and <c>LastOrDefault</c> by reversing the ordering and applying predicates when present.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>Last</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A reshaped source changes what "last" means (Append puts its element
                // at the end), so the reversed-ORDER-BY LIMIT 1 shortcut must not run —
                // evaluate in memory over the reshaped rows.
                if (TryTranslateReshapedScalarTerminal(t, node) is { } reshapedLast)
                    return reshapedLast;
                // Last with predicate over a Take/Skip-windowed source — the predicate
                // must evaluate only against the windowed rows, and "last" must be the
                // last row of the window matching pred (not the last of the full table).
                // Sister of FirstSingleTranslator's windowed branch (commit e0f1397).
                // To preserve "last" semantics across derived-table wrap, lift the
                // source's OrderBy keys and emit them DESCENDING on the outer SELECT.
                LambdaExpression? lastWinPred = null;
                Expression? lastWinSource = null;
                if (node.Arguments.Count > 1
                    && StripQuotes(node.Arguments[1]) is LambdaExpression lwp2
                    && QueryTranslator.SourceHasTakeOrSkip(node.Arguments[0]))
                {
                    lastWinPred = lwp2;
                    lastWinSource = node.Arguments[0];
                }
                else if (node.Arguments.Count == 1
                    && node.Arguments[0] is MethodCallExpression lastWhereWrap
                    && lastWhereWrap.Method.Name == nameof(Queryable.Where)
                    && lastWhereWrap.Arguments.Count == 2
                    && StripQuotes(lastWhereWrap.Arguments[1]) is LambdaExpression lwp1
                    && QueryTranslator.SourceHasTakeOrSkip(lastWhereWrap.Arguments[0]))
                {
                    lastWinPred = lwp1;
                    lastWinSource = lastWhereWrap.Arguments[0];
                }
                if (lastWinSource != null)
                {
                    var lastSubPlan = t.TranslateInSubContext(lastWinSource, t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var lastSubMap);
                    t._mapping = lastSubMap;
                    t.MergeSubPlanParameters(lastSubPlan);
                    var lastAlias = t.EscapeAlias("__wlast" + t._joinCounter++);
                    t._sql.Append("SELECT * FROM (").Append(lastSubPlan.Sql)
                          .Append(") AS ").Append(lastAlias);
                    if (lastWinPred != null)
                    {
                        var lwParam = lastWinPred.Parameters[0];
                        if (!t._correlatedParams.ContainsKey(lwParam))
                            t._correlatedParams[lwParam] = (lastSubMap, lastAlias);
                        var vctxLW = new VisitorContext(t._ctx, lastSubMap, t._provider, lwParam, lastAlias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                        var lwVisitor = FastExpressionVisitorPool.Get(in vctxLW);
                        var lwSql = lwVisitor.Translate(lastWinPred.Body);
                        foreach (var kvp in lwVisitor.GetParameters())
                            t._params[kvp.Key] = kvp.Value;
                        if (t._params.Count > t._parameterManager.Index)
                            t._parameterManager.Index = t._params.Count;
                        FastExpressionVisitorPool.Return(lwVisitor);
                        if (t._where.Length > 0) t._where.Append(" AND ");
                        t._where.Append('(').Append(lwSql).Append(')');
                    }
                    // Reverse the source's OrderBy onto the outer SELECT so we pick
                    // the LAST row of the matched window. If the source has no
                    // explicit OrderBy, fall back to the mapping's key columns
                    // ordered descending, matching the unwindowed-Last default.
                    var orderKeys = ExtractOrderByKeys(lastWinSource);
                    if (orderKeys.Count > 0)
                    {
                        foreach (var (keyLambda, asc) in orderKeys)
                        {
                            var okParam = keyLambda.Parameters[0];
                            if (!t._correlatedParams.ContainsKey(okParam))
                                t._correlatedParams[okParam] = (lastSubMap, lastAlias);
                            var vctxOk = new VisitorContext(t._ctx, lastSubMap, t._provider, okParam, lastAlias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                            var okVisitor = FastExpressionVisitorPool.Get(in vctxOk);
                            var okSql = okVisitor.Translate(keyLambda.Body);
                            FastExpressionVisitorPool.Return(okVisitor);
                            t._orderBy.Add((okSql, !asc));
                        }
                    }
                    else
                    {
                        foreach (var key in lastSubMap.KeyColumns)
                            t._orderBy.Add(($"{lastAlias}.{key.EscCol}", false));
                    }
                    t._take = 1;
                    var pNameLW = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.Index++;
                    t._params[pNameLW] = 1;
                    t._takeParam = pNameLW;
                    t._takeSetByTerminal = true;
                    t._singleResult = t._methodName == "Last";
                    return node;
                }
                if (node.Arguments.Count > 1)
                {
                    var lastPredicate = StripQuotes(node.Arguments[1]) as LambdaExpression;
                    if (lastPredicate != null)
                    {
                        var param = lastPredicate.Parameters[0];
                        var alias = t.EscapeAlias("T" + t._joinCounter);
                        if (!t._correlatedParams.ContainsKey(param))
                            t._correlatedParams[param] = (t._mapping, alias);
                        var vctxLast = new VisitorContext(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth, t._params.Count);
                        var visitor = FastExpressionVisitorPool.Get(in vctxLast);
                        var sql = visitor.Translate(lastPredicate.Body);
                        if (t._where.Length > 0) t._where.Append(" AND ");
                        t._where.Append($"({sql})");
                        foreach (var kvp in visitor.GetParameters())
                            t._params[kvp.Key] = kvp.Value;
                        if (t._params.Count > t._parameterManager.Index)
                            t._parameterManager.Index = t._params.Count;
                        FastExpressionVisitorPool.Return(visitor);
                    }
                }
                var terminalMethodLast = t._methodName;
                var lastSrc = t.Visit(node.Arguments[0]);
                t._methodName = terminalMethodLast;
                if (t._orderBy.Count > 0)
                {
                    for (int i = 0; i < t._orderBy.Count; i++)
                    {
                        var (col, asc) = t._orderBy[i];
                        t._orderBy[i] = (col, !asc);
                    }
                }
                else
                {
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, false));
                }
                t._take = 1;
                var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.Index++;
                t._params[pName] = 1;
                t._takeParam = pName;
                t._takeSetByTerminal = true;
                t._singleResult = t._methodName == "Last";
                return lastSrc;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class MinByMaxByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A reshaped source's extremum can be the appended/prepended element,
                // which the server-side ORDER BY key LIMIT 1 never sees — evaluate in
                // memory over the reshaped rows.
                if (TryTranslateReshapedScalarTerminal(t, node) is { } reshapedMb)
                    return reshapedMb;
                // Save terminal method name before visiting source — source chain overwrites _methodName.
                var terminalName = t._methodName; // "MinBy" or "MaxBy"
                var ascending = terminalName == "MinBy";
                t._orderBy.Clear();
                if (StripQuotes(node.Arguments[1]) is LambdaExpression keySelector)
                {
                    keySelector = t.ExpandProjection(keySelector);
                    var param = keySelector.Parameters[0];
                    var alias = t._outerDerivedAlias ?? t.EscapeAlias("T" + t._joinCounter);
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var vctx = new VisitorContext(t._ctx, t._mapping, t._provider, param, alias,
                        t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap,
                        t._recursionDepth, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx);
                    var keySql = visitor.Translate(keySelector.Body);
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                    // Same ordering-key treatment as OrderByTranslator: nullable keys
                    // rank nulls first on providers that default nulls-last (C# treats
                    // null as the smallest key), and TEXT-stored decimal/TimeSpan/DTO
                    // keys coerce to value ordering — a raw decimal key made MinBy
                    // return the LEXICAL minimum on SQLite.
                    var keyType = keySelector.Body.Type;
                    if (t._provider.RequiresExplicitNullOrderingForNullableKeys
                        && (!keyType.IsValueType || Nullable.GetUnderlyingType(keyType) != null))
                        t._orderBy.Add(($"({keySql} IS NOT NULL)", ascending));
                    t._orderBy.Add((t.CoerceOrderKeySql(keySql, keyType), ascending));
                }
                t._take = 1;
                var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.Index++;
                t._params[pName] = 1;
                t._takeParam = pName;
                t._takeSetByTerminal = true;
                t._singleResult = true;
                var src = t.Visit(node.Arguments[0]);
                t._methodName = terminalName; // restore after source chain overwrites it
                return src;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class CountTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Windowed-source branch — wraps the windowed source as a derived table and
            /// emits `SELECT COUNT(*) FROM (subSql) AS sub [WHERE pred]`. Kept in its own
            /// method to keep the common-path Translate stack-frame lean.
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanC = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapC);
                t._mapping = subMapC;
                t.MergeSubPlanParameters(subPlanC);
                var winAliasC = t.EscapeAlias("__wcn" + t._joinCounter++);
                t._isAggregate = true;
                t._methodName = node.Method.Name;
                if (!t._isDistinct) t._projection = null;
                t._sql.Append("SELECT COUNT(*) FROM (").Append(subPlanC.Sql).AppendFragment(") AS ").Append(winAliasC);
                if (node.Arguments.Count > 1
                    && StripQuotes(node.Arguments[1]) is LambdaExpression predLambda)
                {
                    var lp = predLambda.Parameters[0];
                    t._correlatedParams[lp] = (subMapC, winAliasC);
                    var vctxC = new VisitorContext(t._ctx, subMapC, t._provider, lp, winAliasC, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctxC);
                    var predSql = visitor.Translate(predLambda.Body);
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                    if (t._where.Length > 0) t._where.Append(" AND ");
                    t._where.Append('(').Append(predSql).Append(')');
                }
                return node;
            }

            /// <summary>
            /// Processes <c>Count</c> and <c>LongCount</c>, optionally translating a predicate and marking the query as aggregate.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for the count operation.</param>
            /// <returns>The updated expression node.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._isAggregate = true;
                // Count directly over a raw streaming GroupBy counts GROUPS — one per
                // distinct key, including a null-key group — which is exactly
                // Select(key).Distinct().Count() and stays fully server-side. (COUNT
                // over the flat rows would count rows; COUNT(DISTINCT key) would drop
                // the null-key group.)
                if (node.Arguments.Count == 1
                    && node.Arguments[0] is MethodCallExpression gbDirect
                    && IsRawStreamingGroupByShape(gbDirect)
                    && StripQuotes(gbDirect.Arguments[1]) is LambdaExpression gbKeyLambda)
                {
                    // Ordering never affects group membership, and a dangling ORDER BY
                    // column breaks the DISTINCT derived-table wrap — strip the top run
                    // of ordering operators feeding the GroupBy. Orderings below a
                    // Take/Skip stay (they decide which rows the window keeps).
                    var gbSource = StripLeadingOrderings(gbDirect.Arguments[0]);
                    var srcElementType = gbSource.Type.GetGenericArguments()[0];
                    var keyType = gbKeyLambda.Body.Type;
                    var keyProjection = Expression.Call(typeof(Queryable), nameof(Queryable.Select),
                        new[] { srcElementType, keyType }, gbSource, gbDirect.Arguments[1]);
                    var distinctKeys = Expression.Call(typeof(Queryable), nameof(Queryable.Distinct),
                        new[] { keyType }, keyProjection);
                    var rewritten = Expression.Call(typeof(Queryable), node.Method.Name,
                        new[] { keyType }, distinctKeys);
                    return t.Visit(rewritten);
                }
                // A reshape anywhere in the spine (including below a Take/Skip window)
                // must divert before the windowed COUNT(*) wrap — the sub-plan translation
                // would handle the window client-side, leaving the wrap counting a
                // sub-SELECT that no longer carries the LIMIT. Group-join and raw-GroupBy
                // result tails must divert too: COUNT(*) over the flat rows counts
                // children or rows, not groups.
                if (SourceHasClientTailReshape(node.Arguments[0])
                    || SourceHasGroupJoinResultTail(node.Arguments[0])
                    || SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    var reshapedSource = t.Visit(node.Arguments[0]);
                    if (t.TryAppendClientCountAggregate(node))
                        return reshapedSource;
                    ThrowIfClientTailReshapePending(t, node.Method.Name);
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} after a client-materialized sequence operator has no in-memory equivalent overload.");
                }
                // Count after Take/Skip-windowed source — count only the windowed rows.
                // Sister of the post-Take/Skip family (3040f49 / e0f1397 / 99a02ce /
                // a1eb69e / bfc8180 / d6de693 / e0529a0).
                if (node.Arguments[0] is MethodCallExpression countWinSrc
                    && countWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }
                var source = t.Visit(node.Arguments[0]);
                // A pending client-tail reshape (Append/Prepend/Chunk/Zip/DefaultIfEmpty(value))
                // changes the sequence AFTER materialization, so a server-side COUNT(*)
                // would report the pre-reshape row count. Reduce the reshaped rows
                // client-side instead, matching LINQ-to-Objects semantics.
                if (t.TryAppendClientCountAggregate(node))
                    return source;
                // Preserve _projection when DISTINCT is active so the COUNT(*) builder can
                // wrap as `SELECT COUNT(*) FROM (SELECT DISTINCT <proj> FROM ...) AS T0` —
                // otherwise nuke it so Count(x => predicate) doesn't try to project columns
                // that aren't relevant to the row-count.
                if (!t._isDistinct)
                    t._projection = null;
                // COUNT collapses to a single row, so the source ordering is meaningless —
                // and a surviving ORDER BY over source columns is invalid SQL on
                // Postgres/SQL Server ("column ... invalid in ORDER BY because it is not
                // contained in either an aggregate function or the GROUP BY clause").
                t._orderBy.Clear();
                t._methodName = node.Method.Name;
                if (node.Arguments.Count > 1)
                {
                    var countPredicate = StripQuotes(node.Arguments[1]) as LambdaExpression;
                    if (countPredicate != null)
                    {
                        countPredicate = t.ExpandProjection(countPredicate);
                        var param = countPredicate.Parameters[0];
                        if (!t._correlatedParams.TryGetValue(param, out var info))
                        {
                            info = (t._mapping, t.EscapeAlias("T" + t._joinCounter));
                            t._correlatedParams[param] = info;
                        }
                        var vctxCount = new VisitorContext(t._ctx, t._mapping, t._provider, param, info.Alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth, t._params.Count);
                        var visitor = FastExpressionVisitorPool.Get(in vctxCount);
                        var sql = visitor.Translate(countPredicate.Body);
                        if (t._where.Length > 0) t._where.Append(" AND ");
                        t._where.Append($"({sql})");
                        foreach (var kvp in visitor.GetParameters())
                            t._params[kvp.Key] = kvp.Value;
                        if (t._params.Count > t._parameterManager.Index)
                            t._parameterManager.Index = t._params.Count;
                        FastExpressionVisitorPool.Return(visitor);
                    }
                }
                // Don't rebuild the Count() MethodCallExpression. Visit() already returned the
                // translated source and the translator's side effects (_isAggregate, _methodName,
                // _where) capture everything the plan needs. Rebuilding via node.Update can fail
                // when the source's expression-tree type (e.g. NormQueryableImpl<TRoot>) doesn't
                // satisfy the Count<TAnon>(IQueryable<TAnon>) signature after a chain like
                // Select(anon).Distinct().Count().
                return source;
            }
        }
    }
}
