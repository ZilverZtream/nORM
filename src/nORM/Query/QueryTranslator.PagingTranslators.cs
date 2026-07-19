using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// LINQ paging counts follow Enumerable semantics: Take/TakeLast with a
        /// non-positive count yield an empty sequence and Skip/SkipLast with a
        /// negative count skip nothing -- no exception, and never the raw negative
        /// value in SQL (SQLite treats a negative LIMIT as UNLIMITED, which would
        /// silently return the whole table). Literal counts clamp at translation;
        /// parameter-bound counts clamp at bind time through this pseudo-converter
        /// registered under the paging parameter's name.
        /// </summary>
        private sealed class NonNegativePagingCount : nORM.Mapping.IValueConverter
        {
            public static readonly NonNegativePagingCount Instance = new();
            public Type ModelType => typeof(int);
            public Type ProviderType => typeof(int);
            public object? ConvertToProvider(object? modelValue)
                => modelValue is int i && i < 0 ? 0 : modelValue;
            public object? ConvertFromProvider(object? providerValue) => providerValue;
        }

        private void ClampPagingParameterAtBind(string parameterName)
            => _parameterManager.CompiledParameterConverters[parameterName] = NonNegativePagingCount.Instance;

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class TakeTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Applies the <c>Take</c> operation by tracking the row limit and corresponding parameter.
            /// </summary>
            /// <param name="t">The <see cref="QueryTranslator"/> performing translation.</param>
            /// <param name="node">The method call expression for <c>Take</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = t.Visit(node.Arguments[0]);
                if (t.IsPostMaterializeTailMode && QueryTranslator.TryGetIntValue(node.Arguments[1], out var clientTake))
                {
                    t.AppendPostMaterializeTake(Math.Max(0, clientTake));
                    return source;
                }
                // Non-literal count in tail mode (reshape, group-join, or raw GroupBy
                // result): a server LIMIT would truncate pre-reshape rows or a group's
                // children, so run Take in memory over the assembled sequence. The Try
                // method self-gates on tail mode and installs a pending raw-GroupBy
                // grouping transform first.
                if (t.TryAppendClientSequenceOperator(node))
                    return source;

                if (t.TryBindPagingParameter(node.Arguments[1], out var tName))
                {
                    t._takeParam = tName;
                    t.ClampPagingParameterAtBind(tName);
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int take))
                {
                    take = Math.Max(0, take);
                    // Inline literal Take values directly in SQL instead of parameterizing.
                    // Parameterized LIMIT (@p0) prevents SQLite's planner from using ANALYZE statistics
                    // to estimate result cardinality. Literal LIMIT (20) lets the planner optimize.
                    t._take = take;
                    t._takeParam = null;
                }
                return source;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SkipTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Implements the <c>Skip</c> operation by setting the offset and parameter information.
            /// </summary>
            /// <param name="t">The current <see cref="QueryTranslator"/>.</param>
            /// <param name="node">The method call expression for <c>Skip</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = t.Visit(node.Arguments[0]);
                // Skip in tail mode must drop assembled elements, not server rows — a
                // server OFFSET would let a prepended element bypass the skip, or
                // truncate a group's elements mid-group. The Try method self-gates on
                // tail mode and installs a pending raw-GroupBy grouping transform first.
                if (t.TryAppendClientSequenceOperator(node))
                    return source;
                // Take-then-Skip is algebraically equivalent to Skip(m).Take(n-m): both
                // return rows in the half-open range [m, n). Rewrite by shrinking the
                // already-set Take to (take - skip) so the SQL emits LIMIT (n-m) OFFSET m
                // and honours LINQ semantics without a subquery wrap. Negative or zero
                // newTake means the Take window ended before Skip began — collapse to a
                // no-row query by setting _take = 0.
                if (t._take.HasValue && QueryTranslator.TryGetIntValue(node.Arguments[1], out int skipForLiteral))
                {
                    skipForLiteral = Math.Max(0, skipForLiteral);
                    var originalTake = t._take.Value;
                    t._take = Math.Max(0, originalTake - skipForLiteral);
                    t._skip = skipForLiteral;
                    t._skipParam = null;
                    return source;
                }
                // Take(n).Skip(m) with a runtime parameter on either side: emit a
                // composite LIMIT expression that still satisfies Take(n).Skip(m) ≡
                // Skip(m).Take(n - m). The provider's ApplyPaging accepts arbitrary SQL
                // for the limit / offset slot (it just appends the string verbatim).
                if (t._take.HasValue || t._takeParam != null)
                {
                    var existingTakeExpr = t._takeParam ?? t._take!.Value.ToString();
                    string skipExpr;
                    if (t.TryBindPagingParameter(node.Arguments[1], out var sParam))
                    {
                        skipExpr = sParam;
                        t.ClampPagingParameterAtBind(sParam);
                    }
                    else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int skipLit))
                    {
                        skipExpr = Math.Max(0, skipLit).ToString();
                    }
                    else
                    {
                        throw new NormUnsupportedFeatureException(
                            "Skip argument could not be bound to a parameter or literal.");
                    }
                    // Reset the existing take fields and emit (take - skip) as the new limit
                    // expression; pin the offset. The difference can be negative even with
                    // valid inputs (Take(3).Skip(5) must return NO rows), and SQLite treats
                    // a negative LIMIT as unlimited -- clamp through the provider's
                    // expression-level hook.
                    t._take = null;
                    t._takeParam = t._ctx.RawProvider.ClampNonNegativeLimitExpression($"({existingTakeExpr} - {skipExpr})");
                    t._skip = null;
                    t._skipParam = skipExpr;
                    return source;
                }
                if (t.TryBindPagingParameter(node.Arguments[1], out var sName))
                {
                    t._skipParam = sName;
                    t.ClampPagingParameterAtBind(sName);
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int skip))
                {
                    // Inline literal Skip values directly in SQL (same rationale as Take).
                    t._skip = Math.Max(0, skip);
                    t._skipParam = null;
                }
                return source;
            }
        }

        /// <summary>
        /// Implements the ordered subset of <c>TakeWhile</c> and <c>SkipWhile</c> with
        /// a cumulative break flag. Without an upstream ordering these operators have
        /// no deterministic relational meaning, so the translator still fails closed.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class TakeSkipWhileTranslator(bool takeWhile) : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A reshaped or group-join-result source evaluates TakeWhile/SkipWhile
                // in memory over the assembled rows (already in result order), so
                // neither the ordering requirement nor the window-function SQL applies.
                // Once the source is visited, falling through to SQL generation is
                // never valid.
                if (SourceHasClientTailReshape(node.Arguments[0])
                    || SourceHasGroupJoinResultTail(node.Arguments[0])
                    || SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    var reshapedSource = t.Visit(node.Arguments[0]);
                    if (t.TryAppendClientSequenceOperator(node))
                        return reshapedSource;
                    ThrowIfClientTailReshapePending(t, node.Method.Name);
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} after a client-materialized sequence operator has no in-memory equivalent overload.");
                }
                if (node.Arguments.Count != 2 || StripQuotes(node.Arguments[1]) is not LambdaExpression predicate)
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} requires a one-argument or index-aware predicate overload for provider-mobile SQL translation.");
                }
                if (predicate.Parameters.Count is not (1 or 2))
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} requires a one-argument or index-aware predicate overload for provider-mobile SQL translation.");
                }
                var orderKeys = ExtractOrderByKeys(node.Arguments[0]);
                if (orderKeys.Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} requires an explicit OrderBy/ThenBy chain so provider-mobile SQL has a deterministic row sequence.");
                }

                var subPlan = t.TranslateInSubContext(
                    node.Arguments[0],
                    t._mapping,
                    t._parameterManager.Index,
                    t._joinCounter,
                    t._recursionDepth + 1,
                    out var subMapping);
                t._mapping = subMapping;
                t.MergeSubPlanParameters(subPlan);

                var srcAlias = t.EscapeAlias("__twsrc" + t._joinCounter++);
                var indexedAlias = t.EscapeAlias("__twidx" + t._joinCounter++);
                var outerAlias = t.EscapeAlias(takeWhile ? "__takewhile" + t._joinCounter++ : "__skipwhile" + t._joinCounter++);
                var indexAlias = t._provider.Escape("__norm_while_index");
                var flagAlias = t._provider.Escape("__norm_while_break");

                var predParam = predicate.Parameters[0];
                t._correlatedParams[predParam] = (subMapping, indexedAlias);
                if (predicate.Parameters.Count == 2)
                    t._paramMap[predicate.Parameters[1]] = $"{indexedAlias}.{indexAlias}";
                var predCtx = new VisitorContext(t._ctx, subMapping, t._provider, predParam, indexedAlias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                var predVisitor = FastExpressionVisitorPool.Get(in predCtx);
                var predSql = predVisitor.Translate(predicate.Body);
                foreach (var kvp in predVisitor.GetParameters())
                    t._params[kvp.Key] = kvp.Value;
                if (t._params.Count > t._parameterManager.Index)
                    t._parameterManager.Index = t._params.Count;
                FastExpressionVisitorPool.Return(predVisitor);
                if (predicate.Parameters.Count == 2)
                    t._paramMap.Remove(predicate.Parameters[1]);

                var orderByForSourceWindow = BuildWhileOrderBy(t, orderKeys, subMapping, srcAlias);
                var orderByForCumulativeWindow = BuildWhileOrderBy(t, orderKeys, subMapping, indexedAlias);
                var orderByForOuter = BuildWhileOrderBy(t, orderKeys, subMapping, outerAlias);
                var sourceSql = RemoveTrailingOrderByUnlessPaged(subPlan.Sql);

                t._sql.Append("SELECT * FROM (SELECT ").Append(indexedAlias).Append(".*, ")
                    .Append("SUM(CASE WHEN NOT (").Append(predSql).Append(") THEN 1 ELSE 0 END) OVER (ORDER BY ")
                    .Append(PooledStringBuilder.JoinOrderBy(orderByForCumulativeWindow)).Append(" ROWS UNBOUNDED PRECEDING) AS ").Append(flagAlias)
                    .Append(" FROM (SELECT ").Append(srcAlias).Append(".*, (ROW_NUMBER() OVER (ORDER BY ")
                    .Append(PooledStringBuilder.JoinOrderBy(orderByForSourceWindow)).Append(") - 1) AS ").Append(indexAlias)
                    .Append(" FROM (").Append(sourceSql).Append(") AS ").Append(srcAlias)
                    .Append(") AS ").Append(indexedAlias)
                    .Append(") AS ").Append(outerAlias)
                    .Append(" WHERE ").Append(outerAlias).Append('.').Append(flagAlias)
                    .Append(takeWhile ? " = 0" : " > 0");

                foreach (var (sql, asc) in orderByForOuter)
                    t._orderBy.Add((sql, asc));

                return node;
            }

            private static List<(string col, bool asc)> BuildWhileOrderBy(
                QueryTranslator t,
                IReadOnlyList<(LambdaExpression KeySelector, bool Ascending)> orderKeys,
                TableMapping mapping,
                string alias)
            {
                var result = new List<(string col, bool asc)>(orderKeys.Count);
                foreach (var (keySelector, ascending) in orderKeys)
                {
                    var param = keySelector.Parameters[0];
                    t._correlatedParams[param] = (mapping, alias);
                    var vctx = new VisitorContext(t._ctx, mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx);
                    var sql = visitor.Translate(keySelector.Body);
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                    var keyType = Nullable.GetUnderlyingType(keySelector.Body.Type) ?? keySelector.Body.Type;
                    if (keyType == typeof(decimal))
                        sql = t._provider.OrderByDecimalKeySql(sql);
                    result.Add((sql, ascending));
                }
                return result;
            }

        }

        /// <summary>
        /// Implements <c>TakeLast(source, n)</c>. SQL has no native "last N rows"
        /// operator, so we flip the ORDER BY direction of the source's existing
        /// ordering, apply LIMIT n on the reversed sequence (the DB scans only N
        /// rows), then mark the plan with <c>PostReverse=true</c> so the materializer
        /// reverses the small N-row result list to restore the original order.
        /// Requires an upstream OrderBy or a mapped key column the translator can
        /// default-order by; without either, the reversed direction is undefined.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class TakeLastTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => ApplyTailPaging(t, node, isTake: true);
        }

        /// <summary>
        /// Implements <c>SkipLast(source, n)</c>. Same flip-then-paginate pattern as
        /// TakeLast: flip ORDER BY direction, apply OFFSET n on the reversed sequence
        /// (the DB scans every row minus the last N), then reverse the result list
        /// so the caller sees rows in the original order. For "drop the last N",
        /// scanning all-minus-N rows is unavoidable in any provider.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SkipLastTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => ApplyTailPaging(t, node, isTake: false);
        }

        private static Expression ApplyTailPaging(QueryTranslator t, MethodCallExpression node, bool isTake)
        {
            // TakeLast/SkipLast after a Take/Skip window must count from the end of
            // the WINDOW. The flat flip-order shortcut below would emit a single
            // `ORDER BY … DESC LIMIT/OFFSET` that pages the FULL table, so wrap the
            // windowed source as a derived table (same machinery as Reverse-after-window)
            // with the source's order keys re-derived and FLIPPED on the outer SELECT,
            // then page the outer with the tail count. The window may sit below other
            // operators when the whole chain is wrap-eligible (entity-shaped rows,
            // extracted order keys stay truthful) — the sub-translation carries the
            // interleaved operators.
            if (node.Arguments[0] is MethodCallExpression tailWinSrc
                && (tailWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip)
                    || (SourceHasTakeOrSkip(node.Arguments[0]) && IsTailWrapEligibleSource(node.Arguments[0]))))
            {
                var subPlan = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMap);
                t._mapping = subMap;
                t.MergeSubPlanParameters(subPlan);
                var winAlias = t.EscapeAlias("__wtail" + t._joinCounter++);
                t._sql.AppendFragment("SELECT * FROM (").Append(subPlan.Sql).AppendFragment(") AS ").Append(winAlias);
                var orderKeys = ExtractOrderByKeys(node.Arguments[0]);
                t._orderBy.Clear();
                if (orderKeys.Count > 0)
                {
                    foreach (var (keyLambda, asc) in orderKeys)
                    {
                        var okParam = keyLambda.Parameters[0];
                        if (!t._correlatedParams.ContainsKey(okParam))
                            t._correlatedParams[okParam] = (subMap, winAlias);
                        var vctxOk = new VisitorContext(t._ctx, subMap, t._provider, okParam, winAlias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                        var okVisitor = FastExpressionVisitorPool.Get(in vctxOk);
                        var okSql = okVisitor.Translate(keyLambda.Body);
                        FastExpressionVisitorPool.Return(okVisitor);
                        // Same null-rank + TEXT-storage coercions the forward OrderBy
                        // applied (see ReverseTranslator.TranslateAfterTakeSkipWindow).
                        var okType = keyLambda.Body.Type;
                        if (t._provider.RequiresExplicitNullOrderingForNullableKeys
                            && (!okType.IsValueType || Nullable.GetUnderlyingType(okType) != null))
                            t._orderBy.Add(($"({okSql} IS NOT NULL)", !asc));
                        t._orderBy.Add((t.CoerceOrderKeySql(okSql, okType), !asc));
                    }
                }
                else
                {
                    foreach (var key in subMap.KeyColumns)
                        t._orderBy.Add(($"{winAlias}.{key.EscCol}", false));
                }
                BindTailCount(t, node, isTake);
                t._postReverseResult = true;
                return node;
            }

            // Walk the source first so any upstream OrderBy populates _orderBy.
            var source = t.Visit(node.Arguments[0]);

            // TakeLast/SkipLast in tail mode must count from the end of the assembled
            // sequence (an appended element IS the last one; a group is one element),
            // so the flip-order-and-page SQL shortcut must not run — evaluate in
            // memory instead. The Try method self-gates on tail mode and installs a
            // pending raw-GroupBy grouping transform first.
            if (t.TryAppendClientSequenceOperator(node))
                return source;

            // Reject DEEP paging composition (a Take/Skip buried below other operators):
            // the immediate-window shape wraps a derived table above, but re-deriving
            // order keys through interleaved operators is not part of the v1 contract.
            if (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null)
            {
                throw new NormUnsupportedFeatureException(
                    $"{node.Method.Name} composes with a DIRECTLY preceding Take/Skip window; " +
                    "with other operators between the window and the tail operator, apply " +
                    "TakeLast/SkipLast before any Take/Skip in the chain.");
            }

            // Ensure we have something to reverse. When a wrapping operator (e.g.
            // Where after a Take window) consumed the ordering into a subquery,
            // _orderBy is empty even though the source IS ordered — falling back to
            // key columns would silently tail-page the wrong sequence. Re-derive the
            // source chain's OrderBy keys from the expression first; only a truly
            // unordered source falls back to the entity's key columns (mirrors
            // ReverseTranslator).
            if (t._orderBy.Count == 0)
            {
                var deepKeys = ExtractOrderByKeys(node.Arguments[0]);
                if (deepKeys.Count > 0)
                {
                    // A wrapping operator records its derived-table alias; a flat
                    // query addresses the root table as T0.
                    var rootAlias = t._outerDerivedAlias ?? t.EscapeAlias("T0");
                    foreach (var (keyLambda, asc) in deepKeys)
                    {
                        var okParam = keyLambda.Parameters[0];
                        if (!t._correlatedParams.ContainsKey(okParam))
                            t._correlatedParams[okParam] = (t._mapping, rootAlias);
                        var vctxOk = new VisitorContext(t._ctx, t._mapping, t._provider, okParam, rootAlias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                        var okVisitor = FastExpressionVisitorPool.Get(in vctxOk);
                        var okSql = okVisitor.Translate(keyLambda.Body);
                        FastExpressionVisitorPool.Return(okVisitor);
                        var okType = keyLambda.Body.Type;
                        // Unflipped here; the flip loop below reverses rank and key
                        // together (the null rank is flip-safe by construction).
                        if (t._provider.RequiresExplicitNullOrderingForNullableKeys
                            && (!okType.IsValueType || Nullable.GetUnderlyingType(okType) != null))
                            t._orderBy.Add(($"({okSql} IS NOT NULL)", asc));
                        t._orderBy.Add((t.CoerceOrderKeySql(okSql, okType), asc));
                    }
                }
                else
                {
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, true));
                }
            }
            // Flip the ORDER BY direction so the SQL returns the LAST N rows first.
            for (int i = 0; i < t._orderBy.Count; i++)
            {
                var (col, asc) = t._orderBy[i];
                t._orderBy[i] = (col, !asc);
            }

            BindTailCount(t, node, isTake);

            // Tell the materializer to reverse the final list so the caller sees the
            // original ORDER BY direction. With LIMIT N this is a cheap in-memory
            // reverse of just N rows.
            t._postReverseResult = true;
            return source;
        }

        /// <summary>
        /// A tail operator can wrap its windowed source as a derived table only when
        /// every operator between the tail and the root keeps the rows entity-shaped
        /// and keeps the extracted order keys truthful: projections change the output
        /// columns the outer ORDER BY re-derives against, Reverse inverts the ordering
        /// the extracted keys describe, and joins/set-ops/reshapes change the row
        /// source entirely.
        /// </summary>
        private static bool IsTailWrapEligibleSource(Expression source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case nameof(Queryable.Take):
                    case nameof(Queryable.Skip):
                    case "TakeLast":
                    case "SkipLast":
                    case "TakeWhile":
                    case "SkipWhile":
                    case "Where":
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                    case "Distinct":
                    case "AsNoTrackingWithIdentityResolution":
                    case "AsNoTracking":
                    case "Query": // the `ctx.Query<T>()` chain root
                        break;
                    default:
                        return false;
                }
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
            return true;
        }

        /// <summary>
        /// Binds the TakeLast/SkipLast count onto the reversed sequence. Both
        /// literal-int and parameter-bound counts route through the same paging
        /// fields as the standard Take/Skip translators.
        /// </summary>
        private static void BindTailCount(QueryTranslator t, MethodCallExpression node, bool isTake)
        {
            if (t.TryBindPagingParameter(node.Arguments[1], out var paramName))
            {
                if (isTake) t._takeParam = paramName;
                else t._skipParam = paramName;
                t.ClampPagingParameterAtBind(paramName);
            }
            else if (TryGetIntValue(node.Arguments[1], out int countLit))
            {
                countLit = Math.Max(0, countLit);
                if (isTake) t._take = countLit;
                else t._skip = countLit;
            }
            else
            {
                throw new NormUnsupportedFeatureException(
                    $"{node.Method.Name} argument could not be bound to a parameter or literal.");
            }
        }

    }
}
