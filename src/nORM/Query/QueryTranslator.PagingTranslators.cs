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
                    if (clientTake < 0) throw new ArgumentOutOfRangeException(nameof(clientTake), clientTake, "Take count must be non-negative.");
                    t.AppendPostMaterializeTake(clientTake);
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
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int take))
                {
                    if (take < 0) throw new ArgumentOutOfRangeException(nameof(take), take, "Take count must be non-negative.");
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
                    if (skipForLiteral < 0)
                        throw new ArgumentOutOfRangeException(nameof(skipForLiteral), skipForLiteral, "Skip count must be non-negative.");
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
                    }
                    else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int skipLit))
                    {
                        if (skipLit < 0) throw new ArgumentOutOfRangeException(nameof(skipLit), skipLit, "Skip count must be non-negative.");
                        skipExpr = skipLit.ToString();
                    }
                    else
                    {
                        throw new NormUnsupportedFeatureException(
                            "Skip argument could not be bound to a parameter or literal.");
                    }
                    // Reset the existing take fields and emit (take - skip) as the new limit
                    // expression; pin the offset. Negative results clip to 0 via GREATEST/IIF
                    // on providers that support it — fall back to a portable MAX of (expr, 0)
                    // via the provider's LIMIT engine which generally clamps to 0 anyway.
                    t._take = null;
                    t._takeParam = $"({existingTakeExpr} - {skipExpr})";
                    t._skip = null;
                    t._skipParam = skipExpr;
                    return source;
                }
                if (t.TryBindPagingParameter(node.Arguments[1], out var sName))
                {
                    t._skipParam = sName;
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int skip))
                {
                    if (skip < 0) throw new ArgumentOutOfRangeException(nameof(skip), skip, "Skip count must be non-negative.");
                    // Inline literal Skip values directly in SQL (same rationale as Take).
                    t._skip = skip;
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
                if (SourceHasTakeOrSkip(node.Arguments[0]))
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} after Take/Skip is not part of the v1 provider-mobile contract.");
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
                var sourceSql = RemoveTrailingOrderBy(subPlan.Sql);

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
                        sql = t._provider.NormalizeDecimalForCompare(sql);
                    result.Add((sql, ascending));
                }
                return result;
            }

            private static string RemoveTrailingOrderBy(string sql)
            {
                var idx = sql.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
                return idx < 0 ? sql : sql[..idx];
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
            // Walk the source first so any upstream OrderBy populates _orderBy.
            var source = t.Visit(node.Arguments[0]);

            // TakeLast/SkipLast in tail mode must count from the end of the assembled
            // sequence (an appended element IS the last one; a group is one element),
            // so the flip-order-and-page SQL shortcut must not run — evaluate in
            // memory instead. The Try method self-gates on tail mode and installs a
            // pending raw-GroupBy grouping transform first.
            if (t.TryAppendClientSequenceOperator(node))
                return source;

            // Reject existing _take/_skip composition for now -- combining tail-paging
            // with start-paging requires double-subquery emit that nORM doesn't yet do.
            if (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null)
            {
                throw new NormUnsupportedFeatureException(
                    $"{node.Method.Name} cannot compose with an upstream Take/Skip in v1. " +
                    "Apply TakeLast/SkipLast before any Take/Skip in the chain.");
            }

            // Ensure we have something to reverse -- fall back to the entity's key
            // columns if no explicit OrderBy was applied (mirrors ReverseTranslator).
            if (t._orderBy.Count == 0)
            {
                foreach (var key in t._mapping.KeyColumns)
                    t._orderBy.Add((key.EscCol, true));
            }
            // Flip the ORDER BY direction so the SQL returns the LAST N rows first.
            for (int i = 0; i < t._orderBy.Count; i++)
            {
                var (col, asc) = t._orderBy[i];
                t._orderBy[i] = (col, !asc);
            }

            // Apply Take or Skip on the reversed sequence. Both literal-int and
            // parameter-bound counts route through the same paging fields as the
            // standard Take/Skip translators.
            if (t.TryBindPagingParameter(node.Arguments[1], out var paramName))
            {
                if (isTake) t._takeParam = paramName;
                else t._skipParam = paramName;
            }
            else if (TryGetIntValue(node.Arguments[1], out int countLit))
            {
                if (countLit < 0)
                    throw new ArgumentOutOfRangeException(
                        node.Method.Name,
                        countLit,
                        $"{node.Method.Name} count must be non-negative.");
                if (isTake) t._take = countLit;
                else t._skip = countLit;
            }
            else
            {
                throw new NormUnsupportedFeatureException(
                    $"{node.Method.Name} argument could not be bound to a parameter or literal.");
            }

            // Tell the materializer to reverse the final list so the caller sees the
            // original ORDER BY direction. With LIMIT N this is a cheap in-memory
            // reverse of just N rows.
            t._postReverseResult = true;
            return source;
        }

    }
}
