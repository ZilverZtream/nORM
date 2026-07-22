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
        private sealed class CacheableTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Marks the query as cacheable and records an optional cache expiration before continuing translation.
            /// </summary>
            /// <param name="t">The active <see cref="QueryTranslator"/> instance.</param>
            /// <param name="node">The method call expression representing <c>Cacheable</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._isCacheable = true;
                if (QueryTranslator.TryGetConstantValue(node.Arguments[1], out var value) && value is TimeSpan ts)
                {
                    t._cacheExpiration = ts;
                }
                return t.Visit(node.Arguments[0]);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class WhereTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Windowed-source branch extracted into its own method so the common-path
            /// <see cref="Translate"/> stays stack-frame lean — matters under deeply
            /// nested Where chains where deep recursion can otherwise stack-overflow
            /// (see <c>GlobalFilterConcurrencyTests</c>).
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanW = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapW);
                t._mapping = subMapW;
                t.MergeSubPlanParameters(subPlanW);
                var winAliasW = t.EscapeAlias("__wht" + t._joinCounter++);
                t._sql.AppendFragment("SELECT * FROM (").Append(subPlanW.Sql).AppendFragment(") AS ").Append(winAliasW);
                t._outerDerivedAlias = winAliasW;
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression lambda)
                {
                    lambda = t.ExpandProjection(lambda);
                    var lp = lambda.Parameters[0];
                    t._correlatedParams[lp] = (subMapW, winAliasW);
                    var vctxW = new VisitorContext(t._ctx, subMapW, t._provider, lp, winAliasW, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctxW);
                    var predSql = visitor.Translate(lambda.Body);
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
            /// Translates a LINQ <c>Where</c> call into an equivalent SQL <c>WHERE</c> clause.
            /// </summary>
            /// <param name="t">The active <see cref="QueryTranslator"/>.</param>
            /// <param name="node">The method call expression for <c>Where</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Where after a Take/Skip-windowed source — wrap as derived table so the
                // predicate filters inside the window. Sister of the post-Take/Skip fixes
                // in 3040f49 / e0f1397 / 99a02ce, extended to any Take/Skip in the source
                // spine: the helper sub-translates the whole source, so intervening
                // operators ride inside the derived table and nested Where chains
                // terminate because each sub-translation strictly shrinks the spine.
                // Post-materialize tails (reshapes, group joins, raw GroupBy) are
                // excluded — their filtering runs client-side over assembled rows.
                // Routed to a helper so the common-path Translate stays stack-frame
                // lean (matters under deep global-filter chaining; see
                // GlobalFilterConcurrencyTests).
                if (QueryTranslator.SourceHasTakeOrSkip(node.Arguments[0])
                    && !SourceHasClientTailReshape(node.Arguments[0])
                    && !SourceHasGroupJoinResultTail(node.Arguments[0])
                    && !SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }

                // Where after a set-op (Union / Concat / Intersect / Except) needs a subquery
                // wrap so the predicate applies to the unified rows rather than just the
                // right side of the set op. Visit the set-op source (fills _sql with
                // `<left SQL> <SETOP> <right SQL>`), then wrap as `SELECT * FROM (...) AS T0`
                // and let the rest of Where translation emit the predicate against T0 — which
                // is the alias the predicate visitor naturally uses for the element parameter.
                bool wrappedSetOp = false;
                if (node.Arguments[0] is MethodCallExpression setOpSource
                    && setOpSource.Method.Name is "Union" or "Concat" or "Intersect" or "Except")
                {
                    t.Visit(node.Arguments[0]);
                    var innerSql = t._sql.ToString();
                    t._sql.Clear();
                    var wrapAlias = t.EscapeAlias("T0");
                    t._sql.AppendFragment("SELECT * FROM (").Append(innerSql).AppendFragment(") AS ").Append(wrapAlias);
                    // After the wrap, the joined columns live in T0; subsequent Where/OrderBy
                    // predicates need their parameters mapped to that alias.
                    wrappedSetOp = true;
                }

                var source = wrappedSetOp ? node.Arguments[0] : t.Visit(node.Arguments[0]);
                // Where after a client-tail reshape (Append/Prepend/Chunk/Zip/
                // DefaultIfEmpty(value)) must filter the reshaped sequence, not the server
                // rows — a SQL WHERE would let the reshaped element bypass the predicate.
                // This is also the path AnyAsync(pred)/AllAsync(pred) lower through.
                if (t._postMaterializeTransform != null)
                {
                    if (QueryTranslator.StripQuotes(node.Arguments[1]) is not LambdaExpression clientPredicate
                        || clientPredicate.Parameters.Count != 1)
                    {
                        throw new NormUnsupportedFeatureException(
                            "Where after a client-materialized sequence operator supports only a one-argument predicate.");
                    }
                    t.AppendClientTailFilter(clientPredicate);
                    return source;
                }
                // Backstop: a windowed source normally routes through the derived-table
                // wrap above (any Take/Skip visible in the source spine). Reaching this
                // point means paging state exists without a syntactically detectable
                // window — appending the WHERE onto the flat query would filter the full
                // table before the window applies, so fail closed.
                if ((t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null) && !t._takeSetByTerminal)
                {
                    throw new NormUnsupportedFeatureException(
                        "Where applied after a Take/Skip window that is not syntactically visible in the query spine " +
                        "would silently filter the full table before the window applies. Materialize the window first " +
                        "and filter client-side: " +
                        "`var top = await q.OrderBy(Id).Take(3).ToListAsync(); var filtered = top.Where(r => r.Active).ToList();`");
                }
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression lambda)
                {
                    lambda = t.ExpandProjection(lambda);
                    var param = lambda.Parameters[0];
                    // Any grouped source produces a HAVING clause, not a WHERE — covers both
                    // the direct shape `GroupBy(k).Where(g => g.Sum(...) > N)` AND the
                    // common shape with an intermediate projection,
                    // `GroupBy(k).Select(g => new { Key, Total=g.Sum(...) }).Where(x => x.Total > N)`.
                    // After ExpandProjection the latter's lambda parameter becomes the IGrouping
                    // `g`, so RegisterGroupingKey + the visitor's existing aggregate-on-grouping
                    // path emit `HAVING SUM(...) > @p0`. Detecting on `_groupBy.Count` instead of
                    // the source method's name is required because the source is `Select` here.
                    var isGrouping = t._groupBy.Count > 0;
                    if (!t._correlatedParams.TryGetValue(param, out var info))
                    {
                        // After a derived-table wrap, reuse the alias that TranslateAfterTakeSkipWindow
                        // set on _outerDerivedAlias — not a fresh T{counter} that doesn't exist in SQL.
                        var alias = t._outerDerivedAlias ?? t.EscapeAlias("T" + t._joinCounter);
                        info = (t._mapping, alias);
                        t._correlatedParams[param] = info;
                    }
                    // Record THIS translator's root alias so Build's FROM clause never has
                    // to reverse-lookup by mapping in the shared correlated dict (ambiguous
                    // when a nested subquery targets the same entity type as an outer scope).
                    t._selfRootAlias ??= info.Alias;
                    // paramIndexStart = t._params.Count so that this predicate's visitor
                    // does not reuse @p0/@p1 names already allocated by preceding predicates
                    // (e.g. inner Where's compiled param and global-filter constant colliding).
                    var vctx = new VisitorContext(t._ctx, t._mapping, t._provider, param, info.Alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx);
                    if (isGrouping)
                    {
                        // SelectTranslator rewrites `GroupBy(k).Select(g => proj)` into a 3-arg
                        // `GroupBy(k, (k, gs) => proj)`, so _projection (and the expanded lambda)
                        // can have TWO parameters: the key `k` and the group `gs`. The body's
                        // aggregate calls (`gs.Sum(...)`) reference `gs`, not `k` — register
                        // every parameter as a grouping key so the visitor's aggregate-detection
                        // path (line 1260) fires for whichever parameter the body actually uses.
                        var groupBySql = PooledStringBuilder.Join(t._groupBy);
                        foreach (var p in lambda.Parameters)
                            visitor.RegisterGroupingKey(p, groupBySql);
                    }
                    var sql = visitor.Translate(lambda.Body);
                    var target = isGrouping ? t._having : t._where;
                    if (target.Length > 0) target.Append(" AND ");
                    target.Append($"({sql})");
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    // Sync ParameterManager.Index with _params.Count so that subsequent
                    // translators (Take, Skip) don't reuse parameter names already allocated
                    // by the visitor (e.g. both WHERE and LIMIT producing @p0).
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                }
                return source;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SelectTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Handles projection of elements by capturing the provided selector expression.
            /// Automatically detects untranslatable expressions and splits them into
            /// server-side (SQL) and client-side (in-memory) projections.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>Select</c>.</param>
            /// <returns>The translated source expression.</returns>
            /// <remarks>
            /// Automatic client-side evaluation fallback for untranslatable projections.
            ///
            /// Process:
            /// 1. Analyze projection for untranslatable expressions (e.g., custom helper methods)
            /// 2. Extract required member accesses from those expressions
            /// 3. Rewrite SQL projection to select only raw columns needed
            /// 4. Compile original projection expression into ClientProjection delegate
            /// 5. Store delegate for post-materialization execution in QueryExecutor
            ///
            /// Example:
            /// - Original: ctx.Users.Select(u => Helper.FormatName(u.FirstName, u.LastName))
            /// - SQL: SELECT FirstName, LastName FROM Users
            /// - Client: rows.Select(row => Helper.FormatName(row.FirstName, row.LastName))
            ///
            /// Benefits:
            /// - No more "method not supported" exceptions for simple projections
            /// - Seamless fallback maintains expected LINQ behavior
            /// - Only fetches columns actually needed, minimizing data transfer
            /// </remarks>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var originalProjection = QueryTranslator.StripQuotes(node.Arguments[1]) as LambdaExpression;

                // Collapse `Select(Select(GroupBy(s,k), g => new {...}), x => outerBody)` by composing the
                // OUTER projection through the inner GroupBy projection, so the combined
                // `g => outerBody[with inner members inlined]` dispatches through the single GroupBy+Select
                // path below. Without this the second Select over the grouped {Key, N} result was silently
                // dropped (it returned just the inner projection's first column).
                if (originalProjection != null
                    && originalProjection.Parameters.Count == 1
                    && node.Arguments[0] is MethodCallExpression innerSel
                    && innerSel.Method.Name == nameof(Queryable.Select)
                    && innerSel.Arguments.Count == 2
                    && QueryTranslator.StripQuotes(innerSel.Arguments[1]) is LambdaExpression innerProj
                    && innerProj.Body is NewExpression
                    && innerProj.Parameters.Count == 1
                    && innerProj.Parameters[0].Type.IsGenericType
                    && innerProj.Parameters[0].Type.GetGenericTypeDefinition() == typeof(IGrouping<,>)
                    && innerSel.Arguments[0] is MethodCallExpression innerGb
                    && innerGb.Method.Name == nameof(Queryable.GroupBy)
                    && originalProjection.Parameters[0].Type == innerProj.Body.Type)
                {
                    var composedBody = new nORM.Internal.ParameterReplacer(originalProjection.Parameters[0], innerProj.Body).Visit(originalProjection.Body)!;
                    composedBody = new ProjectionMemberReplacer().Visit(composedBody)!;
                    var composedLambda = Expression.Lambda(composedBody, innerProj.Parameters[0]);
                    var groupingType = innerProj.Parameters[0].Type;
                    var selectMethod = typeof(Queryable).GetMethods()
                        .First(m => m.Name == nameof(Queryable.Select)
                                    && m.IsGenericMethodDefinition
                                    && m.GetParameters().Length == 2
                                    && m.GetParameters()[1].ParameterType.IsGenericType
                                    && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 2)
                        .MakeGenericMethod(groupingType, composedBody.Type);
                    var collapsed = Expression.Call(selectMethod, innerSel.Arguments[0], Expression.Quote(composedLambda));
                    return t.Visit(collapsed);
                }

                // Same collapse, but with an INTERMEDIATE OrderBy/ThenBy chain between the two Selects:
                // `Select(OrderBy(Select(GroupBy, g => new {...}), x => x.Key), x => outerBody)`. The chain
                // orders the grouped {Key, N} projection; without this the outer Select over the ordering
                // is silently dropped (it returned just the inner projection's first column). Compose the
                // outer projection AND each order key through the inner GroupBy projection, then rebuild as
                // `Select(OrderBy(GroupBy, g => key'), g => outerBody')` — the single-GroupBy ordered-then-
                // computed shape the paths below already handle.
                if (originalProjection != null
                    && originalProjection.Parameters.Count == 1
                    && node.Arguments[0] is MethodCallExpression orderedGroupSel
                    && IsQueryableOrdering(orderedGroupSel)
                    && TryUnwrapOrderedGroupSelect(orderedGroupSel, out var groupOrderings, out var groupInnerProj, out var groupInnerGb)
                    && originalProjection.Parameters[0].Type == groupInnerProj.Body.Type)
                {
                    var grpParam = groupInnerProj.Parameters[0];
                    Expression rebuilt = groupInnerGb;
                    // Orderings are collected inner-first; re-apply each with its key composed through the
                    // inner projection (so x.Key -> g.Key, x.N -> g.Count() against the grouping param).
                    foreach (var (orderMethod, orderKey) in groupOrderings)
                    {
                        var composedKeyBody = new ProjectionMemberReplacer().Visit(
                            new nORM.Internal.ParameterReplacer(orderKey.Parameters[0], groupInnerProj.Body).Visit(orderKey.Body)!)!;
                        var composedKey = Expression.Lambda(composedKeyBody, grpParam);
                        rebuilt = BuildQueryableOrdering(orderMethod, rebuilt, composedKey, grpParam.Type, composedKeyBody.Type);
                    }
                    var composedOuterBody = new ProjectionMemberReplacer().Visit(
                        new nORM.Internal.ParameterReplacer(originalProjection.Parameters[0], groupInnerProj.Body).Visit(originalProjection.Body)!)!;
                    rebuilt = BuildGroupingSelect(rebuilt, Expression.Lambda(composedOuterBody, grpParam), grpParam.Type, composedOuterBody.Type);
                    return t.Visit(rebuilt);
                }

                // Rewrite `Select(GroupBy(s, k), g => proj)` (2-arg GroupBy + Select-over-IGrouping)
                // as `GroupBy(s, k, (key, group) => proj')` so the existing 3-arg HandleGroupBy
                // path can build the SELECT — otherwise the projection sees an IGrouping parameter
                // with no table mapping and leaks ArgumentNullException out of the visitor.
                if (originalProjection != null
                    && node.Arguments[0] is MethodCallExpression gb
                    && gb.Method.Name == nameof(Queryable.GroupBy)
                    && gb.Arguments.Count is 2 or 3
                    && originalProjection.Parameters.Count == 1
                    && originalProjection.Parameters[0].Type.IsGenericType
                    && originalProjection.Parameters[0].Type.GetGenericTypeDefinition() == typeof(IGrouping<,>))
                {
                    if (t.IsPostMaterializeTailMode)
                    {
                        var groupedSource = t.Visit(node.Arguments[0]);
                        if (t.IsPostMaterializeTailMode
                            && t.CurrentPostMaterializeElementType == originalProjection.Parameters[0].Type)
                        {
                            t.AppendPostMaterializeSelect(originalProjection);
                            return groupedSource;
                        }
                    }

                    var rewritten = RewriteGroupByThenSelect(gb, originalProjection);
                    return t.Visit(rewritten);
                }

                LambdaExpression? pendingProjection = null;
                Func<object, object>? pendingClientProjection = null;
                Type? pendingClientProjectionResultType = null;
                var shouldLogClientEvaluation = false;
                if (originalProjection != null)
                {
                    // Try to split the projection into server and client parts
                    if (t.TrySplitProjection(originalProjection, out var serverProjection, out var clientProjection, out var allAutoRouteSafe))
                    {
                        // Auto-route-safe leaves (string.Split, ToCharArray, etc.) bypass
                        // the Throw policy -- those are pure LINQ-to-Objects shapes that
                        // can transparently run client-side after raw-column fetch with
                        // no correctness / cost surprises. Complex untranslatables
                        // (correlated subqueries, helper-method calls, lambda invocations)
                        // still surface the full Throw with diagnostic.
                        if (t._ctx.Options.ClientEvaluationPolicy == ClientEvaluationPolicy.Throw
                            && !allAutoRouteSafe)
                        {
                            throw new NormUnsupportedFeatureException(
                                "The query projection requires client-side evaluation — it contains " +
                                "an expression nORM can't translate to SQL. Common culprits: " +
                                "(a) a list-valued or non-aggregate subquery via `ctx.Query<X>()...` in the " +
                                "projection — nORM translates `ctx.Query<X>().Count/Sum/Min/Max/Average(...)` " +
                                "chains built from Where/Select/OrderBy/Distinct as correlated scalar " +
                                "subqueries, but subqueries materializing rows (ToList/First/element access) " +
                                "or using other operators inside the subquery source must be restructured " +
                                "(navigation Include + in-memory shaping, or a join-shaped query); " +
                                "(b) a navigation chain using an unsupported multi-hop operator shape; nORM emits " +
                                "single-hop navigation aggregates and two-hop Count/LongCount/Any over " +
                                "`p.Children.SelectMany(c => c.GrandChildren)`, but deeper or different operators " +
                                "may need `Include(...).ThenInclude(...)` plus an in-memory aggregate, or an explicit " +
                                "join-shaped SQL query; " +
                                "(c) a custom helper method or LINQ-to-Objects construct unreachable in SQL — " +
                                "rewrite using SQL-translatable primitives. If you really need client-side " +
                                "evaluation of the projection, set " +
                                "`DbContextOptions.ClientEvaluationPolicy = ClientEvaluationPolicy.Warn` or " +
                                "`.Allow`. Warn logs each occurrence; Allow runs silently.");
                        }

                        pendingProjection = serverProjection;
                        pendingClientProjection = clientProjection;
                        pendingClientProjectionResultType = originalProjection.Body.Type;
                        shouldLogClientEvaluation = t._ctx.Options.ClientEvaluationPolicy == ClientEvaluationPolicy.Warn;
                        // The client part compiles NOW with its closures captured, so a
                        // cached plan would replay this execution's closure values (and
                        // any folded server-side values) on every later hit. Mark the
                        // plan fold-no-cache when the projection captures closures —
                        // it re-translates per execution with live values.
                        if (clientProjection != null && nORM.Internal.ExpressionCompiler.HasClosureValues(originalProjection.Body))
                            t._closureFoldedIntoSql = true;
                    }
                    else
                    {
                        // Use original projection (fully translatable to SQL)
                        pendingProjection = originalProjection;
                    }
                }

                var source = t.Visit(node.Arguments[0]);
                if (originalProjection != null
                    && IsGroupingProjection(originalProjection)
                    && t._groupBy.Count > 0
                    && !t.IsPostMaterializeTailMode)
                {
                    var groupProjection = t.ExpandProjection(originalProjection);
                    t._projection = groupProjection;
                    t._clientProjection = pendingClientProjection;
                    t._clientProjectionResultType = pendingClientProjectionResultType;
                    if (shouldLogClientEvaluation)
                    {
                        t._ctx.Options.Logger?.LogQuery(
                            "-- CLIENT-EVAL: Projection split for client-side evaluation",
                            EmptyParamDict,
                            TimeSpan.Zero,
                            0);
                    }

                    var sourceFromSql = QueryTranslator.ExtractSourceFromClause(t._sql.ToString());
                    var groupBySql = PooledStringBuilder.Join(t._groupBy);
                    var alias = t._outerDerivedAlias ?? t.EscapeAlias("T" + t._joinCounter);
                    t._sql.Clear();
                    t._streamingGroupByKeySelector = null;
                    t.BuildGroupBySelectClause(groupProjection, groupBySql, alias, sourceFromSql);
                    return source;
                }

                if (originalProjection != null
                    && t.IsPostMaterializeTailMode
                    && t.CurrentPostMaterializeElementType == originalProjection.Parameters[0].Type)
                {
                    t.AppendPostMaterializeSelect(originalProjection);
                    return source;
                }

                if (pendingProjection != null)
                {
                    pendingProjection = t.ExpandProjection(pendingProjection);

                    // A reshaping projection after a set operation (Union/Concat/Intersect/Except) must bind
                    // to the unified compound, not be dropped. The set-op fills _sql with the bare
                    // `<left> <SETOP> <right>`; Build() then skips SELECT generation, so the projection reaches
                    // only the materializer, which name-matches against the arms' raw columns and silently
                    // returns them unprojected. Wrap the compound as a derived table and record its alias so
                    // the rewrite below emits the projected SELECT list against it (mirror of the Where wrap).
                    if (t._outerDerivedAlias == null
                        && t._sql.Length > 0
                        && IsSetOperationCall(node.Arguments[0]))
                    {
                        var innerSetSql = t._sql.ToString();
                        if (innerSetSql.StartsWith("SELECT ", StringComparison.Ordinal))
                        {
                            t._sql.Clear();
                            var setWrapAlias = t.EscapeAlias("__sset" + t._joinCounter++);
                            t._sql.AppendFragment("SELECT * FROM (").Append(innerSetSql)
                                  .AppendFragment(") AS ").Append(setWrapAlias);
                            t._outerDerivedAlias = setWrapAlias;
                            // The compound preserves the arms' element (row) columns; resolve the projection
                            // against that row mapping, not the projected scalar type the outer query carries.
                            var rowType = pendingProjection.Parameters[0].Type;
                            if (t._mapping.Type != rowType && !rowType.IsValueType && rowType != typeof(string))
                                t._mapping = t._ctx.GetMapping(rowType);
                        }
                    }

                    // A windowed derived-table wrap pre-fills _sql as
                    // `SELECT * FROM (...) AS alias`, and Build() skips SELECT generation
                    // for pre-filled SQL — the projection would never reach the statement
                    // and the materializer would name-match computed members against the
                    // wrap's raw columns, silently binding raw values. Rewrite the wrap's
                    // SELECT list from the projection against the wrap alias.
                    if (t._outerDerivedAlias != null && t._sql.Length > 0)
                    {
                        var wrappedSql = t._sql.ToString();
                        if (wrappedSql.StartsWith("SELECT * FROM (", StringComparison.Ordinal))
                        {
                            var selectVisitor = new SelectClauseVisitor(t._mapping, t._groupBy, t._provider, t._outerDerivedAlias, ctx: t._ctx)
                            {
                                ExactDecimalProjectionKeys = t._exactDecimalProjectionKeys,
                                ForceOrdinalStringProjections = t._forceOrdinalStringProjections,
                                SharedParams = t._params,
                                SharedCompiledParams = t._compiledParams,
                                SharedParamConverters = t._paramConverters,
                                OuterRowParameters = pendingProjection.Parameters
                            };
                            var projSelect = selectVisitor.Translate(pendingProjection.Body);
                            t._detectedCollections.AddRange(selectVisitor.DetectedCollections);
                            foreach (var kvp in selectVisitor.DetectedCollectionFilters)
                                t._detectedCollectionFilters[kvp.Key] = kvp.Value;
                            foreach (var kvp in selectVisitor.DetectedCollectionProjections)
                                t._detectedCollectionProjections[kvp.Key] = kvp.Value;
                            foreach (var kvp in selectVisitor.DetectedCollectionTargetMembers)
                                t._detectedCollectionTargetMembers[kvp.Key] = kvp.Value;
                            foreach (var kvp in selectVisitor.DetectedCollectionOrderings)
                                t._detectedCollectionOrderings[kvp.Key] = kvp.Value;
                            t._sql.Clear();
                            t._sql.Append("SELECT ").Append(projSelect).Append(wrappedSql.Substring("SELECT *".Length));
                        }
                    }

                    // Apply the projection after translating the source so source-side
                    // Where/OrderBy/Take/Skip lambdas are not expanded through the
                    // projected row shape. Downstream operators that run after Select
                    // still see _projection when their translators resume.
                    t._projection = pendingProjection;
                    t._clientProjection = pendingClientProjection;
                    t._clientProjectionResultType = pendingClientProjectionResultType;
                    if (shouldLogClientEvaluation)
                    {
                        t._ctx.Options.Logger?.LogQuery(
                            "-- CLIENT-EVAL: Projection split for client-side evaluation",
                            EmptyParamDict,
                            TimeSpan.Zero,
                            0);
                    }
                }

                return source;
            }

            private static bool IsGroupingProjection(LambdaExpression projection)
                => projection.Parameters.Count == 1
                   && projection.Parameters[0].Type.IsGenericType
                   && projection.Parameters[0].Type.GetGenericTypeDefinition() == typeof(IGrouping<,>);

            private static MethodCallExpression RewriteGroupByThenSelect(
                MethodCallExpression groupByCall,
                LambdaExpression selectLambda)
            {
                var groupingType = selectLambda.Parameters[0].Type;
                var keyType = groupingType.GetGenericArguments()[0];
                var elementType = groupingType.GetGenericArguments()[1];
                var sourceType = QueryTranslator.GetElementType(groupByCall.Arguments[0]);
                var resultType = selectLambda.Body.Type;

                var keyParam = Expression.Parameter(keyType, "k");
                var groupParam = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(elementType), "g");

                var rewriter = new GroupingProjectionRewriter(selectLambda.Parameters[0], keyParam, groupParam);
                var newBody = rewriter.Visit(selectLambda.Body)!;
                var resultSelector = Expression.Lambda(newBody, keyParam, groupParam);

                if (groupByCall.Arguments.Count == 3)
                {
                    var groupByMethodWithElement = typeof(Queryable).GetMethods()
                        .First(m => m.Name == nameof(Queryable.GroupBy)
                                    && m.IsGenericMethodDefinition
                                    && m.GetGenericArguments().Length == 4
                                    && m.GetParameters().Length == 4
                                    && m.GetParameters()[3].ParameterType.IsGenericType
                                    && m.GetParameters()[3].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 3)
                        .MakeGenericMethod(sourceType, keyType, elementType, resultType);

                    return Expression.Call(groupByMethodWithElement,
                        groupByCall.Arguments[0],
                        groupByCall.Arguments[1],
                        groupByCall.Arguments[2],
                        Expression.Quote(resultSelector));
                }

                // GroupBy<TSource, TKey, TResult>(source, keySelector, resultSelector) — disambiguate
                // from the per-element overload by looking for a 2-arg inner Func on the result selector.
                var groupByMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == nameof(Queryable.GroupBy)
                                && m.IsGenericMethodDefinition
                                && m.GetGenericArguments().Length == 3
                                && m.GetParameters().Length == 3
                                && m.GetParameters()[2].ParameterType.IsGenericType
                                && m.GetParameters()[2].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 3)
                    .MakeGenericMethod(sourceType, keyType, resultType);

                return Expression.Call(groupByMethod,
                    groupByCall.Arguments[0],
                    groupByCall.Arguments[1],
                    Expression.Quote(resultSelector));
            }

            private static bool IsQueryableOrdering(Expression e)
                => e is MethodCallExpression m
                   && m.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                                    or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending);

            /// <summary>
            /// Walks an OrderBy/ThenBy chain down to its <c>Select(GroupBy(...), g =&gt; new {...})</c>
            /// source. On success returns the orderings INNER-FIRST (ready to re-apply onto the GroupBy),
            /// the inner IGrouping projection lambda, and the GroupBy call. Any non-conforming link fails.
            /// </summary>
            [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
            [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
            private static bool TryUnwrapOrderedGroupSelect(
                MethodCallExpression orderChain,
                out List<(string Method, LambdaExpression Key)> orderings,
                out LambdaExpression innerProjection,
                out MethodCallExpression groupByCall)
            {
                orderings = new List<(string, LambdaExpression)>();
                innerProjection = null!;
                groupByCall = null!;

                Expression current = orderChain;
                while (current is MethodCallExpression m && IsQueryableOrdering(m))
                {
                    if (m.Arguments.Count != 2
                        || QueryTranslator.StripQuotes(m.Arguments[1]) is not LambdaExpression keyLambda)
                        return false;
                    orderings.Add((m.Method.Name, keyLambda));
                    current = m.Arguments[0];
                }

                if (current is not MethodCallExpression sel
                    || sel.Method.Name != nameof(Queryable.Select)
                    || sel.Arguments.Count != 2
                    || QueryTranslator.StripQuotes(sel.Arguments[1]) is not LambdaExpression proj
                    || proj.Body is not NewExpression
                    || proj.Parameters.Count != 1
                    || !proj.Parameters[0].Type.IsGenericType
                    || proj.Parameters[0].Type.GetGenericTypeDefinition() != typeof(IGrouping<,>)
                    || sel.Arguments[0] is not MethodCallExpression gb
                    || gb.Method.Name != nameof(Queryable.GroupBy))
                    return false;

                innerProjection = proj;
                groupByCall = gb;
                // Collected outer-first while walking down; reverse so OrderBy precedes its ThenBy.
                orderings.Reverse();
                return true;
            }

            [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
            [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
            private static MethodCallExpression BuildQueryableOrdering(
                string methodName, Expression source, LambdaExpression keyLambda, Type sourceType, Type keyType)
            {
                var method = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
                    .MakeGenericMethod(sourceType, keyType);
                return Expression.Call(method, source, Expression.Quote(keyLambda));
            }

            [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
            [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
            private static MethodCallExpression BuildGroupingSelect(
                Expression source, LambdaExpression lambda, Type groupingType, Type resultType)
            {
                var selectMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == nameof(Queryable.Select)
                                && m.IsGenericMethodDefinition
                                && m.GetParameters().Length == 2
                                && m.GetParameters()[1].ParameterType.IsGenericType
                                && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 2)
                    .MakeGenericMethod(groupingType, resultType);
                return Expression.Call(selectMethod, source, Expression.Quote(lambda));
            }

            [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
            [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
            private sealed class GroupingProjectionRewriter : ExpressionVisitor
            {
                private readonly ParameterExpression _oldGrouping;
                private readonly ParameterExpression _newKey;
                private readonly ParameterExpression _newGroup;

                public GroupingProjectionRewriter(ParameterExpression oldGrouping, ParameterExpression newKey, ParameterExpression newGroup)
                {
                    _oldGrouping = oldGrouping;
                    _newKey = newKey;
                    _newGroup = newGroup;
                }

                protected override Expression VisitMember(MemberExpression node)
                {
                    if (node.Expression == _oldGrouping && node.Member.Name == "Key")
                        return _newKey;
                    return base.VisitMember(node);
                }

                protected override Expression VisitMethodCall(MethodCallExpression node)
                {
                    if (node.Arguments.Count > 0
                        && node.Arguments[0] == _oldGrouping
                        && node.Method.DeclaringType == typeof(Enumerable))
                    {
                        var args = new Expression[node.Arguments.Count];
                        args[0] = _newGroup;
                        for (int i = 1; i < node.Arguments.Count; i++)
                            args[i] = Visit(node.Arguments[i]);
                        return node.Update(node.Object, args);
                    }

                    return base.VisitMethodCall(node);
                }

                protected override Expression VisitParameter(ParameterExpression node)
                    => node == _oldGrouping ? Expression.Constant(null, _oldGrouping.Type) : base.VisitParameter(node);
            }
        }

        /// <summary>
        /// Type-sensitive ordering-key normalization shared by every ORDER-BY-emitting
        /// site (OrderBy/ThenBy, MinBy/MaxBy, windowed Reverse). Decimal, TimeSpan,
        /// and DateTimeOffset store as TEXT on SQLite and mis-sort lexically; the
        /// provider hooks coerce to numeric/instant ordering (identity on providers
        /// with native types). Plain DateTime is deliberately not coerced — its
        /// fixed-width offset-free text sorts chronologically, and wrapping it in
        /// datetime() would overflow on DateTime.MaxValue's .9999999 fraction.
        /// </summary>
        private string CoerceOrderKeySql(string sql, Type keyType)
        {
            var u = Nullable.GetUnderlyingType(keyType) ?? keyType;
            if (u == typeof(decimal)) return _provider.OrderByDecimalKeySql(sql);
            if (u == typeof(TimeSpan)) return _provider.NormalizeTimeSpanForCompare(sql);
            if (u == typeof(DateTimeOffset)) return _provider.NormalizeDateTimeOffsetForCompare(sql);
            return sql;
        }


    }
}
