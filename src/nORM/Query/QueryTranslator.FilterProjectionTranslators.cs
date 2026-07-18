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

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class OrderByTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Windowed-source branch — wraps the source as a derived table and emits
            /// the new OrderBy against the wrap alias. Replaces a throw-pin with the
            /// LINQ-correct behaviour: outer OrderBy resorts only the windowed rows.
            /// Lives in its own method so the common-path Translate stays
            /// stack-frame lean (same reason as <see cref="WhereTranslator"/>).
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanO = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapO);
                t._mapping = subMapO;
                t.MergeSubPlanParameters(subPlanO);
                var winAliasO = t.EscapeAlias("__wob" + t._joinCounter++);
                t._sql.AppendFragment("SELECT * FROM (").Append(subPlanO.Sql).AppendFragment(") AS ").Append(winAliasO);
                t._outerDerivedAlias = winAliasO;
                // The inner ORDER BY is consumed by the derived table; the outer SELECT
                // must define its own ordering. For OrderBy/OrderByDescending this is
                // the new primary order; for ThenBy/ThenByDescending it appends to whatever
                // _orderBy already holds (which is empty here since we cleared the source's
                // order by wrapping). The outer key gets translated against the wrap alias.
                bool isTopLevel = node.Method.Name is nameof(Queryable.OrderBy)
                                                   or nameof(Queryable.OrderByDescending);
                if (isTopLevel) t._orderBy.Clear();
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression keySel)
                {
                    keySel = t.ExpandProjection(keySel);
                    var kp = keySel.Parameters[0];
                    t._correlatedParams[kp] = (subMapO, winAliasO);
                    var vctxO = new VisitorContext(t._ctx, subMapO, t._provider, kp, winAliasO, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctxO);
                    var keySql = visitor.Translate(keySel.Body);
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                    var ascending = !node.Method.Name.Contains("Descending");
                    t._orderBy.Add((keySql, ascending));
                }
                return node;
            }

            /// <summary>
            /// Translates <c>OrderBy</c> and related ordering methods into SQL <c>ORDER BY</c> clauses.
            /// </summary>
            /// <param name="t">The <see cref="QueryTranslator"/> orchestrating translation.</param>
            /// <param name="node">The method call expression for the ordering method.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // OrderBy after a Take/Skip-windowed source — wrap the windowed source
                // as a derived table so the new ordering applies to the LIMITed window,
                // not the full table. Sister of the WhereTranslator's windowed branch
                // (commit a1eb69e), extended to any Take/Skip in the source spine — the
                // helper sub-translates the whole source, so intervening operators
                // (Where, Select, AsNoTracking) ride inside the derived table. Only a
                // top-level OrderBy re-sorts; ThenBy composes with the existing ordering
                // and must not re-wrap. Post-materialize tails (reshapes, group joins,
                // raw GroupBy) are excluded — their ordering runs client-side over the
                // assembled rows via the tail-mode branch below.
                bool isTopLevelResort = node.Method.Name is nameof(Queryable.OrderBy)
                                                         or nameof(Queryable.OrderByDescending);
                if (isTopLevelResort
                    && QueryTranslator.SourceHasTakeOrSkip(node.Arguments[0])
                    && !SourceHasClientTailReshape(node.Arguments[0])
                    && !SourceHasGroupJoinResultTail(node.Arguments[0])
                    && !SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }

                // Detect OrderBy on the result of a set op (Union/Concat/Intersect/Except).
                // After the set op runs the outer SELECT shape is the unioned projection,
                // not the entity mapping -- so resolving `r.V` against `_mapping` throws
                // "Member 'V' is not supported in this context". The correct SQL just
                // references the projection's aliased column name, e.g.
                // `SELECT V FROM Left UNION SELECT V FROM Right ORDER BY V`.
                bool sourceIsSetOp = node.Arguments[0] is MethodCallExpression sourceMce
                    && sourceMce.Method.Name is "Union" or "Concat" or "Intersect" or "Except";
                var source = t.Visit(node.Arguments[0]);
                if (sourceIsSetOp
                    && QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression setOpKeySel
                    && setOpKeySel.Body is MemberExpression keyMember
                    && keyMember.Expression is ParameterExpression)
                {
                    var ascendingSet = !node.Method.Name.Contains("Descending");
                    t._orderBy.Add((t._provider.Escape(keyMember.Member.Name), ascendingSet));
                    return source;
                }
                // Detect OrderBy applied AFTER a Take/Skip — LINQ semantics
                // (`OrderBy(a).Take(n).OrderBy(b)`) require the outer OrderBy to resort
                // the limited window, which SQL can only express via a subquery wrap.
                // nORM's translator instead appends the outer OrderBy onto a flat
                // `_orderBy` list, emitting `ORDER BY a, b LIMIT n` — wrong rows, wrong
                // order, no exception. Surface a clear error with the documented
                // workarounds rather than letting the silent-wrongness through.
                // (ThenBy / ThenByDescending compose with the EXISTING ordering; they
                // share the OrderByTranslator dispatch but legitimately add to _orderBy
                // before Take applies — only the top-level OrderBy/OrderByDescending
                // after Take/Skip is wrong.)
                // Backstop: a windowed source normally routes through the derived-table
                // wrap above (any Take/Skip visible in the source spine). Reaching this
                // point means paging state exists without a syntactically detectable
                // window — appending the new ORDER BY onto the flat list would sort the
                // full table before the window applies, so fail closed.
                bool isTopLevelOrder = node.Method.Name is nameof(Queryable.OrderBy)
                                                        or nameof(Queryable.OrderByDescending);
                if (isTopLevelOrder && (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null) && !t._takeSetByTerminal)
                {
                    throw new NormUnsupportedFeatureException(
                        "OrderBy applied after a Take/Skip window that is not syntactically visible in the query spine " +
                        "would silently sort the full table before the window applies. Materialize the window first " +
                        "and resort client-side: " +
                        "`var top = await q.OrderByDescending(Score).Take(3).ToListAsync(); var sorted = top.OrderBy(x => x.Name).ToList();`");
                }
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression keySelector)
                {
                    // A raw streaming GroupBy defers its grouping transform to Generate();
                    // install it now so ordering over the IGroupings routes to the
                    // post-materialize order below instead of being silently dropped
                    // (the group key has no SQL column once the GROUP BY is discarded).
                    if (t._streamingGroupByKeySelector != null
                        && keySelector.Parameters[0].Type.IsGenericType
                        && keySelector.Parameters[0].Type.GetGenericTypeDefinition() == typeof(IGrouping<,>))
                    {
                        t.EnsurePendingStreamingGroupTransform();
                    }
                    if (t.IsPostMaterializeTailMode
                        && t.CurrentPostMaterializeElementType == keySelector.Parameters[0].Type)
                    {
                        var thenBy = node.Method.Name is nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending);
                        t.AppendPostMaterializeOrder(keySelector, !node.Method.Name.Contains("Descending"), thenBy);
                        return source;
                    }

                    if (keySelector.Parameters.Count == 1
                        && keySelector.Body is MemberExpression projectedMember
                        && projectedMember.Expression == keySelector.Parameters[0]
                        && t._projection != null
                        && keySelector.Parameters[0].Type == t._projection.Body.Type
                        && t.ProjectionContainsMember(projectedMember.Member.Name))
                    {
                        var ascendingAlias = !node.Method.Name.Contains("Descending");
                        // The alias references the projected expression verbatim, so the
                        // type-sensitive ordering coercions CoerceOrderKey applies to
                        // direct keys (decimal / TimeSpan / DateTimeOffset TEXT storage
                        // lex-sorts wrong) must wrap the alias too. The hooks are
                        // identity on providers with native types, so the standard
                        // bare-alias ORDER BY survives there; SQLite resolves aliases
                        // inside ORDER BY expressions.
                        var aliasSql = t._provider.Escape(projectedMember.Member.Name);
                        var aliasType = Nullable.GetUnderlyingType(projectedMember.Type) ?? projectedMember.Type;
                        if (aliasType == typeof(decimal)) aliasSql = t._provider.OrderByDecimalKeySql(aliasSql);
                        else if (aliasType == typeof(TimeSpan)) aliasSql = t._provider.NormalizeTimeSpanForCompare(aliasSql);
                        else if (aliasType == typeof(DateTimeOffset)) aliasSql = t._provider.NormalizeDateTimeOffsetForCompare(aliasSql);
                        t._orderBy.Add((aliasSql, ascendingAlias));
                        return source;
                    }

                    keySelector = t.ExpandProjection(keySelector);
                    var param = keySelector.Parameters[0];
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
                    var vctx = new VisitorContext(t._ctx, t._mapping, t._provider, param, info.Alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth, t._params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx);
                    // Mirror the WhereTranslator grouping setup so that
                    // `GroupBy(k).Select(g => new {Cat=g.Key, ...}).OrderBy(x => x.Cat)` works:
                    // ExpandProjection collapses `x.Cat` → bare key parameter `k`, and the
                    // visitor's VisitParameter (see e72ca37) emits the group-by SQL when the
                    // parameter is in _groupingKeys — otherwise it falls through to the base
                    // ParameterExpression handler which emits NOTHING and the ORDER BY clause
                    // ends up as `ORDER BY  ASC` (SQLite: `no such column: ASC`).
                    if (t._groupBy.Count > 0)
                    {
                        var groupBySql = PooledStringBuilder.Join(t._groupBy);
                        foreach (var p in keySelector.Parameters)
                            visitor.RegisterGroupingKey(p, groupBySql);
                    }
                    // Use node.Method.Name instead of t._methodName: visiting the source expression
                    // in t.Visit(node.Arguments[0]) above may have updated t._methodName to the last
                    // method encountered in the source chain (e.g., "Where"), losing the "Descending"
                    // marker.  node.Method.Name always refers to this OrderBy/OrderByDescending call.
                    var ascending = !node.Method.Name.Contains("Descending");
                    // GroupBy(...).Select(g => new { ..., Agg = g.Sum(...) }).OrderBy(x => x.Agg):
                    // ExpandProjection inlines `x.Agg` → `g.Sum(s => s.X)`. The generic SQL visitor
                    // doesn't know how to translate `g.Sum(...)`, so route the aggregate body
                    // through TranslateGroupAggregateMethod (the same path Select uses), reusing
                    // the alias HandleGroupBy assigned to the group element.
                    if (t._groupBy.Count > 0
                        && keySelector.Body is MethodCallExpression aggCall
                        && t.TranslateGroupAggregateMethod(aggCall, t.EscapeAlias("T" + t._joinCounter)) is { } aggSql)
                    {
                        t._orderBy.Add((aggSql, ascending));
                        FastExpressionVisitorPool.Return(visitor);
                        return source;
                    }
                    // Navigation-collection aggregate as the OrderBy key, after
                    // ExpandProjection inlines `r.Total` -> `p.Items.Sum(i => i.Amount)`.
                    // ExpressionToSqlVisitor doesn't recognise nav.Sum/Min/Max/Average in this
                    // position (only the Any/All/Count rewrite hits the predicate side), so a
                    // bare visitor.Translate would throw "Member 'Items' is not supported".
                    // SCV already has the full nav-aggregate emit path (commits efba58f / 5d1da71
                    // for Sum/Min/Max/Average; 0977c64 / 665e16d / c3c044b for Count/LongCount;
                    // EmitNavigationCountSubquery covers Any/All); use it directly with the
                    // outer alias the FROM clause will end up rendering.
                    if (keySelector.Body is MethodCallExpression navAggCall
                        && (navAggCall.Method.DeclaringType == typeof(Enumerable)
                            || navAggCall.Method.DeclaringType == typeof(Queryable))
                        && navAggCall.Method.Name is nameof(Enumerable.Sum) or nameof(Enumerable.Min)
                                                  or nameof(Enumerable.Max) or nameof(Enumerable.Average)
                                                  or nameof(Enumerable.Count) or nameof(Enumerable.LongCount)
                                                  or nameof(Enumerable.Any) or nameof(Enumerable.All)
                        && navAggCall.Arguments.Count >= 1
                        && QueryTranslator.UnwrapNavMember(navAggCall.Arguments[0]) is MemberExpression navMember
                        && navMember.Expression is ParameterExpression
                        && t._mapping.Relations.ContainsKey(navMember.Member.Name))
                    {
                        var scv = new SelectClauseVisitor(t._mapping, t._groupBy, t._provider, info.Alias, ctx: t._ctx) { SharedParams = t._params, SharedCompiledParams = t._compiledParams, SharedParamConverters = t._paramConverters, OuterRowParameters = keySelector.Parameters };
                        var navSql = scv.Translate(navAggCall);
                        t._orderBy.Add((navSql, ascending));
                        FastExpressionVisitorPool.Return(visitor);
                        return source;
                    }

                    // Composite anonymous-type key (e.g. `OrderBy(r => new { r.A, r.B })`) —
                    // emit one ORDER BY entry per member so the SQL becomes
                    // `ORDER BY "T0"."A", "T0"."B"` rather than the comma-joined single
                    // SELECT-list shape that the projection visitor would emit naturally
                    // and that SQL rejects inside ORDER BY.
                    // Decimal columns store as TEXT; SQLite ORDER BY does lex
                    // compare which mis-orders mixed-magnitude values (10.5 <
                    // 2 lex because '1' < '2'). Wrap decimal-typed keys with
                    // CAST AS REAL to force numeric ordering. Sister to the
                    // ETSV.VisitBinary CAST fix (8d795f4).
                    // Route via provider hook: SqliteProvider wraps decimal with
                    // CAST AS REAL, other providers keep identity (native DECIMAL).
                    // TimeSpan columns store as canonical 'c' TEXT; lex ORDER BY mis-sorts multi-day
                    // durations ('10.00:00:00' < '9.23:59:59' lexically, but 10 days > 9d23h). Wrap the
                    // key with NormalizeTimeSpanForCompare so the sort is numeric — the exact fix the
                    // WHERE path already applies in ETSV.VisitBinary. Providers with a native TIME/
                    // INTERVAL type return identity from the hook.
                    // DateTimeOffset stores as offset-suffixed TEXT on SQLite; lex ORDER BY sorts by
                    // wall-clock text, not UTC instant, so mixed-offset rows mis-sort. Route through
                    // NormalizeDateTimeOffsetForCompare (identity on providers whose native type
                    // already sorts by instant). Plain DateTime is deliberately NOT coerced: its
                    // fixed-width offset-free text already sorts chronologically, and wrapping it in
                    // datetime() would overflow on DateTime.MaxValue's .9999999 fraction.
                    string CoerceOrderKey(string sql, Type keyType) => t.CoerceOrderKeySql(sql, keyType);
                    // C# sorts null keys as SMALLEST (first ascending, last descending). PostgreSQL
                    // defaults to the opposite, so nullable keys there get a leading null-rank
                    // entry `(key IS NOT NULL)` with the same direction — false(null) < true, and
                    // because rank and key flip together the semantics survive Reverse().
                    void AddOrderKey(string keySql, Type keyType)
                    {
                        if (t._provider.RequiresExplicitNullOrderingForNullableKeys
                            && (!keyType.IsValueType || Nullable.GetUnderlyingType(keyType) != null))
                            t._orderBy.Add(($"({keySql} IS NOT NULL)", ascending));
                        t._orderBy.Add((CoerceOrderKey(keySql, keyType), ascending));
                    }
                    if (keySelector.Body is NewExpression newKey && newKey.Arguments.Count > 0)
                    {
                        foreach (var member in newKey.Arguments)
                        {
                            var memberSql = visitor.Translate(member);
                            AddOrderKey(memberSql, member.Type);
                        }
                    }
                    else
                    {
                        var sql = visitor.Translate(keySelector.Body);
                        AddOrderKey(sql, keySelector.Body.Type);
                    }
                    // Merge any parameters the visitor allocated (e.g. for COALESCE fallback
                    // constants in `OrderBy(r => r.Col ?? int.MaxValue)`) back into the outer
                    // translator. Without this the emitted SQL references @p0 but the command's
                    // parameter list never gets it bound — SQLite throws "Must add values for
                    // the following parameters" at execution time.
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    if (t._params.Count > t._parameterManager.Index)
                        t._parameterManager.Index = t._params.Count;
                    FastExpressionVisitorPool.Return(visitor);
                }
                return source;
            }
        }

    }
}
