using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Collections.Generic;
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
        private static readonly Dictionary<string, IMethodCallTranslator> _methodTranslators = new()
        {
            { "Cacheable", new CacheableTranslator() },
            { "Where", new WhereTranslator() },
            { "Select", new SelectTranslator() },
            { "OrderBy", new OrderByTranslator() },
            { "OrderByDescending", new OrderByTranslator() },
            { "ThenBy", new OrderByTranslator() },
            { "ThenByDescending", new OrderByTranslator() },
            { "Take", new TakeTranslator() },
            { "Skip", new SkipTranslator() },
            { "TakeLast", new TakeLastTranslator() },
            { "SkipLast", new SkipLastTranslator() },
            { "Join", new JoinTranslator(false) },
            { "GroupJoin", new JoinTranslator(true) },
            { "SelectMany", new SelectManyTranslator() },
            { "Distinct", new DistinctTranslator() },
            { "DistinctBy", new DistinctByTranslator() },
            { "ExceptBy", new ExceptByTranslator() },
            { "IntersectBy", new IntersectByTranslator() },
            { "UnionBy", new UnionByTranslator() },
            { "Reverse", new ReverseTranslator() },
            { "Union", new SetOperationTranslator() },
            { "Concat", new SetOperationTranslator() },
            { "Intersect", new SetOperationTranslator() },
            { "Except", new SetOperationTranslator() },
            { "Any", new SetPredicateTranslator() },
            { "Contains", new SetPredicateTranslator() },
            { "ElementAt", new ElementAtTranslator() },
            { "ElementAtOrDefault", new ElementAtTranslator() },
            { "First", new FirstSingleTranslator() },
            { "FirstOrDefault", new FirstSingleTranslator() },
            { "Single", new FirstSingleTranslator() },
            { "SingleOrDefault", new FirstSingleTranslator() },
            { "Last", new LastTranslator() },
            { "LastOrDefault", new LastTranslator() },
            { "Count", new CountTranslator() },
            { "LongCount", new CountTranslator() },
            { "InternalSumExpression", new AggregateExpressionTranslator() },
            { "InternalAverageExpression", new AggregateExpressionTranslator() },
            { "InternalMinExpression", new AggregateExpressionTranslator() },
            { "InternalMaxExpression", new AggregateExpressionTranslator() },
            { "GroupBy", new GroupByTranslator() },
            { "Sum", new DirectAggregateTranslator() },
            { "Average", new DirectAggregateTranslator() },
            { "Min", new DirectAggregateTranslator() },
            { "Max", new DirectAggregateTranslator() },
            { "All", new AllTranslator() },
            { "WithRowNumber", new RowNumberTranslator() },
            { "WithRank", new RankTranslator() },
            { "WithDenseRank", new DenseRankTranslator() },
            { "WithLag", new LagTranslator() },
            { "WithLead", new LeadTranslator() },
            { "Include", new IncludeTranslator() },
            { "ThenInclude", new ThenIncludeTranslator() },
            { "AsNoTracking", new AsNoTrackingTranslator() },
            { "AsSplitQuery", new AsSplitQueryTranslator() },
            { "AsOf", new AsOfTranslator() },
            { "Cast", new CastOrOfTypeTranslator() },
            { "OfType", new CastOrOfTypeTranslator() }
        };

        private static string GetWindowAlias(LambdaExpression selector, int paramIndex, string defaultAlias)
        {
            if (selector.Body is NewExpression ne)
            {
                for (int i = 0; i < ne.Arguments.Count; i++)
                {
                    if (ne.Arguments[i] == selector.Parameters[paramIndex])
                        return ne.Members?[i].Name ?? defaultAlias;
                }
            }
            else if (selector.Body is MemberInitExpression mi)
            {
                foreach (var b in mi.Bindings)
                {
                    if (b is MemberAssignment ma && ma.Expression == selector.Parameters[paramIndex])
                        return b.Member.Name;
                }
            }
            return defaultAlias;
        }

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
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression lambda)
                {
                    lambda = t.ExpandProjection(lambda);
                    var lp = lambda.Parameters[0];
                    t._correlatedParams[lp] = (subMapW, winAliasW);
                    var vctxW = new VisitorContext(t._ctx, subMapW, t._provider, lp, winAliasW, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
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
                // in 3040f49 / e0f1397 / 99a02ce. Restricted to the case where the
                // IMMEDIATE source is a Take/Skip MethodCall — peeling further would
                // recurse into ourselves on nested Where chains. Routed to a helper so
                // the common-path Translate stays stack-frame lean (matters under deep
                // global-filter chaining; see GlobalFilterConcurrencyTests).
                if (node.Arguments[0] is MethodCallExpression directSrc
                    && directSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
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
                if ((t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null) && !t._takeSetByTerminal)
                {
                    throw new NormUnsupportedFeatureException(
                        "Where applied after Take or Skip would silently filter the full table — the " +
                        "translator appends the WHERE onto a flat query, so " +
                        "`q.OrderBy(Id).Take(3).Where(r => r.Active)` emits `WHERE Active = 1 LIMIT 3` " +
                        "which filters every row first then takes 3 surviving rows, instead of taking " +
                        "the first 3 rows and filtering inside that window. SQL needs a subquery wrap " +
                        "(`SELECT * FROM (… LIMIT n) WHERE p`) that nORM doesn't yet emit. " +
                        "Workarounds: " +
                        "(1) Move the Where BEFORE the Take if you want filter-then-window: " +
                        "`q.Where(r => r.Active).OrderBy(Id).Take(3)`. " +
                        "(2) Materialize the window first and filter client-side: " +
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
                        info = (t._mapping, t.EscapeAlias("T" + t._joinCounter));
                        t._correlatedParams[param] = info;
                    }
                    // paramIndexStart = t._params.Count so that this predicate's visitor
                    // does not reuse @p0/@p1 names already allocated by preceding predicates
                    // (e.g. inner Where's compiled param and global-filter constant colliding).
                    var vctx = new VisitorContext(t._ctx, t._mapping, t._provider, param, info.Alias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth, t._params.Count);
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
                    && gb.Arguments.Count == 2
                    && originalProjection.Parameters.Count == 1
                    && originalProjection.Parameters[0].Type.IsGenericType
                    && originalProjection.Parameters[0].Type.GetGenericTypeDefinition() == typeof(IGrouping<,>))
                {
                    var rewritten = RewriteGroupByThenSelect(gb, originalProjection);
                    return t.Visit(rewritten);
                }

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
                                "(a) a correlated subquery via `ctx.Query<X>().Count/Sum/...(...)` in the projection — " +
                                "configure a navigation property and use `parent.Children.Count()` / " +
                                "`parent.Children.Sum(c => c.X)` instead, which nORM emits as a " +
                                "correlated scalar subquery `(SELECT COUNT(*) FROM Children WHERE FK=parent.PK)`; " +
                                "(b) a MULTI-HOP navigation chain like `p.Children.SelectMany(c => c.GrandChildren).Count()` — " +
                                "nORM only emits single-hop nav subqueries; project the inner relation client-side " +
                                "(`Include(p => p.Children).ThenInclude(c => c.GrandChildren)`) then aggregate in " +
                                "memory, or compute the aggregate via a join in the SQL shape directly; " +
                                "(c) a custom helper method or LINQ-to-Objects construct unreachable in SQL — " +
                                "rewrite using SQL-translatable primitives. If you really need client-side " +
                                "evaluation of the projection, set " +
                                "`DbContextOptions.ClientEvaluationPolicy = ClientEvaluationPolicy.Warn` or " +
                                "`.Allow`. Warn logs each occurrence; Allow runs silently.");
                        }

                        // Store both projections plus the original lambda's result type so the
                        // plan's elementType reflects what the caller actually receives.
                        t._projection = serverProjection;
                        t._clientProjection = clientProjection;
                        t._clientProjectionResultType = originalProjection.Body.Type;

                        if (t._ctx.Options.ClientEvaluationPolicy == ClientEvaluationPolicy.Warn)
                        {
                            t._ctx.Options.Logger?.LogQuery(
                                "-- CLIENT-EVAL: Projection split for client-side evaluation",
                                EmptyParamDict,
                                TimeSpan.Zero,
                                0);
                        }
                    }
                    else
                    {
                        // Use original projection (fully translatable to SQL)
                        t._projection = originalProjection;
                    }
                }

                return t.Visit(node.Arguments[0]);
            }

            private static MethodCallExpression RewriteGroupByThenSelect(
                MethodCallExpression groupByCall,
                LambdaExpression selectLambda)
            {
                var groupingType = selectLambda.Parameters[0].Type;
                var keyType = groupingType.GetGenericArguments()[0];
                var sourceType = groupingType.GetGenericArguments()[1];
                var resultType = selectLambda.Body.Type;

                var keyParam = Expression.Parameter(keyType, "k");
                var groupParam = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(sourceType), "g");

                var rewriter = new GroupingProjectionRewriter(selectLambda.Parameters[0], keyParam, groupParam);
                var newBody = rewriter.Visit(selectLambda.Body)!;
                var resultSelector = Expression.Lambda(newBody, keyParam, groupParam);

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

                protected override Expression VisitParameter(ParameterExpression node)
                    => node == _oldGrouping ? _newGroup : base.VisitParameter(node);
            }
        }

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
                    var vctxO = new VisitorContext(t._ctx, subMapO, t._provider, kp, winAliasO, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
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
                // (commit a1eb69e). Restricted to the immediate-source-is-Take/Skip
                // case for the same recursion-avoidance reason and routed through a
                // helper to keep the common-path Translate stack-frame lean.
                if (node.Arguments[0] is MethodCallExpression directOrderSrc
                    && directOrderSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
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
                bool isTopLevelOrder = node.Method.Name is nameof(Queryable.OrderBy)
                                                        or nameof(Queryable.OrderByDescending);
                if (isTopLevelOrder && (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null) && !t._takeSetByTerminal)
                {
                    throw new NormUnsupportedFeatureException(
                        "OrderBy applied after Take or Skip would silently produce wrong rows — the " +
                        "translator currently appends the new ORDER BY onto a flat list, so " +
                        "`OrderByDescending(Score).Take(3).OrderBy(Name)` emits `ORDER BY Score DESC, Name ASC LIMIT 3` " +
                        "which sorts the FULL table by both keys then limits, rather than first taking the " +
                        "top-3 by Score and resorting those 3 by Name. SQL needs a subquery wrap " +
                        "(`SELECT * FROM (… ORDER BY a LIMIT n) ORDER BY b`) that nORM doesn't yet emit. " +
                        "Workarounds: " +
                        "(1) Materialize the windowed result first and resort client-side: " +
                        "`var top = await q.OrderByDescending(Score).Take(3).ToListAsync(); var sorted = top.OrderBy(x => x.Name).ToList();` " +
                        "(2) If you only need a stable secondary ordering, use ThenBy: " +
                        "`q.OrderByDescending(Score).ThenBy(Name).Take(3)` — sorts and limits in one pass " +
                        "(this is a DIFFERENT operation: it picks top-3 by (Score DESC, Name ASC) jointly, " +
                        "not top-3 by Score then resort).");
                }
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression keySelector)
                {
                    keySelector = t.ExpandProjection(keySelector);
                    var param = keySelector.Parameters[0];
                    if (!t._correlatedParams.TryGetValue(param, out var info))
                    {
                        info = (t._mapping, t.EscapeAlias("T" + t._joinCounter));
                        t._correlatedParams[param] = info;
                    }
                    var vctx = new VisitorContext(t._ctx, t._mapping, t._provider, param, info.Alias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth, t._params.Count);
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
                        var scv = new SelectClauseVisitor(t._mapping, t._groupBy, t._provider, info.Alias);
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
                    string CoerceDecimalKey(string sql, Type keyType)
                    {
                        var u = Nullable.GetUnderlyingType(keyType) ?? keyType;
                        return u == typeof(decimal) ? t._provider.NormalizeDecimalForCompare(sql) : sql;
                    }
                    if (keySelector.Body is NewExpression newKey && newKey.Arguments.Count > 0)
                    {
                        foreach (var member in newKey.Arguments)
                        {
                            var memberSql = visitor.Translate(member);
                            t._orderBy.Add((CoerceDecimalKey(memberSql, member.Type), ascending));
                        }
                    }
                    else
                    {
                        var sql = visitor.Translate(keySelector.Body);
                        t._orderBy.Add((CoerceDecimalKey(sql, keySelector.Body.Type), ascending));
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
        /// Implements <c>TakeLast(source, n)</c>. SQL has no native "last N rows"
        /// operator, so we flip the ORDER BY direction of the source's existing
        /// ordering, apply LIMIT n on the reversed sequence (the DB scans only N
        /// rows), then mark the plan with <c>PostReverse=true</c> so the materializer
        /// reverses the small N-row result list to restore the original order.
        /// Requires an upstream OrderBy or a mapped key column the translator can
        /// default-order by; without either, the reversed direction is undefined.
        /// </summary>
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
        private sealed class SkipLastTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => ApplyTailPaging(t, node, isTake: false);
        }

        private static Expression ApplyTailPaging(QueryTranslator t, MethodCallExpression node, bool isTake)
        {
            // Walk the source first so any upstream OrderBy populates _orderBy.
            var source = t.Visit(node.Arguments[0]);

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

        /// <summary>
        /// v1 DistinctBy: SQL fetches every row, then a compiled key selector
        /// dedupes the materialized list keeping the first occurrence per key
        /// (LINQ semantics). The contract -- "one row per distinct key, full row
        /// preserved" -- is correct; for large tables a future translator pass can
        /// emit `ROW_NUMBER() OVER (PARTITION BY key) WHERE rn = 1` server-side
        /// without breaking the caller.
        /// </summary>
        private sealed class DistinctByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var keyLambda = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(
                        ErrorMessages.QueryTranslationFailed,
                        "DistinctBy requires a key selector lambda."));

                // Compile the key selector to an open delegate operating on object so the
                // dedupe loop in the executor doesn't need to know the element type.
                var entityType = keyLambda.Parameters[0].Type;
                var keyType = keyLambda.Body.Type;
                var entityObjParam = Expression.Parameter(typeof(object), "entity");
                var castEntity = Expression.Convert(entityObjParam, entityType);
                var body = new ParameterReplacer(keyLambda.Parameters[0], castEntity).Visit(keyLambda.Body)!;
                var boxedBody = keyType.IsValueType ? (Expression)Expression.Convert(body, typeof(object)) : body;
                var keySelector = Expression.Lambda<Func<object, object?>>(boxedBody, entityObjParam).Compile();

                System.Collections.IList Dedupe(System.Collections.IList list)
                {
                    if (list.Count <= 1) return list;
                    // Build a fresh list of the same element type so the caller gets
                    // back e.g. List<DbiItem> rather than List<object>. Reuse the
                    // executor's CreateList factory via the element type captured here.
                    var deduped = QueryExecutor.CreateList(entityType, list.Count);
                    var seen = new HashSet<object?>(EqualityComparer<object?>.Default);
                    foreach (var item in list)
                    {
                        var key = item is null ? null : keySelector(item);
                        if (seen.Add(key))
                            deduped.Add(item);
                    }
                    return deduped;
                }

                t._postMaterializeTransform = Dedupe;
                return t.Visit(node.Arguments[0]);
            }
        }

        /// <summary>
        /// v1 ExceptBy / IntersectBy / UnionBy. All three share the same shape as
        /// DistinctBy: a compiled key selector wraps a post-materialize transform
        /// that applies the LINQ set-op semantics in memory. ExceptBy and IntersectBy
        /// take an <c>IEnumerable&lt;TKey&gt;</c> as the second arg; UnionBy takes an
        /// <c>IEnumerable&lt;TSource&gt;</c> and appends its by-key-new rows after
        /// the source.
        /// </summary>
        private sealed class ExceptByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => InstallKeyedSetOp(t, node, KeyedSetOp.Except);
        }

        private sealed class IntersectByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => InstallKeyedSetOp(t, node, KeyedSetOp.Intersect);
        }

        private sealed class UnionByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => InstallKeyedSetOp(t, node, KeyedSetOp.Union);
        }

        private enum KeyedSetOp { Except, Intersect, Union }

        private static Expression InstallKeyedSetOp(QueryTranslator t, MethodCallExpression node, KeyedSetOp op)
        {
            // node.Arguments: [0] = source IQueryable<TSource>,
            //                 [1] = second IEnumerable<TKey> (Except/Intersect) or
            //                       IEnumerable<TSource> (Union),
            //                 [2] = key selector lambda.
            if (node.Arguments.Count < 3)
            {
                throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{node.Method.Name} requires a second collection and a key selector."));
            }
            var keyLambda = StripQuotes(node.Arguments[2]) as LambdaExpression
                ?? throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{node.Method.Name} requires a key selector lambda."));

            var entityType = keyLambda.Parameters[0].Type;
            var keySelector = CompileKeySelector(keyLambda);

            // Compile a delegate that LAZILY reads the second collection from its
            // closure access expression. The QueryPlan cache keys by expression
            // fingerprint -- if we captured the IEnumerable's value at translate
            // time, repeated calls with different captured collections would reuse
            // the stale-value transform. Reading live on each transform invocation
            // mirrors how nORM's CompiledParameters re-extract closure values for
            // SQL parameters per-call.
            var secondExpr = node.Arguments[1];
            // Box to non-generic IEnumerable so the delegate type is fixed regardless
            // of whether the user passed IEnumerable<TKey> (Except/Intersect) or
            // IEnumerable<TSource> (Union).
            Expression secondCast = typeof(System.Collections.IEnumerable).IsAssignableFrom(secondExpr.Type)
                ? secondExpr
                : Expression.Convert(secondExpr, typeof(System.Collections.IEnumerable));
            var secondLookup = Expression.Lambda<Func<System.Collections.IEnumerable?>>(secondCast).Compile();

            System.Collections.IList Transform(System.Collections.IList sourceList)
            {
                var second = secondLookup() ?? throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{op}'s second argument resolved to null at invocation time."));

                // Materialize the second collection's keys for this invocation.
                //   Except/Intersect -> second is IEnumerable<TKey> directly
                //   Union            -> second is IEnumerable<TSource>; apply keySelector
                var secondKeys = new HashSet<object?>();
                object?[]? unionAppendRows = null;
                if (op == KeyedSetOp.Union)
                {
                    var rows = new List<object?>();
                    foreach (var item in second)
                    {
                        var key = item is null ? null : keySelector(item);
                        secondKeys.Add(key);
                        rows.Add(item);
                    }
                    unionAppendRows = rows.ToArray();
                }
                else
                {
                    foreach (var item in second) secondKeys.Add(item);
                }

                var result = QueryExecutor.CreateList(entityType, sourceList.Count);
                var seenInResult = new HashSet<object?>();
                foreach (var item in sourceList)
                {
                    var key = item is null ? null : keySelector(item);
                    bool keep = op switch
                    {
                        KeyedSetOp.Except => !secondKeys.Contains(key),
                        KeyedSetOp.Intersect => secondKeys.Contains(key),
                        // Union starts by taking source rows; dedupe by key.
                        KeyedSetOp.Union => true,
                        _ => true
                    };
                    if (keep && seenInResult.Add(key))
                        result.Add(item);
                }
                if (op == KeyedSetOp.Union && unionAppendRows != null)
                {
                    foreach (var item in unionAppendRows)
                    {
                        var key = item is null ? null : keySelector(item);
                        if (seenInResult.Add(key))
                            result.Add(item);
                    }
                }
                return result;
            }

            t._postMaterializeTransform = Transform;
            return t.Visit(node.Arguments[0]);
        }

        private static Func<object, object?> CompileKeySelector(LambdaExpression keyLambda)
        {
            var entityType = keyLambda.Parameters[0].Type;
            var keyType = keyLambda.Body.Type;
            var entityObjParam = Expression.Parameter(typeof(object), "entity");
            var castEntity = Expression.Convert(entityObjParam, entityType);
            var body = new ParameterReplacer(keyLambda.Parameters[0], castEntity).Visit(keyLambda.Body)!;
            var boxedBody = keyType.IsValueType ? (Expression)Expression.Convert(body, typeof(object)) : body;
            return Expression.Lambda<Func<object, object?>>(boxedBody, entityObjParam).Compile();
        }

        private sealed class JoinTranslator : IMethodCallTranslator
        {
            private readonly bool _isGroupJoin;
            public JoinTranslator(bool isGroupJoin) => _isGroupJoin = isGroupJoin;

            /// <summary>
            /// Translates <c>Join</c> and <c>GroupJoin</c> operations by delegating to the appropriate handler.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression representing the join.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return _isGroupJoin ? t.HandleGroupJoin(node) : t.HandleInnerJoin(node);
            }
        }

        private sealed class SelectManyTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates <c>SelectMany</c> calls into SQL <c>JOIN</c> operations.
            /// </summary>
            /// <param name="t">The <see cref="QueryTranslator"/> executing translation.</param>
            /// <param name="node">The method call expression for <c>SelectMany</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleSelectMany(node);
            }
        }

        private sealed class DistinctTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Marks the query results as distinct by setting the <c>DISTINCT</c> flag.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>Distinct</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Set _isDistinct BEFORE visiting the source. JoinBuilder.BuildJoinClauseInto
                // captures `distinct: _isDistinct` at join-emit time (e97b814), which runs
                // INSIDE the Visit below — if we set the flag afterward, the join SQL is
                // built without DISTINCT and the test (Join.Distinct) silently returns
                // duplicates. The Take/Skip pin check below runs AFTER Visit because it
                // needs _take/_skip populated by inner translators.
                t._isDistinct = true;
                var source = t.Visit(node.Arguments[0]);
                // Sister of bca0523 / 47acc83 for Distinct: Distinct applied AFTER a
                // Take/Skip silently de-dupes the FULL projected set before the LIMIT
                // applies — `.Take(3).Distinct()` emits `SELECT DISTINCT col … LIMIT 3`
                // which gives 3 rows from the distinct universe instead of dedupe-of-
                // the-windowed-3. SQL needs a subquery wrap (`SELECT DISTINCT col FROM
                // (… LIMIT n)`) that nORM doesn't yet emit. Detect and throw.
                if ((t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null) && !t._takeSetByTerminal)
                {
                    throw new NormUnsupportedFeatureException(
                        "Distinct applied after Take or Skip would silently dedupe the full table — the " +
                        "translator emits `SELECT DISTINCT col … LIMIT n` which gives N rows from the " +
                        "distinct universe of the full table, not the dedupe of the windowed N rows. " +
                        "LINQ semantics for `q.Take(3).Distinct()` require taking the first 3 and then " +
                        "dropping duplicates inside that window — SQL needs a subquery wrap " +
                        "(`SELECT DISTINCT col FROM (… LIMIT n)`) that nORM doesn't yet emit. " +
                        "Workarounds: " +
                        "(1) Move the Distinct BEFORE the Take if you want dedupe-then-window: " +
                        "`q.Select(x => x.Cat).Distinct().Take(3)` (the canonical top-N-distinct shape). " +
                        "(2) Materialize the window first and dedupe client-side: " +
                        "`var top = await q.Take(3).Select(x => x.Cat).ToListAsync(); var unique = top.Distinct().ToList();`");
                }
                return source;
            }
        }

        private sealed class ReverseTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Reverses the current ordering or applies a descending order on key columns if none exists.
            /// </summary>
            /// <param name="t">The translator working on the query.</param>
            /// <param name="node">The method call expression for <c>Reverse</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Reverse after a Take/Skip-windowed source must reverse only the
                // windowed rows. The default path flips the existing _orderBy
                // direction and sends a single `ORDER BY … DESC LIMIT N` to the
                // server — which picks the BOTTOM rows of the full table, not the
                // reverse of the top-N window. Sister of the post-Take/Skip fixes
                // (3040f49 / e0f1397 / 99a02ce / a1eb69e / bfc8180 / d6de693).
                if (node.Arguments[0] is MethodCallExpression revWinSrc
                    && revWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }
                var revSource = t.Visit(node.Arguments[0]);
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
                return revSource;
            }

            /// <summary>
            /// Windowed-source branch — wraps the source as a derived table and
            /// emits the reversed OrderBy keys on the outer SELECT. Kept in its
            /// own method so the common-path Translate stays stack-frame lean.
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanR = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapR);
                t._mapping = subMapR;
                t.MergeSubPlanParameters(subPlanR);
                var winAliasR = t.EscapeAlias("__wrev" + t._joinCounter++);
                t._sql.AppendFragment("SELECT * FROM (").Append(subPlanR.Sql).AppendFragment(") AS ").Append(winAliasR);
                // Lift the source's OrderBy keys and flip them for the outer SELECT.
                // If no explicit OrderBy is present in the source chain, fall back
                // to the mapping's key columns ordered descending (matching the
                // unwindowed-Reverse default).
                var orderKeysR = ExtractOrderByKeys(node.Arguments[0]);
                t._orderBy.Clear();
                if (orderKeysR.Count > 0)
                {
                    foreach (var (keyLambda, asc) in orderKeysR)
                    {
                        var okParam = keyLambda.Parameters[0];
                        if (!t._correlatedParams.ContainsKey(okParam))
                            t._correlatedParams[okParam] = (subMapR, winAliasR);
                        var vctxOk = new VisitorContext(t._ctx, subMapR, t._provider, okParam, winAliasR, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
                        var okVisitor = FastExpressionVisitorPool.Get(in vctxOk);
                        var okSql = okVisitor.Translate(keyLambda.Body);
                        FastExpressionVisitorPool.Return(okVisitor);
                        t._orderBy.Add((okSql, !asc));
                    }
                }
                else
                {
                    foreach (var key in subMapR.KeyColumns)
                        t._orderBy.Add(($"{winAliasR}.{key.EscCol}", false));
                }
                return node;
            }
        }

        private sealed class SetOperationTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Handles set operations such as <c>Union</c>, <c>Intersect</c>, and <c>Except</c> by combining
            /// the SQL generated for the left and right sequences.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression representing the set operation.</param>
            /// <returns>The original method call expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // SQL parsers reject ORDER BY / LIMIT / OFFSET on a bare set-op arm.
                // SQLite is strictest ("ORDER BY clause should come after UNION ALL
                // not before"); SqlServer and Postgres tolerate some forms but the
                // semantics are dialect-specific. Wrap any arm that carries an
                // OrderBy/Take/Skip as a derived table so the clause binds to the
                // arm only — every dialect accepts `SELECT * FROM (subq) AS alias`.
                bool leftNeedsWrap  = SourceHasOrderTakeOrSkip(node.Arguments[0]);
                bool rightNeedsWrap = SourceHasOrderTakeOrSkip(node.Arguments[1]);
                // UNION / INTERSECT / EXCEPT all use set semantics that dedup by string
                // equality on SQLite, so '10.5' vs '10.50' register as distinct rows even
                // though they're the same decimal. Concat (UNION ALL) doesn't dedup, but
                // we coerce uniformly so the materialized values match across arms (without
                // coercion one arm could yield decimal 10.5 from '10.5' while the other
                // yields 10.50 from '10.50', producing inconsistent row shapes for the same
                // logical value). The flag is scoped per-arm via try/finally.
                var savedCoerce = t._coerceDecimalProjectionsToReal;
                t._coerceDecimalProjectionsToReal = true;
                string leftSql, rightSql;
                try
                {
                    leftSql = t.TranslateSubExpression(node.Arguments[0]);
                    rightSql = t.TranslateSubExpression(node.Arguments[1]);
                }
                finally
                {
                    t._coerceDecimalProjectionsToReal = savedCoerce;
                }
                var setOp = node.Method.Name switch
                {
                    "Union" => "UNION",
                    // Concat preserves duplicates (LINQ-to-Objects semantics) -> UNION ALL.
                    "Concat" => "UNION ALL",
                    "Intersect" => "INTERSECT",
                    "Except" => "EXCEPT",
                    _ => throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, "Set operation"))
                };
                // SQLite rejects bare-parenthesised compound SELECTs in set ops;
                // every dialect accepts `SELECT * FROM (subq) AS alias` though,
                // so wrap each LIMIT/OFFSET arm as a derived table. Unwrapped arms
                // are emitted as-is to keep the SQL minimal.
                t._sql.Clear();
                if (leftNeedsWrap) t._sql.Append("SELECT * FROM (").Append(leftSql).Append(") AS ").Append(t._provider.Escape("__lset0"));
                else t._sql.Append(leftSql);
                t._sql.Append(' ').Append(setOp).Append(' ');
                if (rightNeedsWrap) t._sql.Append("SELECT * FROM (").Append(rightSql).Append(") AS ").Append(t._provider.Escape("__rset0"));
                else t._sql.Append(rightSql);
                // TranslateSubExpression isolates each side in its own context, so any
                // Select-projection inside the arguments never propagates to the outer
                // translator. Without it, Generate() builds the materializer against
                // _mapping.Columns — for an anonymous-typed Union (`Select(p => new {…})
                // .Union(Select(c => new {…}))`) that's the left-source entity's columns,
                // not the projected anonymous type's ctor params, and GetCachedConstructor
                // throws "No suitable constructor for <>f__AnonymousType…" because the
                // arities don't match. Lift the projection from the left source (compiler
                // forces both sides to share the same shape for typed Union) so the
                // materializer reconstructs the anonymous type correctly.
                if (t._projection == null)
                {
                    var lifted = ExtractTrailingProjection(node.Arguments[0]);
                    if (lifted != null)
                        t._projection = lifted;
                }
                return node;
            }

            /// <summary>
            /// Returns true when the arm-source chain contains <c>OrderBy</c>,
            /// <c>OrderByDescending</c>, <c>ThenBy</c>, <c>ThenByDescending</c>,
            /// <c>Take</c>, or <c>Skip</c> — clauses that emit ORDER BY / LIMIT /
            /// OFFSET in the arm SQL and must be wrapped as a derived table so the
            /// outer set op parses.
            /// </summary>
            private static bool SourceHasOrderTakeOrSkip(Expression source)
            {
                var current = source;
                while (current is MethodCallExpression mce)
                {
                    if (mce.Method.Name is nameof(Queryable.Take)
                        or nameof(Queryable.Skip)
                        or nameof(Queryable.OrderBy)
                        or nameof(Queryable.OrderByDescending)
                        or nameof(Queryable.ThenBy)
                        or nameof(Queryable.ThenByDescending))
                        return true;
                    if (mce.Arguments.Count == 0) break;
                    current = mce.Arguments[0];
                }
                return false;
            }

            /// <summary>
            /// Walks back through the source expression chain looking for the most-recent
            /// projection-defining call (Select / SelectMany) and returns its lambda.
            /// Skips over Where / OrderBy / Take / Skip / Distinct / Reverse / AsNoTracking
            /// / AsSplitQuery, which preserve the projection shape. Returns null if no
            /// projecting call is found (e.g. raw `Query&lt;T&gt;()` on both sides — the
            /// entity-Columns path the outer Generate() will fall back to is already correct).
            /// </summary>
            private static LambdaExpression? ExtractTrailingProjection(Expression source)
            {
                var current = source;
                while (current is MethodCallExpression mce)
                {
                    if ((mce.Method.Name == nameof(Queryable.Select)
                         || mce.Method.Name == nameof(Queryable.SelectMany))
                        && mce.Arguments.Count >= 2
                        && QueryTranslator.StripQuotes(mce.Arguments[mce.Arguments.Count - 1]) is LambdaExpression lambda)
                    {
                        return lambda;
                    }
                    if (mce.Arguments.Count == 0) break;
                    current = mce.Arguments[0];
                }
                return null;
            }
        }

        private sealed class SetPredicateTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates set-based predicates like <c>Any</c> or <c>Contains</c>.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for the predicate.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleSetOperation(node);
            }
        }

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
                var pNameEa = t._ctx.Provider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
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
                var pName = t._ctx.Provider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
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
                    var vctxWin = new VisitorContext(t._ctx, subMappingFs, t._provider, winParam, winAlias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
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
                    var pNameW = t._ctx.Provider.ParamPrefix + "p" + t._parameterManager.Index++;
                    t._params[pNameW] = t._take;
                    t._takeParam = pNameW;
                    t._takeSetByTerminal = true;
                    t._singleResult = t._methodName == "First" || t._methodName == "Single";
                    return node;
                }
                if (node.Arguments.Count > 1)
                {
                    var predicate = StripQuotes(node.Arguments[1]) as LambdaExpression;
                    if (predicate != null)
                    {
                        var param = predicate.Parameters[0];
                        var alias = t.EscapeAlias("T" + t._joinCounter);
                        if (!t._correlatedParams.ContainsKey(param))
                            t._correlatedParams[param] = (t._mapping, alias);
                        var vctxFS = new VisitorContext(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth, t._params.Count);
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
                }
                // Single/SingleOrDefault must fetch 2 rows so the caller can detect duplicates.
                // First/FirstOrDefault only need 1 row.
                var isSingle = t._methodName == "Single" || t._methodName == "SingleOrDefault";
                t._take = isSingle ? 2 : 1;
                var pName = t._ctx.Provider.ParamPrefix + "p" + t._parameterManager.Index++;
                t._params[pName] = t._take;
                t._takeParam = pName;
                // Mark this _take as set by a terminal so the post-Take/Skip pin family
                // doesn't false-positive on `q.OrderBy(k).First()`-style chains (the
                // pins fire on `_take.HasValue && !_takeSetByTerminal`).
                t._takeSetByTerminal = true;
                t._singleResult = t._methodName == "First" || t._methodName == "Single";
                // Preserve the terminal method name because visiting source arguments will
                // overwrite _methodName with child operator names (e.g., "Where").
                var terminalMethod = t._methodName;
                var result = t.Visit(node.Arguments[0]);
                t._methodName = terminalMethod;
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
                        var vctxLW = new VisitorContext(t._ctx, lastSubMap, t._provider, lwParam, lastAlias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
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
                            var vctxOk = new VisitorContext(t._ctx, lastSubMap, t._provider, okParam, lastAlias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
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
                    var pNameLW = t._ctx.Provider.ParamPrefix + "p" + t._parameterManager.Index++;
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
                        var vctxLast = new VisitorContext(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth, t._params.Count);
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
                var pName = t._ctx.Provider.ParamPrefix + "p" + t._parameterManager.Index++;
                t._params[pName] = 1;
                t._takeParam = pName;
                t._takeSetByTerminal = true;
                t._singleResult = t._methodName == "Last";
                return lastSrc;
            }
        }

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
                    var vctxC = new VisitorContext(t._ctx, subMapC, t._provider, lp, winAliasC, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth + 1, t._params.Count);
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
                // Count after Take/Skip-windowed source — count only the windowed rows.
                // Sister of the post-Take/Skip family (3040f49 / e0f1397 / 99a02ce /
                // a1eb69e / bfc8180 / d6de693 / e0529a0).
                if (node.Arguments[0] is MethodCallExpression countWinSrc
                    && countWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }
                var source = t.Visit(node.Arguments[0]);
                // Preserve _projection when DISTINCT is active so the COUNT(*) builder can
                // wrap as `SELECT COUNT(*) FROM (SELECT DISTINCT <proj> FROM ...) AS T0` —
                // otherwise nuke it so Count(x => predicate) doesn't try to project columns
                // that aren't relevant to the row-count.
                if (!t._isDistinct)
                    t._projection = null;
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
                        var vctxCount = new VisitorContext(t._ctx, t._mapping, t._provider, param, info.Alias, t._correlatedParams, t._compiledParams, t._paramMap, t._recursionDepth, t._params.Count);
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

        private sealed class AggregateExpressionTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Delegates translation of aggregate expressions used in projected form, such as <c>Sum(x =&gt; ...)</c>.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression representing the aggregate.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleAggregateExpression(node);
            }
        }

        private sealed class GroupByTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Converts a LINQ <c>GroupBy</c> call into the SQL <c>GROUP BY</c> clause and related projections.
            /// </summary>
            /// <param name="t">The translator responsible for query compilation.</param>
            /// <param name="node">The method call expression for <c>GroupBy</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleGroupBy(node);
            }
        }

        private sealed class DirectAggregateTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Handles direct aggregate operators like <c>Sum</c>, <c>Min</c>, or <c>Max</c> that operate on the entire sequence.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for the aggregate.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleDirectAggregate(node);
            }
        }

        private sealed class AllTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates the <c>All</c> predicate to its SQL equivalent, typically using <c>NOT EXISTS</c>.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>All</c>.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleAllOperation(node);
            }
        }

        private sealed class RowNumberTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Adds a <c>ROW_NUMBER()</c> window function to the query and binds the result to the supplied selector.
            /// </summary>
            /// <param name="t">The translator creating the window function.</param>
            /// <param name="node">The method call expression for <c>WithRowNumber</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var resultSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithRowNumber requires a result selector"));
                var alias = GetWindowAlias(resultSelector, 1, "RowNumber");
                var wf = new WindowFunctionInfo("ROW_NUMBER", null, 0, null, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class RankTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Registers a <c>RANK()</c> window function and maps the result via the provided selector.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>WithRank</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var resultSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithRank requires a result selector"));
                var alias = GetWindowAlias(resultSelector, 1, "Rank");
                var wf = new WindowFunctionInfo("RANK", null, 0, null, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class DenseRankTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Adds a <c>DENSE_RANK()</c> window function to the query.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>WithDenseRank</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var resultSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithDenseRank requires a result selector"));
                var alias = GetWindowAlias(resultSelector, 1, "DenseRank");
                var wf = new WindowFunctionInfo("DENSE_RANK", null, 0, null, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class LagTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Generates a <c>LAG</c> window function, capturing value, offset and optional default expressions.
            /// </summary>
            /// <param name="t">The translator creating the window.</param>
            /// <param name="node">The method call expression for <c>WithLag</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var valueSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLag requires a value selector"));
                int offset = TryGetIntValue(node.Arguments[2], out var off) ? off : 1;
                var resultSelector = StripQuotes(node.Arguments[3]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLag requires a result selector"));
                LambdaExpression? defaultSelector = null;
                if (node.Arguments.Count > 4)
                    defaultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;
                var alias = GetWindowAlias(resultSelector, 1, "Lag");
                var wf = new WindowFunctionInfo("LAG", valueSelector, offset, defaultSelector, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class LeadTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Generates a <c>LEAD</c> window function, capturing value, offset and optional default expressions.
            /// </summary>
            /// <param name="t">The translator creating the window.</param>
            /// <param name="node">The method call expression for <c>WithLead</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var valueSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLead requires a value selector"));
                int offset = TryGetIntValue(node.Arguments[2], out var off) ? off : 1;
                var resultSelector = StripQuotes(node.Arguments[3]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLead requires a result selector"));
                LambdaExpression? defaultSelector = null;
                if (node.Arguments.Count > 4)
                    defaultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;
                var alias = GetWindowAlias(resultSelector, 1, "Lead");
                var wf = new WindowFunctionInfo("LEAD", valueSelector, offset, defaultSelector, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class IncludeTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Processes an <c>Include</c> call, registering the requested navigation path for eager loading.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>Include</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Support both instance calls (source.Include(lambda)) and
                // static/extension calls (Include(source, lambda)).
                Expression source;
                Expression? rawLambda;
                if (node.Object != null)
                {
                    source = node.Object;
                    rawLambda = node.Arguments.Count > 0 ? node.Arguments[0] : null;
                }
                else
                {
                    source = node.Arguments[0];
                    rawLambda = node.Arguments.Count > 1 ? node.Arguments[1] : null;
                }

                // Visit source FIRST to establish _mapping before the Relations lookup.
                var visited = t.Visit(source);

                if (rawLambda != null)
                {
                    var includeLambda = rawLambda is UnaryExpression qu ? qu.Operand as LambdaExpression : rawLambda as LambdaExpression;
                    if (includeLambda != null)
                    {
                        var member = includeLambda.Body is UnaryExpression unary ?
                                     (MemberExpression)unary.Operand :
                                     (MemberExpression)includeLambda.Body;
                        var propName = member.Member.Name;
                        if (t._mapping != null && t._mapping.Relations.TryGetValue(propName, out var relation))
                        {
                            // Guard against composite-PK dependents early at translation time.
                            var depMap = t._ctx.GetMapping(relation.DependentType);
                            if (depMap.KeyColumns.Length > 1)
                                throw new NormUnsupportedFeatureException(
                                    $"Include on '{depMap.Type.Name}' with a composite primary key is not supported by " +
                                    "the eager-loader. The IN-batched parent-key fetch matches one column; composite " +
                                    "PKs need a tuple-IN predicate that nORM doesn't emit. Workarounds: " +
                                    "(1) write an explicit join with projection: `from p in ctx.Query<Parent>() join c " +
                                    "in ctx.Query<Child>() on p.Id equals c.ParentId select new { p, c }` and rebuild " +
                                    "the parent graph client-side; (2) fetch the principals first, then issue a second " +
                                    "`ctx.Query<Child>().Where(c => parentIds.Contains(c.ParentId)).ToListAsync()` and " +
                                    "associate manually; (3) reshape the dependent so its PK is a single surrogate column.");
                            t._includes.Add(new IncludePlan(new List<TableMapping.Relation> { relation }));
                            t.TrackMapping(relation.DependentType);
                        }
                        else if (t._mapping != null)
                        {
                            // Check if this is a many-to-many navigation property
                            var jtm = t._mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == propName);
                            if (jtm != null)
                            {
                                t._m2mIncludes.Add(new M2MIncludePlan(jtm));
                                t.TrackMapping(jtm.RightType);
                            }
                        }
                    }
                }
                return visited;
            }
        }

        private sealed class ThenIncludeTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Extends the most recently registered include path with an additional navigation property.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>ThenInclude</c>.</param>
            /// <returns>The translated expression representing the parent include.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var parentExpression = t.Visit(node.Arguments[0]);
                if (node.Arguments.Count > 1)
                {
                    // Arguments[1] is typically a quoted lambda (UnaryExpression{Quote}); strip quotes.
                    var thenLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
                    if (thenLambda != null)
                    {
                        var member = thenLambda.Body is UnaryExpression unary2 ?
                                     (MemberExpression)unary2.Operand :
                                     (MemberExpression)thenLambda.Body;
                        var propName = member.Member.Name;
                        if (t._includes.Count > 0)
                        {
                            var lastInclude = t._includes[^1];
                            var lastRelation = lastInclude.Path.Last();
                            var parentMap = t.TrackMapping(lastRelation.DependentType);
                            if (parentMap.Relations.TryGetValue(propName, out var relation))
                            {
                                // Guard against composite-PK dependents early at translation time.
                                var depMap = t._ctx.GetMapping(relation.DependentType);
                                if (depMap.KeyColumns.Length > 1)
                                    throw new NormUnsupportedFeatureException(
                                        $"Include with composite primary key is not supported for '{relation.DependentType.Name}'.");
                                lastInclude.Path.Add(relation);
                                t.TrackMapping(relation.DependentType);
                            }
                        }
                    }
                }
                return parentExpression;
            }
        }

        private sealed class AsNoTrackingTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Marks the query so that returned entities are not tracked by the context.
            /// </summary>
            /// <param name="t">The translator applying the option.</param>
            /// <param name="node">The method call expression for <c>AsNoTracking</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._noTracking = true;
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        private sealed class CastOrOfTypeTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = node.Arguments[0];
                var sourceElement = GetElementType(source);
                var targetElement = node.Method.GetGenericArguments().FirstOrDefault();
                if (targetElement == null)
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} requires a generic type argument.");
                }
                // Cast / OfType collapse to an identity pass-through at the SQL layer when
                // the target type matches the source element type (or is a reference-type
                // base that the runtime cast will satisfy on materialization). TPH/derived
                // filtering by discriminator isn't wired in v1 — surface that case explicitly
                // rather than silently returning the wrong rows.
                if (targetElement == sourceElement ||
                    (!targetElement.IsValueType && targetElement.IsAssignableFrom(sourceElement)))
                {
                    return t.Visit(source);
                }
                throw new NormUnsupportedFeatureException(
                    $"{node.Method.Name}<{targetElement.Name}>() on IQueryable<{sourceElement.Name}> would require a runtime type filter " +
                    "that nORM has not yet wired to TPH/discriminator metadata. Project explicitly with Select(...) instead.");
            }
        }

        private sealed class AsSplitQueryTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Indicates that related data should be loaded using multiple queries instead of a single join.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>AsSplitQuery</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._splitQuery = true;
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        private sealed class AsOfTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Applies temporal querying by translating the <c>AsOf</c> operation into a timestamp filter.
            /// </summary>
            /// <param name="t">The translator managing the temporal context.</param>
            /// <param name="node">The method call expression for <c>AsOf</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var timeTravelArg = node.Arguments[1];
                if (QueryTranslator.TryGetConstantValue(timeTravelArg, out var value))
                {
                    if (value is DateTime dt)
                    {
                        t._asOfTimestamp = dt;
                    }
                    else if (value is string tagName)
                    {
                        t._asOfTimestamp = t.GetTimestampForTagAsync(tagName).GetAwaiter().GetResult();
                    }
                }
                else
                {
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, ".AsOf() requires a constant DateTime or string tag."));
                }
                return t.Visit(node.Arguments[0]);
            }
        }
    }
}
