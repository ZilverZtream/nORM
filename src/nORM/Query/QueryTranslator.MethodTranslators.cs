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
            { "Join", new JoinTranslator(false) },
            { "GroupJoin", new JoinTranslator(true) },
            { "SelectMany", new SelectManyTranslator() },
            { "Distinct", new DistinctTranslator() },
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
            /// Translates a LINQ <c>Where</c> call into an equivalent SQL <c>WHERE</c> clause.
            /// </summary>
            /// <param name="t">The active <see cref="QueryTranslator"/>.</param>
            /// <param name="node">The method call expression for <c>Where</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
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
                // Sister of bca0523 for Where: Where applied AFTER a Take/Skip would
                // silently filter the FULL table before the LIMIT applies (SQL appends
                // WHERE to the flat query), instead of filtering inside the windowed
                // result as LINQ semantics demand. Detect and pin with the same
                // materialize-then-filter workaround pattern. Note: Where BEFORE Take is
                // the common pre-paging filter pattern and stays untouched — this only
                // fires when `_take`/`_skip` is already set by the time Where translates,
                // i.e. the chain order is `…Take(n).Where(p)`.
                if (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null)
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
                    if (t.TrySplitProjection(originalProjection, out var serverProjection, out var clientProjection))
                    {
                        if (t._ctx.Options.ClientEvaluationPolicy == ClientEvaluationPolicy.Throw)
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
            /// Translates <c>OrderBy</c> and related ordering methods into SQL <c>ORDER BY</c> clauses.
            /// </summary>
            /// <param name="t">The <see cref="QueryTranslator"/> orchestrating translation.</param>
            /// <param name="node">The method call expression for the ordering method.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = t.Visit(node.Arguments[0]);
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
                if (isTopLevelOrder && (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null))
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
                    // Composite anonymous-type key (e.g. `OrderBy(r => new { r.A, r.B })`) —
                    // emit one ORDER BY entry per member so the SQL becomes
                    // `ORDER BY "T0"."A", "T0"."B"` rather than the comma-joined single
                    // SELECT-list shape that the projection visitor would emit naturally
                    // and that SQL rejects inside ORDER BY.
                    if (keySelector.Body is NewExpression newKey && newKey.Arguments.Count > 0)
                    {
                        foreach (var member in newKey.Arguments)
                        {
                            var memberSql = visitor.Translate(member);
                            t._orderBy.Add((memberSql, ascending));
                        }
                    }
                    else
                    {
                        var sql = visitor.Translate(keySelector.Body);
                        t._orderBy.Add((sql, ascending));
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
                var source = t.Visit(node.Arguments[0]);
                // Sister of bca0523 / 47acc83 for Distinct: Distinct applied AFTER a
                // Take/Skip silently de-dupes the FULL projected set before the LIMIT
                // applies — `.Take(3).Distinct()` emits `SELECT DISTINCT col … LIMIT 3`
                // which gives 3 rows from the distinct universe instead of dedupe-of-
                // the-windowed-3. SQL needs a subquery wrap (`SELECT DISTINCT col FROM
                // (… LIMIT n)`) that nORM doesn't yet emit. Detect and throw.
                if (t._take.HasValue || t._takeParam != null || t._skip.HasValue || t._skipParam != null)
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
                t._isDistinct = true;
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
                var leftSql = t.TranslateSubExpression(node.Arguments[0]);
                var rightSql = t.TranslateSubExpression(node.Arguments[1]);
                var setOp = node.Method.Name switch
                {
                    "Union" => "UNION",
                    // Concat preserves duplicates (LINQ-to-Objects semantics) -> UNION ALL.
                    "Concat" => "UNION ALL",
                    "Intersect" => "INTERSECT",
                    "Except" => "EXCEPT",
                    _ => throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, "Set operation"))
                };
                t._sql.Clear();
                t._sql.Append(leftSql).Append(' ').Append(setOp).Append(' ').Append(rightSql);
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
            /// Applies <c>ElementAt</c> or <c>ElementAtOrDefault</c> semantics by adjusting the skip/take parameters.
            /// </summary>
            /// <param name="t">The active <see cref="QueryTranslator"/>.</param>
            /// <param name="node">The method call expression for the element access.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
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
                t._singleResult = t._methodName == "First" || t._methodName == "Single";
                // Preserve the terminal method name because visiting source arguments will
                // overwrite _methodName with child operator names (e.g., "Where").
                var terminalMethod = t._methodName;
                var result = t.Visit(node.Arguments[0]);
                t._methodName = terminalMethod;
                return result;
            }
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
                t._singleResult = t._methodName == "Last";
                return lastSrc;
            }
        }

        private sealed class CountTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Processes <c>Count</c> and <c>LongCount</c>, optionally translating a predicate and marking the query as aggregate.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for the count operation.</param>
            /// <returns>The updated expression node.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._isAggregate = true;
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
