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
        private sealed class OrderByTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// True when the source, walked through any OrderBy/ThenBy chain, bottoms out at a set operation.
            /// The order keys of a set-op result must reference unqualified result columns, and a ThenBy after
            /// <c>OrderBy(&lt;setop&gt;)</c> is still ordering that compound.
            /// </summary>
            private static bool SourceChainIsSetOp(Expression source)
            {
                var current = source;
                while (current is MethodCallExpression m)
                {
                    if (m.Method.Name is "Union" or "Concat" or "Intersect" or "Except")
                        return true;
                    if (m.Arguments.Count > 0
                        && m.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                                         or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending))
                    {
                        current = m.Arguments[0];
                        continue;
                    }
                    return false;
                }
                return false;
            }

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
                // Walk through any OrderBy/ThenBy chain: a ThenBy after `OrderBy(<setop>)` still orders the
                // compound, so its key must be an unqualified result column too. Detecting only a DIRECT set-op
                // source ordered the first key correctly but let a trailing ThenBy fall through to the path that
                // table-qualifies the term, which a compound SELECT rejects ("Nth ORDER BY term does not match
                // any column in the result set").
                bool sourceIsSetOp = SourceChainIsSetOp(node.Arguments[0]);
                var source = t.Visit(node.Arguments[0]);
                // A set op with a LOCAL sequence arm is evaluated CLIENT-side (the DB arm's
                // rows are combined in memory), leaving a post-materialize tail. Its trailing
                // OrderBy must sort the COMBINED sequence client-side too — the SQL-side set-op
                // ordering branches below would order only the DB arm and let the appended local
                // values fall out of order. Skip them so control reaches the post-materialize
                // ordering path (which sorts the assembled rows).
                bool setOpIsClientEvaluated = sourceIsSetOp && t.IsPostMaterializeTailMode;
                if (sourceIsSetOp && !setOpIsClientEvaluated
                    && QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression setOpKeySel
                    && setOpKeySel.Body is MemberExpression keyMember
                    && keyMember.Expression is ParameterExpression)
                {
                    var ascendingSet = !node.Method.Name.Contains("Descending");
                    // A full-entity set op names each result column by its MAPPED column (the union arm
                    // selects `t.Col`), so order by the mapped column — honouring a fluent HasColumnName
                    // rename, which the CLR property name misses. A projection set op
                    // (`.Select(x => new { V }).Union(...)`) aliases its output by the member name, so that
                    // shape keeps the member name; the two are distinguished by whether the key member is a
                    // column on the entity mapping (its declaring type is the mapped entity or a base).
                    var setOpOrderCol =
                        keyMember.Member.DeclaringType?.IsAssignableFrom(t._mapping.Type) == true
                        && t._mapping.ColumnsByName.TryGetValue(keyMember.Member.Name, out var setOpKeyColumn)
                            ? setOpKeyColumn.EscCol
                            : t._provider.Escape(keyMember.Member.Name);
                    t._orderBy.Add((setOpOrderCol, ascendingSet));
                    return source;
                }
                // Identity order key over a set op: `.Select(r => r.Scalar).Union(...).OrderBy(x => x)`.
                // Identity ordering (`x => x`) only compiles for a comparable SCALAR element, so the
                // compound query has exactly ONE result column (the projected scalar). Order by its
                // ordinal position — an ordinal ORDER BY is valid in a compound SELECT on all four
                // providers and, unlike a table-qualified term, is accepted against the UNION result
                // set (the member-key branch above table-/name-qualifies, which a compound rejects).
                if (sourceIsSetOp && !setOpIsClientEvaluated
                    && QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression setOpIdentitySel
                    && setOpIdentitySel.Body is ParameterExpression setOpIdentityParam
                    && setOpIdentityParam == setOpIdentitySel.Parameters[0])
                {
                    var ascendingIdentity = !node.Method.Name.Contains("Descending");
                    t._orderBy.Add(("1", ascendingIdentity));
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
