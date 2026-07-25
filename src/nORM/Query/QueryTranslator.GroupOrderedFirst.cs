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
        // Correlated-subquery translation for the "first / greatest-N row per group"
        // projection shape (g.OrderBy(...).First().Member and friends). Split out of
        // QueryTranslator.GroupByProjection.cs to keep each partial focused; every member
        // stays on the same partial QueryTranslator class.
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

            // The subquery re-scans the grouped table, so the entity's global filters (soft-delete) AND the
            // tenant predicate must be repeated inside it — the outer GROUP BY is filtered by ApplyGlobalFilters
            // but this correlated subquery is not, so a soft-deleted or cross-tenant "latest" row would
            // otherwise win the ordering and leak. CombineWithTenant applies the tenant equality EXPLICITLY
            // here rather than relying on it incidentally riding in _groupOrderedFirstSourceWheres below — so
            // tenant isolation of the greatest-N-per-group re-scan is defense-in-depth, not accidental.
            var globalFilter = GlobalFilterFragment.CombineWithTenant(_ctx, _mapping.Type);
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
                    case "AsNoTrackingWithIdentityResolution":
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
            return _provider.ExactKeySql(sql, partType);
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

    }
}
