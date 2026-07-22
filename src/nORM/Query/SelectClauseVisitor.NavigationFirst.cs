using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using nORM.Core;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        /// <summary>
        /// Order-key CLR types whose SQL ordering matches C# ordering, so a navigation
        /// <c>OrderBy(key).Select(sel).First()</c> selects the SAME row both ways. Excludes string
        /// (culture vs collation), decimal / TimeSpan / DateTimeOffset / TimeOnly (TEXT-canonical storage
        /// lex-sorts differently) — those keep the client-evaluation fallback rather than risk a
        /// silent-wrong first row.
        /// </summary>
        /// <summary>
        /// True when a lambda body reads a member of its element parameter that maps to a
        /// value-converter column — which the navigation First/order emit cannot handle (raw stored value
        /// / stored-representation ordering). Nested (nav-chain) members are conservatively ignored here.
        /// </summary>
        private static bool LambdaReferencesConverterColumn(LambdaExpression lambda, TableMapping depMap)
        {
            var param = lambda.Parameters[0];
            var finder = new ConverterColumnFinder(param, depMap);
            finder.Visit(lambda.Body);
            return finder.Found;
        }

        private sealed class ConverterColumnFinder : ExpressionVisitor
        {
            private readonly ParameterExpression _param;
            private readonly TableMapping _depMap;
            public bool Found { get; private set; }
            public ConverterColumnFinder(ParameterExpression param, TableMapping depMap) { _param = param; _depMap = depMap; }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (!Found && node.Expression == _param
                    && _depMap.ColumnsByName.TryGetValue(node.Member.Name, out var col)
                    && col.Converter != null)
                    Found = true;
                return base.VisitMember(node);
            }
        }

        internal static bool IsSafeNavFirstOrderKeyType(Type t)
        {
            var u = Nullable.GetUnderlyingType(t) ?? t;
            if (u.IsEnum) return true;
            return u == typeof(int) || u == typeof(long) || u == typeof(short) || u == typeof(byte)
                || u == typeof(sbyte) || u == typeof(uint) || u == typeof(ulong) || u == typeof(ushort)
                || u == typeof(bool) || u == typeof(double) || u == typeof(float)
                || u == typeof(DateTime) || u == typeof(DateOnly);
        }

        /// <summary>
        /// Structural recognizer (no mapping) for
        /// <c>nav.[Where(pred).]OrderBy*(safeKey).Select(scalar).First/FirstOrDefault/Last/LastOrDefault()</c>
        /// — the deterministic "top related value" shape the projection emits as a LIMIT-1 correlated
        /// subquery. The analyzer uses this to admit exactly this shape (and nothing else) without
        /// exposing every First over a navigation to the visitor.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal static bool IsNavigationOrderedFirstShape(MethodCallExpression node)
        {
            if (node.Method.Name is not (nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                    or nameof(Queryable.Last) or nameof(Queryable.LastOrDefault))
                || node.Arguments.Count != 1
                || node.Arguments[0] is not MethodCallExpression selCall
                || selCall.Method.Name != nameof(Queryable.Select)
                || selCall.Arguments.Count != 2
                || StripQuotes(selCall.Arguments[1]) is not LambdaExpression selLambda)
                return false;
            var selT = Nullable.GetUnderlyingType(selLambda.Body.Type) ?? selLambda.Body.Type;
            if (!selT.IsValueType && selT != typeof(string))
                return false;

            Expression cur = selCall.Arguments[0];
            bool hasOrdering = false;
            while (cur is MethodCallExpression ord
                && ord.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                    or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending)
                && ord.Arguments.Count == 2
                && StripQuotes(ord.Arguments[1]) is LambdaExpression keyLambda)
            {
                if (!IsSafeNavFirstOrderKeyType(keyLambda.Body.Type))
                    return false;
                hasOrdering = true;
                cur = ord.Arguments[0];
            }
            if (!hasOrdering)
                return false;
            if (cur is MethodCallExpression w && w.Method.Name == nameof(Queryable.Where) && w.Arguments.Count == 2)
                cur = w.Arguments[0];
            return cur is MemberExpression { Expression: ParameterExpression } m
                && m.Type != typeof(string)
                && typeof(System.Collections.IEnumerable).IsAssignableFrom(m.Type)
                && m.Type.IsGenericType;
        }

        /// <summary>
        /// Emits <c>nav.[Where(pred).]OrderBy*(key).Select(sel).First/Last()</c> as a LIMIT-1 correlated
        /// subquery: <c>(SELECT sel FROM child WHERE fk = pk [AND pred] ORDER BY keys [LIMIT 1])</c>.
        /// Last reverses the ordering. Empty collection → SQL NULL (matches FirstOrDefault; the correlated-
        /// subquery family shares this documented divergence from First-throws-on-empty).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private bool TryVisitNavigationOrderedFirstScalar(MethodCallExpression node, StringBuilder sb)
        {
            if (!IsNavigationOrderedFirstShape(node))
                return false;
            var selCall = (MethodCallExpression)node.Arguments[0];
            var selectorLambda = (LambdaExpression)StripQuotes(selCall.Arguments[1])!;

            var orderings = new List<(LambdaExpression Key, bool Ascending)>();
            Expression cur = selCall.Arguments[0];
            while (cur is MethodCallExpression ord
                && ord.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                    or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending)
                && StripQuotes(ord.Arguments[1]) is LambdaExpression keyLambda)
            {
                orderings.Add((keyLambda, ord.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.ThenBy)));
                cur = ord.Arguments[0];
            }
            orderings.Reverse(); // collected outer-first; primary OrderBy must lead

            LambdaExpression? filter = null;
            if (cur is MethodCallExpression w && w.Method.Name == nameof(Queryable.Where)
                && StripQuotes(w.Arguments[1]) is LambdaExpression wl)
            {
                filter = wl;
                cur = w.Arguments[0];
            }

            if (cur is not MemberExpression navMember
                || navMember.Expression is not ParameterExpression
                || !_mapping.Relations.TryGetValue(navMember.Member.Name, out var relation))
                return false;

            // Last/LastOrDefault = First of the reversed ordering.
            if (node.Method.Name is nameof(Queryable.Last) or nameof(Queryable.LastOrDefault))
                for (var i = 0; i < orderings.Count; i++)
                    orderings[i] = (orderings[i].Key, !orderings[i].Ascending);

            var depType = relation.DependentType;

            // Value-converter columns are supported here, matching the ctx.Query correlated path: the emit
            // selects/orders the STORED column, ORDER BY runs on the stored representation (EF-consistent),
            // and when the SELECTED column has a converter the materializer applies ConvertFromProvider to
            // the scalar result — the converter is registered by ComputeProjectionSubqueryConverters, which
            // recognizes this navigation-First shape (see IsNavigationScalarColumnOp).

            var depTable = GetTableName(depType);
            RecordNavReferencedTable(depType);
            var depAlias = _provider.Escape("__nav");

            string RenderChild(LambdaExpression lambda) =>
                TryRenderDependentSelector(lambda.Body, lambda.Parameters[0], depAlias, depType)
                ?? RenderDependentSelectorViaSubVisitor(lambda, depAlias, depType)
                ?? throw new NormUnsupportedFeatureException(
                    "First/FirstOrDefault over a navigation collection could not translate its selector/order key to SQL.");

            var selectorSql = RenderChild(selectorLambda);

            var inner = new StringBuilder();
            inner.Append("SELECT ").Append(selectorSql).Append(" FROM ")
                 .Append(NavigationTableSource(depType, depTable)).Append(' ').Append(depAlias).Append(" WHERE ");
            AppendNavigationRelationPredicate(inner, relation, depAlias, _outerAlias);
            var visibility = _ctx != null ? GlobalFilterFragment.CombineWithTenant(_ctx, depType) : null;
            if (visibility != null)
                inner.Append(" AND ").Append(RenderNavigationFilter(visibility, depAlias));
            if (filter != null)
                inner.Append(" AND ").Append(RenderNavigationFilter(filter, depAlias));
            inner.Append(" ORDER BY ");
            for (var i = 0; i < orderings.Count; i++)
            {
                if (i > 0) inner.Append(", ");
                inner.Append(RenderChild(orderings[i].Key));
                if (!orderings[i].Ascending) inner.Append(" DESC");
            }

            sb.Append(_provider.BuildScalarLimitedSubquery(inner.ToString()));
            return true;
        }

        /// <summary>
        /// The many-to-many analogue of <see cref="TryVisitNavigationOrderedFirstScalar"/>:
        /// <c>p.Tags.[Where(pred).]OrderBy*(key).Select(sel).First/Last()</c> where <c>Tags</c> is an m2m
        /// collection. M2M links live in <see cref="TableMapping.ManyToManyJoins"/>, not
        /// <see cref="TableMapping.Relations"/>, so the relation-based emit above returns false and — without
        /// this — the First fell through to the outer handler, which rendered it as a bogus SQL function
        /// (<c>FIRSTORDEFAULT(...)</c>) and crashed. Emits a LIMIT-1 correlated subquery that joins the bridge
        /// table to the related table, mirroring <see cref="TryVisitManyToManyAggregate"/>. A value-converter
        /// selector is materialized via ConvertFromProvider (registered by ComputeProjectionSubqueryConverters,
        /// which recognizes this shape too); ordering runs on the stored column, EF-consistent.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private bool TryVisitManyToManyOrderedFirstScalar(MethodCallExpression node, StringBuilder sb)
        {
            if (_ctx == null || !IsNavigationOrderedFirstShape(node))
                return false;
            var selCall = (MethodCallExpression)node.Arguments[0];
            var selectorLambda = (LambdaExpression)StripQuotes(selCall.Arguments[1])!;

            var orderings = new List<(LambdaExpression Key, bool Ascending)>();
            Expression cur = selCall.Arguments[0];
            while (cur is MethodCallExpression ord
                && ord.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                    or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending)
                && StripQuotes(ord.Arguments[1]) is LambdaExpression keyLambda)
            {
                orderings.Add((keyLambda, ord.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.ThenBy)));
                cur = ord.Arguments[0];
            }
            orderings.Reverse();

            LambdaExpression? filter = null;
            if (cur is MethodCallExpression w && w.Method.Name == nameof(Queryable.Where)
                && StripQuotes(w.Arguments[1]) is LambdaExpression wl)
            {
                filter = wl;
                cur = w.Arguments[0];
            }

            if (cur is not MemberExpression navMember || navMember.Expression is not ParameterExpression)
                return false;
            var jtm = _mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == navMember.Member.Name);
            if (jtm == null)
                return false;

            // It IS an m2m ordered-First: emit a correct correlated subquery or fail loud — never fall through
            // to the outer handler (which renders First as a nonexistent SQL function). Same guards as the
            // m2m aggregate emit: the bridge is read live, and only single-column bridge keys are handled.
            if (QueryTranslator.HasActiveTemporalScope)
                throw new NormUnsupportedFeatureException(
                    $"First/Last over the many-to-many collection '{navMember.Member.Name}' under AsOf isn't supported yet: the " +
                    "bridge table is read live, so the association set would reflect the current era, not the historical one. " +
                    "Load the collection under AsOf and evaluate it client-side.");
            if (jtm.LeftKeyColumns.Count != 1 || jtm.RightKeyColumns.Count != 1)
                throw new NormUnsupportedFeatureException(
                    $"First/Last over the many-to-many collection '{navMember.Member.Name}' with a composite key isn't supported yet. " +
                    "Materialise the related items and evaluate it client-side.");

            // Last/LastOrDefault = First of the reversed ordering.
            if (node.Method.Name is nameof(Queryable.Last) or nameof(Queryable.LastOrDefault))
                for (var i = 0; i < orderings.Count; i++)
                    orderings[i] = (orderings[i].Key, !orderings[i].Ascending);

            var rightMap = _ctx.GetMapping(jtm.RightType);
            var jtAlias = _provider.Escape("__m2mj");
            var rightAlias = _provider.Escape("__m2mr");
            QueryTranslator.RecordReferencedTable(jtm.TableName);
            QueryTranslator.RecordReferencedTable(rightMap.TableName);
            var ownerKey = jtm.LeftKeyColumns[0];
            var rightKey = jtm.RightKeyColumns[0];

            string RenderRight(LambdaExpression lambda) =>
                TryRenderDependentSelector(lambda.Body, lambda.Parameters[0], rightAlias, jtm.RightType)
                ?? RenderDependentSelectorViaSubVisitor(lambda, rightAlias, jtm.RightType)
                ?? throw new NormUnsupportedFeatureException(
                    $"First/Last over the many-to-many collection '{navMember.Member.Name}' could not translate its " +
                    "selector/order key to SQL. Materialise the related items and evaluate it client-side.");

            var selectorSql = RenderRight(selectorLambda);

            var inner = new StringBuilder();
            inner.Append("SELECT ").Append(selectorSql)
                 .Append(" FROM ").Append(jtm.EscTableName).Append(' ').Append(jtAlias)
                 .Append(" JOIN ").Append(QueryTranslator.TemporalTableSource(rightMap)).Append(' ').Append(rightAlias)
                 .Append(" ON ").Append(rightAlias).Append('.').Append(rightKey.EscCol)
                 .Append(" = ").Append(jtAlias).Append('.').Append(jtm.EscRightFkColumn)
                 .Append(" WHERE ").Append(jtAlias).Append('.').Append(jtm.EscLeftFkColumn)
                 .Append(" = ").Append(_outerAlias).Append('.').Append(ownerKey.EscCol);
            var visibility = GlobalFilterFragment.CombineWithTenant(_ctx, jtm.RightType);
            if (visibility != null)
                inner.Append(" AND ").Append(RenderNavigationFilter(visibility, rightAlias));
            if (filter != null)
                inner.Append(" AND ").Append(RenderNavigationFilter(filter, rightAlias));
            inner.Append(" ORDER BY ");
            for (var i = 0; i < orderings.Count; i++)
            {
                if (i > 0) inner.Append(", ");
                inner.Append(RenderRight(orderings[i].Key));
                if (!orderings[i].Ascending) inner.Append(" DESC");
            }

            sb.Append(_provider.BuildScalarLimitedSubquery(inner.ToString()));
            return true;
        }
    }
}
