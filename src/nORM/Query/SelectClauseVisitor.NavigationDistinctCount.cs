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
        /// Handles <c>p.Children.Select(c =&gt; c.Col).Distinct().Count()</c> — the distinct-count over a
        /// navigation collection — for one-to-many (Relations), many-to-many (ManyToManyJoins) and owned
        /// (OwnedCollections) collections. The scalar-selector form emits a NULL-aware count,
        /// <c>COUNT(DISTINCT col) + CASE WHEN COUNT(*) &gt; COUNT(col) THEN 1 ELSE 0 END</c>, which matches C#
        /// <c>Distinct().Count()</c> for both nullable and non-nullable selectors: SQL <c>COUNT(DISTINCT)</c>
        /// ignores NULLs while C# counts null as one distinct value, so the CASE re-adds the null group iff any
        /// NULL is present (for a non-null column <c>COUNT(*) == COUNT(col)</c>, so it adds 0). The whole-entity
        /// form is <c>COUNT(*)</c> (Distinct is a no-op given the unique key).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private bool TryVisitDistinctCountNavigationAggregate(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Method.Name is not (nameof(Queryable.Count) or nameof(Queryable.LongCount))
                || node.Arguments.Count != 1
                || node.Arguments[0] is not MethodCallExpression distinctCall
                || distinctCall.Method.Name != nameof(Queryable.Distinct)
                || distinctCall.Arguments.Count != 1)
                return false;

            // After Distinct the source is either Select(nav, sel).Distinct() (distinct scalar values) or
            // nav.Distinct() directly (distinct ENTITIES — a no-op given the unique key, so COUNT(*)).
            Expression afterDistinct = distinctCall.Arguments[0];
            LambdaExpression? selectorLambda = null;
            if (afterDistinct is MethodCallExpression selCall
                && selCall.Method.Name == nameof(Queryable.Select)
                && selCall.Arguments.Count == 2
                && StripQuotes(selCall.Arguments[1]) is LambdaExpression sel)
            {
                selectorLambda = sel;
                afterDistinct = selCall.Arguments[0];
            }

            Expression selSource = afterDistinct;
            LambdaExpression? selFilter = null;
            if (selSource is MethodCallExpression preWhere
                && preWhere.Method.Name == nameof(Queryable.Where)
                && preWhere.Arguments.Count == 2
                && StripQuotes(preWhere.Arguments[1]) is LambdaExpression preWhereLambda)
            {
                selFilter = preWhereLambda;
                selSource = preWhere.Arguments[0];
            }

            if (selSource is not MemberExpression selNav
                || selNav.Expression is not ParameterExpression)
                return false;

            // Resolve the collection kind and build the correct correlated FROM/WHERE. m2m links live in
            // ManyToManyJoins and owned collections in OwnedCollections (NOT Relations); without an explicit
            // branch the distinct-count fell through to the outer handler and SILENTLY collapsed to a single
            // global COUNT(DISTINCT) row instead of one per outer row. Each kind fails loud (never silent-wrong)
            // for the sub-shapes its aggregate sibling also declines (AsOf scope, composite keys).
            string fromClause, wherePredicate, elementAlias;
            Type elementType;
            if (_mapping.Relations.TryGetValue(selNav.Member.Name, out var relation))
            {
                elementType = relation.DependentType;
                RecordNavReferencedTable(elementType);
                elementAlias = _provider.Escape("__nav");
                fromClause = $"{NavigationTableSource(elementType, GetTableName(elementType))} {elementAlias}";
                var wsb = new StringBuilder();
                AppendNavigationRelationPredicate(wsb, relation, elementAlias, _outerAlias);
                wherePredicate = wsb.ToString();
            }
            else if (_ctx != null && _mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == selNav.Member.Name) is { } jtm)
            {
                if (QueryTranslator.HasActiveTemporalScope)
                    throw new NormUnsupportedFeatureException(
                        $"Distinct-count over the many-to-many collection '{selNav.Member.Name}' under AsOf isn't supported yet: " +
                        "the bridge table is read live. Load the collection under AsOf and evaluate it client-side.");
                if (jtm.LeftKeyColumns.Count != 1 || jtm.RightKeyColumns.Count != 1)
                    throw new NormUnsupportedFeatureException(
                        $"Distinct-count over the many-to-many collection '{selNav.Member.Name}' with a composite key isn't supported yet. " +
                        "Materialise the related items and evaluate it client-side.");
                var rightMap = _ctx.GetMapping(jtm.RightType);
                var jtAlias = _provider.Escape("__m2mj");
                elementAlias = _provider.Escape("__m2mr");
                elementType = jtm.RightType;
                QueryTranslator.RecordReferencedTable(jtm.TableName);
                QueryTranslator.RecordReferencedTable(rightMap.TableName);
                fromClause = $"{jtm.EscTableName} {jtAlias} JOIN {QueryTranslator.TemporalTableSource(rightMap)} {elementAlias} " +
                             $"ON {elementAlias}.{jtm.RightKeyColumns[0].EscCol} = {jtAlias}.{jtm.EscRightFkColumn}";
                wherePredicate = $"{jtAlias}.{jtm.EscLeftFkColumn} = {_outerAlias}.{jtm.LeftKeyColumns[0].EscCol}";
            }
            else if (_mapping.OwnedCollections.FirstOrDefault(o => o.NavigationProperty.Name == selNav.Member.Name) is { } owned)
            {
                if (QueryTranslator.HasActiveTemporalScope)
                    throw new NormUnsupportedFeatureException(
                        $"Distinct-count over the owned collection '{selNav.Member.Name}' under AsOf isn't supported yet: the aggregate " +
                        "reads the live table. Load the owned collection under AsOf and evaluate it client-side.");
                if (_mapping.KeyColumns.Length != 1)
                    throw new NormUnsupportedFeatureException(
                        $"Distinct-count over the owned collection '{selNav.Member.Name}' on an entity with a composite key isn't supported yet. " +
                        "Materialise the owned items and evaluate it client-side.");
                elementAlias = _provider.Escape("__nav");
                elementType = owned.OwnedType;
                QueryTranslator.RecordReferencedTable(owned.TableName);
                fromClause = $"{owned.EscTable} {elementAlias}";
                wherePredicate = $"{elementAlias}.{owned.EscForeignKeyColumn} = {_outerAlias}.{_mapping.KeyColumns[0].EscCol}";
            }
            else
            {
                return false;
            }

            // COUNT(DISTINCT sel) for a scalar selector; COUNT(*) for distinct entities (no-op by key).
            string countExpr;
            if (selectorLambda != null)
            {
                var selectorSql = TryRenderDependentSelector(selectorLambda.Body, selectorLambda.Parameters[0], elementAlias, elementType)
                    ?? RenderDependentSelectorViaSubVisitor(selectorLambda, elementAlias, elementType)
                    ?? throw new NormUnsupportedFeatureException(
                        "Distinct-count over a navigation collection could not translate its selector to SQL.");
                // NULL-aware: SQL COUNT(DISTINCT) drops NULLs but C# Distinct().Count() counts null as one
                // value. Re-add the null group iff any NULL is present (non-null column => COUNT(*)==COUNT(col)
                // => adds 0), so nullable and reference selectors count correctly instead of undercounting.
                countExpr = $"COUNT(DISTINCT {selectorSql}) + CASE WHEN COUNT(*) > COUNT({selectorSql}) THEN 1 ELSE 0 END";
            }
            else
            {
                countExpr = "COUNT(*)";
            }

            sb.Append('(').Append("SELECT ").Append(countExpr).Append(" FROM ").Append(fromClause).Append(" WHERE ").Append(wherePredicate);
            var visibility = _ctx != null ? GlobalFilterFragment.CombineWithTenant(_ctx, elementType) : null;
            if (visibility != null)
                sb.Append(" AND ").Append(RenderNavigationFilter(visibility, elementAlias));
            if (selFilter != null)
                sb.Append(" AND ").Append(RenderNavigationFilter(selFilter, elementAlias));
            sb.Append(')');
            return true;
        }
    }
}
