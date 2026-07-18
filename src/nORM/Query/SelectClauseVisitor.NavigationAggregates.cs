using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        private bool TryVisitNavigationAggregate(MethodCallExpression node, StringBuilder sb)
        {
            Expression navCandidate = node.Arguments.Count >= 1 ? node.Arguments[0] : null!;
            LambdaExpression? navFilter = null;
            if (navCandidate is MethodCallExpression whereCall
                && whereCall.Method.Name == nameof(Queryable.Where)
                && whereCall.Arguments.Count == 2
                && StripQuotes(whereCall.Arguments[1]) is LambdaExpression whereLambda)
            {
                navFilter = whereLambda;
                navCandidate = whereCall.Arguments[0];
            }

            if (_ctx != null
                && navCandidate is MethodCallExpression twoHopCall
                && twoHopCall.Method.Name == nameof(Enumerable.SelectMany)
                && twoHopCall.Arguments.Count == 2
                && twoHopCall.Arguments[0] is MemberExpression hop1NavMember
                && hop1NavMember.Expression is ParameterExpression
                && _mapping.Relations.TryGetValue(hop1NavMember.Member.Name, out var hop1Relation)
                && StripQuotes(twoHopCall.Arguments[1]) is LambdaExpression hop2SelectorLambda
                && hop2SelectorLambda.Body is MemberExpression hop2NavMember
                && (node.Method.Name is nameof(Queryable.Count)
                                     or nameof(Queryable.LongCount)
                                     or nameof(Queryable.Any)))
            {
                var intermediateMapping = _ctx.GetMapping(hop1Relation.DependentType);
                QueryTranslator.RecordReferencedTable(intermediateMapping.TableName);
                if (intermediateMapping.Relations.TryGetValue(hop2NavMember.Member.Name, out var hop2Relation))
                {
                    var hop2Filter = node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression hop2Predicate
                        ? hop2Predicate
                        : null;
                    EmitTwoHopNavigationCountSubquery(sb, node.Method.Name, hop1Relation, intermediateMapping, hop2Relation, hop2Filter);
                    return true;
                }
            }

            if (navCandidate is MemberExpression navMember
                && navMember.Expression is ParameterExpression
                && _mapping.Relations.TryGetValue(navMember.Member.Name, out var relation)
                && (node.Method.Name is nameof(Queryable.Count)
                                     or nameof(Queryable.LongCount)
                                     or nameof(Queryable.Any)
                                     or nameof(Queryable.All)))
            {
                if (navFilter == null
                    && node.Arguments.Count == 2
                    && StripQuotes(node.Arguments[1]) is LambdaExpression directPred)
                {
                    navFilter = directPred;
                }

                EmitNavigationCountSubquery(sb, node, relation, navFilter);
                return true;
            }

            if (TryVisitSelectedNavigationScalarAggregate(node, sb))
                return true;

            if (TryVisitDirectNavigationScalarAggregate(node, sb))
                return true;

            if (TryVisitOwnedCollectionAggregate(node, sb))
                return true;

            return TryVisitManyToManyAggregate(node, sb);
        }

        /// <summary>
        /// Parses an aggregate method call whose source is a navigation collection into its parts —
        /// the navigation member, an optional element selector (<c>Select(x =&gt; x.Col)</c>), and an optional
        /// filter (a leading <c>Where(...)</c> or a direct predicate argument). Returns false when the source
        /// isn't a simple <c>param.Nav</c> access. Shared by the owned-collection and many-to-many emit paths.
        /// </summary>
        private static bool TryParseCollectionAggregate(MethodCallExpression node, out MemberExpression navMember,
            out LambdaExpression? selector, out LambdaExpression? filter)
        {
            navMember = null!;
            selector = null;
            filter = null;
            var method = node.Method.Name;
            if (node.Arguments.Count < 1)
                return false;

            var src = node.Arguments[0];
            if (src is MethodCallExpression selCall && selCall.Method.Name == nameof(Queryable.Select)
                && selCall.Arguments.Count == 2 && StripQuotes(selCall.Arguments[1]) is LambdaExpression selLambda)
            {
                selector = selLambda;
                src = selCall.Arguments[0];
            }
            if (src is MethodCallExpression whereCall && whereCall.Method.Name == nameof(Queryable.Where)
                && whereCall.Arguments.Count == 2 && StripQuotes(whereCall.Arguments[1]) is LambdaExpression whereLambda)
            {
                filter = whereLambda;
                src = whereCall.Arguments[0];
            }
            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression argLambda)
            {
                if (method is nameof(Queryable.Sum) or nameof(Queryable.Min) or nameof(Queryable.Max) or nameof(Queryable.Average))
                    selector ??= argLambda;
                else
                    filter ??= argLambda;
            }

            if (src is not MemberExpression member || member.Expression is not ParameterExpression)
                return false;
            navMember = member;
            return true;
        }

        /// <summary>
        /// Handles a projected aggregate over a MANY-TO-MANY collection — <c>p.Tags.Count()</c>,
        /// <c>p.Tags.Any()</c>, <c>p.Tags.Sum(t =&gt; t.Weight)</c>. M2M links live in
        /// <see cref="TableMapping.ManyToManyJoins"/>, not <see cref="TableMapping.Relations"/>, so like owned
        /// collections they matched none of the relation-based paths and fell through to the outer-aggregate
        /// handler, silently collapsing the result to a single wrong row. This emits a correlated subquery
        /// that joins the bridge table to the related table (so the right entity's global/tenant filters
        /// restrict visibility, matching the loaded collection) and FAILS LOUD for the sub-shapes not yet
        /// supported (composite keys, All, non-member selectors) — never silent-wrong.
        /// </summary>
        private bool TryVisitManyToManyAggregate(MethodCallExpression node, StringBuilder sb)
        {
            var method = node.Method.Name;
            if (method is not (nameof(Queryable.Count) or nameof(Queryable.LongCount) or nameof(Queryable.Any)
                               or nameof(Queryable.All) or nameof(Queryable.Sum) or nameof(Queryable.Min)
                               or nameof(Queryable.Max) or nameof(Queryable.Average)))
                return false;
            if (_ctx == null || !TryParseCollectionAggregate(node, out var navMember, out var selector, out var filter))
                return false;

            var jtm = _mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == navMember.Member.Name);
            if (jtm == null)
                return false;

            // It IS a many-to-many aggregate: emit a correct correlated subquery or fail loud — never fall
            // through to the outer-aggregate handler (which silently collapses to one wrong row).
            if (QueryTranslator.HasActiveTemporalScope)
                throw new NormUnsupportedFeatureException(
                    $"Aggregating the many-to-many collection '{navMember.Member.Name}' under AsOf isn't supported yet: the " +
                    "bridge table is read live, so the association set would reflect the current era, not the historical one. " +
                    "Load the collection under AsOf and aggregate the result client-side.");
            if (jtm.LeftKeyColumns.Count != 1 || jtm.RightKeyColumns.Count != 1)
                throw new NormUnsupportedFeatureException(
                    $"Aggregating the many-to-many collection '{navMember.Member.Name}' with a composite key isn't supported yet. " +
                    "Materialise the related items and aggregate them client-side.");
            if (method is nameof(Queryable.All))
                throw new NormUnsupportedFeatureException(
                    $"All(...) over the many-to-many collection '{navMember.Member.Name}' isn't supported yet. " +
                    "Materialise the related items and evaluate it client-side.");

            var rightMap = _ctx.GetMapping(jtm.RightType);
            var jtAlias = _provider.Escape("__m2mj");
            var rightAlias = _provider.Escape("__m2mr");
            QueryTranslator.RecordReferencedTable(jtm.TableName);
            QueryTranslator.RecordReferencedTable(rightMap.TableName);

            var ownerKey = jtm.LeftKeyColumns[0];
            var rightKey = jtm.RightKeyColumns[0];
            // Join the bridge table to the related table so the right entity's visibility filters apply and
            // scalar selectors can reach its columns — mirrors how the loaded m2m collection is built.
            var fromJoin = $"{jtm.EscTableName} {jtAlias} JOIN {QueryTranslator.TemporalTableSource(rightMap)} {rightAlias} " +
                           $"ON {rightAlias}.{rightKey.EscCol} = {jtAlias}.{jtm.EscRightFkColumn}";

            var conds = new List<string> { $"{jtAlias}.{jtm.EscLeftFkColumn} = {_outerAlias}.{ownerKey.EscCol}" };
            var rightVisibility = GlobalFilterFragment.CombineWithTenant(_ctx, jtm.RightType);
            if (rightVisibility != null)
                conds.Add($"({RenderNavigationFilter(rightVisibility, rightAlias)})");
            if (filter != null)
                conds.Add($"({RenderNavigationFilter(filter, rightAlias)})");
            var whereSql = string.Join(" AND ", conds);

            if (method is nameof(Queryable.Count) or nameof(Queryable.LongCount))
            {
                sb.Append("(SELECT COUNT(*) FROM ").Append(fromJoin).Append(" WHERE ").Append(whereSql).Append(')');
                return true;
            }
            if (method is nameof(Queryable.Any))
            {
                sb.Append("(SELECT CASE WHEN EXISTS(SELECT 1 FROM ").Append(fromJoin)
                  .Append(" WHERE ").Append(whereSql).Append(") THEN 1 ELSE 0 END)");
                return true;
            }

            var selBody = selector?.Body;
            while (selBody is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } conv)
                selBody = conv.Operand;
            if (selBody is not MemberExpression selMember || selMember.Expression != selector!.Parameters[0])
                throw new NormUnsupportedFeatureException(
                    $"{method}(...) over the many-to-many collection '{navMember.Member.Name}' requires a simple member selector " +
                    "(e.g. t => t.Weight). Materialise the related items and aggregate them client-side for anything richer.");
            if (!rightMap.ColumnsByName.TryGetValue(selMember.Member.Name, out var col))
                throw new NormUnsupportedFeatureException(
                    $"Aggregate selector member '{selMember.Member.Name}' is not a mapped column on the related entity '{jtm.RightType.Name}'.");

            var sqlAgg = method switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                _ => "AVG",
            };
            var operandSql = $"{rightAlias}.{col.EscCol}";
            var aggType = Nullable.GetUnderlyingType(selector!.Body.Type) ?? selector.Body.Type;
            string aggCall;
            if (aggType == typeof(decimal))
            {
                aggCall = _provider.DecimalAggregateSql(sqlAgg, operandSql);
            }
            else
            {
                if (sqlAgg == "AVG")
                    operandSql = _provider.AverageAggregateOperand(operandSql, selector.Body.Type);
                aggCall = $"{sqlAgg}({operandSql})";
            }
            sb.Append("(SELECT ").Append(aggCall).Append(" FROM ").Append(fromJoin).Append(" WHERE ").Append(whereSql).Append(')');
            return true;
        }

        /// <summary>
        /// Handles a projected aggregate over an OWNED collection (OwnsMany) — <c>o.Lines.Count()</c>,
        /// <c>o.Lines.Any()</c>, <c>o.Lines.Sum(l =&gt; l.Amount)</c> (and Min/Max/Average). Owned collections
        /// live in <see cref="TableMapping.OwnedCollections"/>, NOT <see cref="TableMapping.Relations"/>, so
        /// they never matched the relation-based paths above and fell through to the outer-aggregate handler,
        /// which silently collapsed the whole result to a single wrong row. This emits the correct correlated
        /// subquery against the owned child table, and FAILS LOUD (never silent-wrong) for the sub-shapes not
        /// yet supported here (filtered aggregates, composite owner keys, All, non-member selectors).
        /// </summary>
        private bool TryVisitOwnedCollectionAggregate(MethodCallExpression node, StringBuilder sb)
        {
            var method = node.Method.Name;
            if (method is not (nameof(Queryable.Count) or nameof(Queryable.LongCount) or nameof(Queryable.Any)
                               or nameof(Queryable.All) or nameof(Queryable.Sum) or nameof(Queryable.Min)
                               or nameof(Queryable.Max) or nameof(Queryable.Average)))
                return false;
            if (!TryParseCollectionAggregate(node, out var navMember, out var selector, out var filter))
                return false;

            var owned = _mapping.OwnedCollections.FirstOrDefault(o => o.NavigationProperty.Name == navMember.Member.Name);
            if (owned == null)
                return false;

            // From here it IS an owned-collection aggregate: emit a correct correlated subquery or fail loud —
            // never fall through to the outer-aggregate handler (which silently collapses to one wrong row).
            if (QueryTranslator.HasActiveTemporalScope)
                throw new NormUnsupportedFeatureException(
                    $"Aggregating the owned collection '{navMember.Member.Name}' under AsOf isn't supported yet: the aggregate " +
                    "reads the live table, so it would count the current rows instead of the historical era. Load the owned " +
                    "collection under AsOf and aggregate the result client-side.");
            if (_mapping.KeyColumns.Length != 1)
                throw new NormUnsupportedFeatureException(
                    $"Aggregating the owned collection '{navMember.Member.Name}' on an entity with a composite key isn't supported yet. " +
                    "Materialise the owned items and aggregate them client-side.");
            if (method is nameof(Queryable.All))
                throw new NormUnsupportedFeatureException(
                    $"All(...) over the owned collection '{navMember.Member.Name}' isn't supported yet. " +
                    "Materialise the owned items and evaluate it client-side.");

            var ownerKey = _mapping.KeyColumns[0];
            var depAlias = _provider.Escape("__nav");
            QueryTranslator.RecordReferencedTable(owned.TableName);
            var fkPredicate = $"{depAlias}.{owned.EscForeignKeyColumn} = {_outerAlias}.{ownerKey.EscCol}";
            // A predicate (`Count(l => p)`, `Where(l => p).Sum(...)`) restricts which owned rows the aggregate
            // ranges over. RenderNavigationFilter resolves the owned columns through GetMapping(ownedType) —
            // the same hardened nav-filter grammar the relation path uses, so closures mint @cp params and
            // renamed/converter owned columns bind correctly. A shape the grammar can't render throws there.
            var filterSql = filter != null ? RenderNavigationFilter(filter, depAlias) : null;
            var whereSql = filterSql != null ? $"{fkPredicate} AND ({filterSql})" : fkPredicate;

            if (method is nameof(Queryable.Count) or nameof(Queryable.LongCount))
            {
                sb.Append("(SELECT COUNT(*) FROM ").Append(owned.EscTable).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(whereSql).Append(')');
                return true;
            }
            if (method is nameof(Queryable.Any))
            {
                sb.Append("(SELECT CASE WHEN EXISTS(SELECT 1 FROM ").Append(owned.EscTable).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(whereSql).Append(") THEN 1 ELSE 0 END)");
                return true;
            }

            // Sum / Min / Max / Average with a simple member selector on the owned element.
            var selBody = selector?.Body;
            while (selBody is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } conv)
                selBody = conv.Operand;
            if (selBody is not MemberExpression selMember || selMember.Expression != selector!.Parameters[0])
                throw new NormUnsupportedFeatureException(
                    $"{method}(...) over the owned collection '{navMember.Member.Name}' requires a simple member selector " +
                    "(e.g. l => l.Amount). Materialise the owned items and aggregate them client-side for anything richer.");
            var col = owned.Columns.FirstOrDefault(c => c.Prop.Name == selMember.Member.Name);
            if (col == null)
                throw new NormUnsupportedFeatureException(
                    $"Aggregate selector member '{selMember.Member.Name}' is not a mapped column on the owned collection '{navMember.Member.Name}'.");

            var sqlAgg = method switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                _ => "AVG",
            };
            var operandSql = $"{depAlias}.{col.EscCol}";
            var aggType = Nullable.GetUnderlyingType(selector!.Body.Type) ?? selector.Body.Type;
            string aggCall;
            if (aggType == typeof(decimal))
            {
                aggCall = _provider.DecimalAggregateSql(sqlAgg, operandSql);
            }
            else
            {
                if (sqlAgg == "AVG")
                    operandSql = _provider.AverageAggregateOperand(operandSql, selector.Body.Type);
                aggCall = $"{sqlAgg}({operandSql})";
            }
            sb.Append("(SELECT ").Append(aggCall).Append(" FROM ").Append(owned.EscTable).Append(' ').Append(depAlias)
              .Append(" WHERE ").Append(whereSql).Append(')');
            return true;
        }

        private bool TryVisitSelectedNavigationScalarAggregate(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Arguments.Count != 1
                || node.Method.Name is not (nameof(Queryable.Sum)
                                            or nameof(Queryable.Min)
                                            or nameof(Queryable.Max)
                                            or nameof(Queryable.Average))
                || node.Arguments[0] is not MethodCallExpression selCall
                || selCall.Method.Name != nameof(Queryable.Select)
                || selCall.Arguments.Count != 2
                || StripQuotes(selCall.Arguments[1]) is not LambdaExpression selectorLambda)
            {
                return false;
            }

            Expression selSource = selCall.Arguments[0];
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
                || selNav.Expression is not ParameterExpression
                || !_mapping.Relations.TryGetValue(selNav.Member.Name, out var selRelation))
            {
                return false;
            }

            EmitNavigationScalarAggregateSubquery(sb, node.Method.Name, selRelation, selectorLambda, selFilter);
            return true;
        }

        private bool TryVisitDirectNavigationScalarAggregate(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Arguments.Count != 2
                || node.Method.Name is not (nameof(Queryable.Sum)
                                            or nameof(Queryable.Min)
                                            or nameof(Queryable.Max)
                                            or nameof(Queryable.Average))
                || StripQuotes(node.Arguments[1]) is not LambdaExpression directSelectorLambda)
            {
                return false;
            }

            Expression directSource = node.Arguments[0];
            LambdaExpression? directFilter = null;
            if (directSource is MethodCallExpression directWhere
                && directWhere.Method.Name == nameof(Queryable.Where)
                && directWhere.Arguments.Count == 2
                && StripQuotes(directWhere.Arguments[1]) is LambdaExpression directWhereLambda)
            {
                directFilter = directWhereLambda;
                directSource = directWhere.Arguments[0];
            }

            if (directSource is not MemberExpression directNav
                || directNav.Expression is not ParameterExpression
                || !_mapping.Relations.TryGetValue(directNav.Member.Name, out var directRelation))
            {
                return false;
            }

            EmitNavigationScalarAggregateSubquery(sb, node.Method.Name, directRelation, directSelectorLambda, directFilter);
            return true;
        }

        private void EmitNavigationCountSubquery(StringBuilder sb, MethodCallExpression node, TableMapping.Relation relation, LambdaExpression? extraFilter)
        {
            // Resolve the dependent table mapping from the relation's DependentType. The
            // navigation registration on Relations doesn't carry the dependent TableMapping
            // directly, so look it up via the principal mapping's column lookup (Relations is
            // populated post-build, so we can reach DependentType.Mapping through the
            // ForeignKey property — which IS on the dependent type).
            var depType = relation.DependentType;
            // We need an escaped table identifier and column for the dependent. Since SCV
            // doesn't have a TableMappingProvider, build the SQL identifiers manually from
            // attributes — match the same convention TableMapping uses (table name from
            // [Table] attribute or type name; columns from property names escaped by provider).
            var depTable = GetTableName(depType);
            RecordNavReferencedTable(depType);
            var depAlias = _provider.Escape("__nav");
            // Outer alias / table-name reference for parent columns — set via SCV ctor.
            var outerAlias = _outerAlias;
            var extraFilterSql = extraFilter != null
                ? RenderNavigationFilter(extraFilter, depAlias)
                : null;
            // The dependent's global filters (soft-delete, tenant) restrict which rows are visible to
            // the aggregate. Applied as a plain AND in every branch (including All, where the extra
            // predicate is inverted but visibility is not) so a projected Count/Any/All counts only
            // visible rows instead of leaking filtered-out ones.
            var globalFilter = _ctx != null ? GlobalFilterFragment.CombineWithTenant(_ctx, depType) : null;
            var globalFilterSql = globalFilter != null ? RenderNavigationFilter(globalFilter, depAlias) : null;
            // Use predicate-overload Count(predicate) sugar when the unwrapped filter is the
            // first / only filter and the outer call is Count() — keeps the SQL compact.
            // For Any/All, AND the filter into the EXISTS / NOT EXISTS subquery's WHERE.

            sb.Append('(').Append("SELECT ");
            if (node.Method.Name is nameof(Queryable.Any))
            {
                sb.Append("CASE WHEN EXISTS(SELECT 1 FROM ").Append(NavigationTableSource(depType, depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, relation, depAlias, outerAlias);
                if (globalFilterSql != null) sb.Append(" AND ").Append(globalFilterSql);
                if (extraFilterSql != null) sb.Append(" AND ").Append(extraFilterSql);
                sb.Append(") THEN 1 ELSE 0 END");
            }
            else if (node.Method.Name is nameof(Queryable.All))
            {
                sb.Append("CASE WHEN NOT EXISTS(SELECT 1 FROM ").Append(NavigationTableSource(depType, depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, relation, depAlias, outerAlias);
                if (globalFilterSql != null) sb.Append(" AND ").Append(globalFilterSql);
                // All(p) ≡ NOT EXISTS(visible row matching NOT p) — invert the extra filter, not the
                // global filter (visibility must still restrict the set the predicate ranges over).
                if (extraFilterSql != null) sb.Append(" AND NOT (").Append(extraFilterSql).Append(')');
                sb.Append(") THEN 1 ELSE 0 END");
            }
            else
            {
                sb.Append("COUNT(*) FROM ").Append(NavigationTableSource(depType, depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, relation, depAlias, outerAlias);
                if (globalFilterSql != null) sb.Append(" AND ").Append(globalFilterSql);
                if (extraFilterSql != null) sb.Append(" AND ").Append(extraFilterSql);
            }
            sb.Append(')');
        }

        /// <summary>
        /// FROM source for a navigation subquery's dependent table. When a DbContext is
        /// available this resolves through the authoritative <see cref="TableMapping"/>
        /// (via <see cref="QueryTranslator.TemporalTableSource"/>, which yields the live
        /// <see cref="TableMapping.EscTable"/> normally and the AsOf history window while a
        /// temporal scope is active) — so a fluent <c>ToTable()</c> override, schema, and the
        /// provider's identifier escaping are all honoured. The attribute-derived
        /// <paramref name="tableName"/> is only the fallback when no context is present; it
        /// misses fluent table names and multi-segment schemas, so it must not be used when
        /// the mapping is reachable. This mirrors the two-hop emit, which already routes its
        /// hop tables through <see cref="QueryTranslator.TemporalTableSource"/> unconditionally.
        /// </summary>
        private string NavigationTableSource(Type dependentType, string tableName)
            => _ctx != null
                ? QueryTranslator.TemporalTableSource(_ctx.GetMapping(dependentType))
                : _provider.Escape(tableName);

        private void AppendNavigationRelationPredicate(StringBuilder sb, TableMapping.Relation relation, string dependentAlias, string principalAlias)
        {
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                // Use the mapped column identifiers (EscCol), NOT the CLR property names: a key column
                // renamed via [Column("...")] or fluent HasColumnName otherwise emits `alias.<PropName>`
                // for a column that does not exist. EscCol equals the escaped property name in the common
                // (un-renamed) case, so this is byte-identical there and correct under renaming.
                sb.Append(dependentAlias).Append('.').Append(relation.ForeignKeys[i].EscCol)
                  .Append(" = ").Append(principalAlias).Append('.').Append(relation.PrincipalKeys[i].EscCol);
            }
        }

        private void EmitTwoHopNavigationCountSubquery(
            StringBuilder sb,
            string methodName,
            TableMapping.Relation hop1Rel,
            TableMapping intermediateMapping,
            TableMapping.Relation hop2Rel,
            LambdaExpression? hop2Filter)
        {
            var hop2DepMapping = _ctx!.GetMapping(hop2Rel.DependentType);
            var hop1EscTable  = QueryTranslator.TemporalTableSource(intermediateMapping);
            var hop1Alias     = _provider.Escape("__mhn1");
            var hop2EscTable  = QueryTranslator.TemporalTableSource(hop2DepMapping);
            var hop2Alias     = _provider.Escape("__mhn2");
            var hop2FilterSql = hop2Filter != null
                ? RenderNavigationFilter(hop2Filter, hop2Alias)
                : null;
            // Row visibility (soft-delete global filters, tenant) restricts BOTH hops:
            // an invisible intermediate row must not bridge to visible leaves, and
            // invisible leaves must not count.
            var hop1Visibility = GlobalFilterFragment.CombineWithTenant(_ctx, intermediateMapping.Type) is { } v1
                ? RenderNavigationFilter(v1, hop1Alias)
                : null;
            var hop2Visibility = GlobalFilterFragment.CombineWithTenant(_ctx, hop2Rel.DependentType) is { } v2
                ? RenderNavigationFilter(v2, hop2Alias)
                : null;
            if (hop2Visibility != null)
                hop2FilterSql = hop2FilterSql == null ? hop2Visibility : $"({hop2Visibility}) AND ({hop2FilterSql})";

            if (methodName is nameof(Queryable.Any))
            {
                sb.Append("(SELECT CASE WHEN EXISTS(SELECT 1 FROM ").Append(hop2EscTable).Append(' ').Append(hop2Alias)
                  .Append(" WHERE ");
                if (hop2FilterSql != null)
                    sb.Append(hop2FilterSql).Append(" AND ");
                sb.Append("EXISTS(SELECT 1 FROM ").Append(hop1EscTable).Append(' ').Append(hop1Alias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, hop1Rel, hop1Alias, _outerAlias);
                sb.Append(" AND ");
                AppendNavigationRelationPredicate(sb, hop2Rel, hop2Alias, hop1Alias);
                if (hop1Visibility != null)
                    sb.Append(" AND ").Append(hop1Visibility);
                sb.Append(")) THEN 1 ELSE 0 END)");
            }
            else // Count / LongCount
            {
                sb.Append("(SELECT COUNT(*) FROM ").Append(hop2EscTable).Append(' ').Append(hop2Alias)
                  .Append(" WHERE ");
                if (hop2FilterSql != null)
                    sb.Append(hop2FilterSql).Append(" AND ");
                sb.Append("EXISTS(SELECT 1 FROM ").Append(hop1EscTable).Append(' ').Append(hop1Alias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, hop1Rel, hop1Alias, _outerAlias);
                sb.Append(" AND ");
                AppendNavigationRelationPredicate(sb, hop2Rel, hop2Alias, hop1Alias);
                if (hop1Visibility != null)
                    sb.Append(" AND ").Append(hop1Visibility);
                sb.Append("))");
            }
        }

        /// <summary>
        /// Records a navigation-subquery's dependent table into the ambient referenced-table
        /// scope so a Cacheable query reading it is invalidated when that table is written.
        /// The single-hop nav-aggregate emit resolves its table via [Table]-attribute lookup
        /// (GetTableName), bypassing GetMapping, so it must record the table explicitly.
        /// </summary>
        private void RecordNavReferencedTable(Type dependentType)
        {
            if (_ctx == null) return;
            try { QueryTranslator.RecordReferencedTable(_ctx.GetMapping(dependentType).TableName); }
            catch { /* mapping resolution is best-effort for cache tagging */ }
        }

        private string RenderNavigationFilter(LambdaExpression filter, string depAlias)
            => RenderNavigationFilterBody(filter.Body, filter.Parameters[0], depAlias);

        // Renders a predicate against the dependent alias. Recursive so it covers the common global
        // filter shapes (soft-delete `!c.IsDeleted`, a bare boolean flag, and `&&`/`||` compositions)
        // in addition to the simple `c.X op constant` comparisons.
        private string RenderNavigationFilterBody(Expression body, ParameterExpression elementParam, string depAlias)
        {
            switch (body)
            {
                case UnaryExpression { NodeType: ExpressionType.Not } notExpr:
                    return $"NOT ({RenderNavigationFilterBody(notExpr.Operand, elementParam, depAlias)})";

                // Bare boolean member (`c.IsActive`) → `alias.IsActive = <true>`.
                case MemberExpression boolMember when boolMember.Type == typeof(bool) && boolMember.Expression == elementParam:
                    return $"{RenderFilterSide(boolMember, elementParam, depAlias)} = {_provider.BooleanTrueLiteral}";

                // String match on a column (`c.Sku.StartsWith("A")`, `.EndsWith(...)`, `.Contains(...)`) with
                // a constant pattern — shares the provider case-sensitivity/LIKE-escaping logic with the
                // projection path so the subquery filter matches the same rows the outer query would.
                case MethodCallExpression stringMatch when TryRenderNavStringMatch(stringMatch, elementParam, depAlias, out var matchSql):
                    return matchSql;

                case BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } logical:
                    var logicalOp = logical.NodeType == ExpressionType.AndAlso ? "AND" : "OR";
                    return $"({RenderNavigationFilterBody(logical.Left, elementParam, depAlias)} {logicalOp} {RenderNavigationFilterBody(logical.Right, elementParam, depAlias)})";

                // Null comparison (`c.DeletedAt == null` / `!= null`) must lower to IS [NOT] NULL — the
                // general `col = NULL` form below is always unknown, so the whole subquery filter would match
                // nothing (silently dropping every child, e.g. for a `DeletedAt == null` soft-delete filter).
                case BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual } nullCmp
                    when IsNullConstant(nullCmp.Left) || IsNullConstant(nullCmp.Right):
                    var nullOperand = IsNullConstant(nullCmp.Left) ? nullCmp.Right : nullCmp.Left;
                    var nullTest = nullCmp.NodeType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL";
                    return $"{RenderFilterSide(nullOperand, elementParam, depAlias)} {nullTest}";

                case BinaryExpression be:
                    // If one side is a value-converter column, the OTHER side's value must be converted to the
                    // provider representation, or `col = <rawModelValue>` compares against the wrong stored
                    // value and matches nothing (silent-wrong). Non-converter columns pass null → unchanged.
                    var leftColConv = ColumnConverterFor(be.Left, elementParam);
                    var rightColConv = ColumnConverterFor(be.Right, elementParam);
                    var lhs = RenderFilterSide(be.Left, elementParam, depAlias, valueConverter: rightColConv);
                    var rhs = RenderFilterSide(be.Right, elementParam, depAlias, valueConverter: leftColConv);
                    var op = be.NodeType switch
                    {
                        ExpressionType.Equal => "=",
                        ExpressionType.NotEqual => "<>",
                        ExpressionType.GreaterThan => ">",
                        ExpressionType.GreaterThanOrEqual => ">=",
                        ExpressionType.LessThan => "<",
                        ExpressionType.LessThanOrEqual => "<=",
                        _ => throw new InvalidOperationException(
                            $"Navigation filter binary operator '{be.NodeType}' isn't yet supported in a projection subquery. " +
                            "Use a simple comparison (==, !=, <, >, <=, >=, &&, ||) or wrap with `ClientEvaluationPolicy.Allow`.")
                    };
                    return $"{lhs} {op} {rhs}";

                default:
                    throw new InvalidOperationException(
                        "Navigation filter inside a projection subquery supports comparisons (==, !=, <, >, <=, >=), " +
                        "`IS [NOT] NULL` (`== null` / `!= null`), `StartsWith`/`EndsWith`/`Contains` with a constant " +
                        "pattern, enum comparisons, bare boolean flags, `!`, and `&&`/`||` compositions. Shapes like " +
                        "`list.Contains(col)` (IN) and comparisons on value-converter columns aren't supported here " +
                        "yet — filter after materialization, or wrap with `ClientEvaluationPolicy.Allow`.");
            }
        }

        /// <summary>
        /// Renders a string <c>StartsWith</c>/<c>EndsWith</c>/<c>Contains</c> call on a column of the filter
        /// element with a CONSTANT pattern (optionally with a StringComparison) into a navigation-filter
        /// predicate, reusing <see cref="EmitStringMatch"/>. Returns false for any other shape (variable
        /// pattern, non-element receiver) so the caller falls through to the unsupported-shape error.
        /// </summary>
        /// <summary>
        /// The value converter of the filter-element column the expression accesses, or null when the
        /// expression isn't such a column, has no converter, or can't be resolved. Used to convert the
        /// opposing operand of a comparison to the provider representation.
        /// </summary>
        private IValueConverter? ColumnConverterFor(Expression expr, ParameterExpression elementParam)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            if (_ctx == null || expr is not MemberExpression me || me.Expression != elementParam
                || me.Member.DeclaringType == null)
                return null;
            try
            {
                var map = _ctx.GetMapping(me.Member.DeclaringType);
                return map.ColumnsByName.TryGetValue(me.Member.Name, out var col) ? col.Converter : null;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>True when the expression is a null literal (possibly wrapped in a nullable Convert).</summary>
        private static bool IsNullConstant(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                e = u.Operand;
            return e is ConstantExpression { Value: null };
        }

        private bool TryRenderNavStringMatch(MethodCallExpression mc, ParameterExpression elementParam, string depAlias, out string sql)
        {
            sql = string.Empty;
            if (mc.Object is not MemberExpression receiver || receiver.Expression != elementParam)
                return false;
            if (mc.Method.DeclaringType != typeof(string)
                || mc.Method.Name is not (nameof(string.StartsWith) or nameof(string.EndsWith) or nameof(string.Contains)))
                return false;
            if (!(mc.Arguments.Count == 1 || (mc.Arguments.Count == 2 && mc.Arguments[1].Type == typeof(StringComparison))))
                return false;
            if (!QueryTranslator.TryGetConstantValue(mc.Arguments[0], out var rawPattern) || !(rawPattern is string || rawPattern is char))
                return false;

            var patternStr = rawPattern as string ?? ((char)rawPattern!).ToString();
            var ignoreCase = mc.Arguments.Count == 2
                && QueryTranslator.TryGetConstantValue(mc.Arguments[1], out var cmpVal)
                && cmpVal is StringComparison.OrdinalIgnoreCase
                    or StringComparison.CurrentCultureIgnoreCase
                    or StringComparison.InvariantCultureIgnoreCase;

            sql = EmitStringMatch(RenderFilterSide(receiver, elementParam, depAlias), patternStr, mc.Method.Name, ignoreCase);
            return true;
        }

        private string RenderFilterSide(Expression expr, ParameterExpression elementParam, string depAlias, IValueConverter? valueConverter = null)
        {
            // Peel the enum→underlying Convert the compiler inserts around either operand of an enum
            // comparison (`l.EnumCol == EnumValue`, or `== capturedEnum`) so the plain column / captured
            // value is rendered. A value-converter enum column is fine now: the BinaryExpression case applies
            // the column's converter to the opposing operand, so the comparison uses the stored provider
            // representation rather than the raw underlying number.
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } enumConv
                && (Nullable.GetUnderlyingType(enumConv.Operand.Type) ?? enumConv.Operand.Type).IsEnum)
            {
                expr = enumConv.Operand;
            }

            // Member access on the element parameter → column on the dependent. Resolve the mapped
            // column through the TableMapping (like the selector path RenderElementColumnSql) so a fluent
            // HasColumnName override is honoured — not only a [Column] attribute, which is all the member's
            // own metadata carries. Otherwise a fluently renamed filter column emits `alias.<PropName>` for
            // a column that does not exist. The attribute/property-name path remains the fallback when the
            // mapping isn't reachable, and is byte-identical to the mapping's EscCol in the un-renamed case.
            if (expr is MemberExpression me && me.Expression == elementParam)
            {
                if (_ctx != null && me.Member.DeclaringType != null)
                {
                    try
                    {
                        if (_ctx.GetMapping(me.Member.DeclaringType).ColumnsByName.TryGetValue(me.Member.Name, out var mappedCol))
                            return $"{depAlias}.{mappedCol.EscCol}";
                    }
                    catch { /* fall through to attribute/property-name resolution */ }
                }
                var colAttr = me.Member.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), inherit: false)
                    .Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>().FirstOrDefault();
                var colName = colAttr?.Name ?? me.Member.Name;
                return $"{depAlias}.{_provider.Escape(colName)}";
            }
            // Inline constants are plan-cache-safe literals (the constant's identity is part of
            // the expression fingerprint). When compared against a value-converter column, the constant is
            // converted to the provider representation so `col = <converted>` matches the stored value.
            if (expr is ConstantExpression ce)
                return FormatLiteral(valueConverter != null ? valueConverter.ConvertToProvider(ce.Value) : ce.Value);
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert } u && u.Operand is ConstantExpression ce2)
                return FormatLiteral(valueConverter != null ? valueConverter.ConvertToProvider(ce2.Value) : ce2.Value);
            // CLOSURE captures must NOT literal-ize: plans are cached by fingerprint, so a baked
            // value would freeze the first run's filter into every later run. Emit a compiled
            // parameter through the shared channel when available (same contract as the main
            // projection closures); fall back to the literal only for channel-less fragment uses.
            if (expr is MemberExpression closureMe && QueryTranslator.TryGetConstantValue(closureMe, out var closureVal))
            {
                if (SharedParams != null && SharedCompiledParams != null)
                {
                    var reused = QueryTranslator.TryReuseClosureSlot(closureMe);
                    if (reused != null)
                        return reused;
                    var paramName = $"{_provider.ParamPrefix}cp{SharedCompiledParams.Count}";
                    SharedParams[paramName] = DBNull.Value;
                    SharedCompiledParams.Add(paramName);
                    QueryTranslator.RecordClosureSlot(closureMe, paramName);
                    // Compared against a value-converter column: record the converter so the extractor binds
                    // the provider representation of the captured value, not the raw model value.
                    if (valueConverter != null && SharedParamConverters != null)
                        SharedParamConverters[paramName] = valueConverter;
                    return paramName;
                }
                return FormatLiteral(closureVal);
            }
            throw new InvalidOperationException(
                $"Navigation filter side '{expr}' isn't a simple member access or constant — only `c.X op constant` is supported in a projection subquery.");
        }

        private static string FormatLiteral(object? value)
        {
            // Enums lower to their underlying integer so HasFlag / equality
            // projections work with closure-captured flag locals -- without this
            // the closure-fold path emits the enum boxed and FormatLiteral
            // threw "type '<EnumName>' isn't supported".
            if (value is Enum e)
                value = Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), System.Globalization.CultureInfo.InvariantCulture);
            return value switch
            {
                null => "NULL",
                bool b => b ? "1" : "0",
                string s => $"'{s.Replace("'", "''")}'",
                int or long or short or byte or sbyte or uint or ulong or ushort => value.ToString()!,
                double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
                float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
                decimal m => m.ToString(System.Globalization.CultureInfo.InvariantCulture),
                // DateTime/DateTimeOffset/DateOnly/TimeOnly/TimeSpan/Guid -- emit
                // a single-quoted text literal matching the canonical format
                // Microsoft.Data.Sqlite uses for parameter binding, so the
                // result round-trips through the materializer. DateTime uses
                // 'yyyy-MM-dd HH:mm:ss.FFFFFFF' (variable trailing zeros).
                DateTime dt => $"'{dt.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", System.Globalization.CultureInfo.InvariantCulture)}'",
                DateTimeOffset dto => $"'{dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", System.Globalization.CultureInfo.InvariantCulture)}'",
                DateOnly d => $"'{d.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)}'",
                TimeOnly t => $"'{t.ToString("HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture)}'",
                TimeSpan ts => $"'{ts.ToString("c", System.Globalization.CultureInfo.InvariantCulture)}'",
                Guid g => $"'{g.ToString("D", System.Globalization.CultureInfo.InvariantCulture)}'",
                // CultureInfo / IFormatProvider arguments to ParseExact /
                // TryParse / ToString carry no SQL representation -- the
                // provider's TranslateMethodCall doesn't consume them. Emit
                // NULL so the per-arg projection visit doesn't blow up; the
                // arg never reaches the SQL output because the overload-aware
                // handler ignores it.
                System.Globalization.CultureInfo => "NULL",
                System.IFormatProvider => "NULL",
                _ => throw new InvalidOperationException(
                    $"Navigation filter literal of type '{value.GetType().Name}' isn't supported in a projection subquery. " +
                    "Use int/long/short/byte/string/bool/double/decimal/DateTime/DateTimeOffset/DateOnly/TimeOnly/TimeSpan/Guid, " +
                    "or wrap with `ClientEvaluationPolicy.Allow`.")
            };
        }

        private void EmitNavigationScalarAggregateSubquery(StringBuilder sb, string methodName, TableMapping.Relation relation, LambdaExpression selectorLambda, LambdaExpression? extraFilter = null)
        {
            // Mirror of EmitNavigationCountSubquery — adds aggregate-function dispatch and
            // a per-row selector. Selector is a member access on the dependent element
            // (e.g. `c => c.Amount`); resolve it to the column name via attribute lookup.
            var depType = relation.DependentType;
            var depTable = GetTableName(depType);
            RecordNavReferencedTable(depType);
            var depAlias = _provider.Escape("__nav");

            // Resolve the selector: a member on the dependent element (optionally
            // Convert-wrapped, e.g. `(int?)c.X` for nullable aggregation) or a
            // reference-navigation chain (`c.Dept.Bonus`), rendered as the nested
            // correlated scalar the reference-nav feature uses everywhere else.
            var selectorSql = TryRenderDependentSelector(selectorLambda.Body, selectorLambda.Parameters[0], depAlias, depType)
                ?? throw new InvalidOperationException(
                    "Navigation aggregate Select(c => …).Sum/Min/Max/Average in a projection supports member selectors, " +
                    "including reference-navigation chains (`c => c.Dept.Bonus`) and nullable casts. Computed selectors " +
                    "(e.g. `c => c.A + c.B`) aren't yet routed through the correlated subquery emit — wrap with " +
                    "`ClientEvaluationPolicy.Allow` or aggregate after materialising.");

            var sqlAgg = methodName switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                nameof(Queryable.Average) => "AVG",
                _ => methodName.ToUpperInvariant()
            };

            // Decimal aggregate selectors route through the provider's full-precision hook -
            // sister to the top-level and grouped aggregate paths. AverageAggregateOperand is
            // identity for decimal.
            var aggSelType = Nullable.GetUnderlyingType(selectorLambda.Body.Type) ?? selectorLambda.Body.Type;
            string navAggCall;
            if (aggSelType == typeof(decimal))
            {
                navAggCall = _provider.DecimalAggregateSql(sqlAgg, selectorSql);
            }
            else
            {
                // C# Average over ints is a double; SQL Server's AVG(int) truncates (and the truncated
                // Int32 then fails the double materializer). Same hook as the other aggregate emit
                // paths: cast integral operands to FLOAT there, identity elsewhere.
                if (sqlAgg == "AVG")
                    selectorSql = _provider.AverageAggregateOperand(selectorSql, selectorLambda.Body.Type);
                navAggCall = $"{sqlAgg}({selectorSql})";
            }

            sb.Append('(').Append("SELECT ").Append(navAggCall)
              .Append(" FROM ").Append(NavigationTableSource(depType, depTable)).Append(' ').Append(depAlias)
              .Append(" WHERE ");
            AppendNavigationRelationPredicate(sb, relation, depAlias, _outerAlias);
            // Row visibility (soft-delete global filters, tenant) restricts which
            // dependent rows enter the aggregate — same rule as the Count/Any/All emit.
            var visibility = _ctx != null ? GlobalFilterFragment.CombineWithTenant(_ctx, depType) : null;
            if (visibility != null)
                sb.Append(" AND ").Append(RenderNavigationFilter(visibility, depAlias));
            if (extraFilter != null)
            {
                var filterSql = RenderNavigationFilter(extraFilter, depAlias);
                sb.Append(" AND ").Append(filterSql);
            }
            sb.Append(')');
        }

        /// <summary>
        /// Lowers an explicit-queryable scalar aggregate in a projection —
        /// <c>Select(p =&gt; new { N = ctx.Query&lt;Child&gt;().Count(c =&gt; c.ParentId == p.Id) })</c> —
        /// to a correlated scalar subquery: the sibling of the navigation-collection
        /// aggregate emit and of the predicate-side route in ExpressionToSqlVisitor.
        /// The consumed ctx capture reserves a <c>_ctx_unused</c> alignment slot and the
        /// sub-translation seeds its compiled-parameter names from the shared list so
        /// positional bindings stay aligned.
        /// </summary>
        private bool TryEmitCorrelatedQueryableAggregate(MethodCallExpression node, StringBuilder sb)
        {
            if (_ctx == null || SharedParams == null || SharedCompiledParams == null)
                return false;
            var isFirst = QueryTranslator.IsQueryRootedScalarFirst(node);
            if (!QueryTranslator.IsQueryRootedScalarAggregate(node) && !isFirst)
                return false;

            var methodName = node.Method.Name;
            var source = node.Arguments[0];
            LambdaExpression? lambdaArg = null;
            if (node.Arguments.Count > 1)
            {
                var quoted = node.Arguments[1];
                while (quoted is UnaryExpression { NodeType: ExpressionType.Quote } q) quoted = q.Operand;
                lambdaArg = quoted as LambdaExpression;
                if (lambdaArg == null || lambdaArg.Parameters.Count != 1)
                    return false;
            }

            var elementType = source.Type.IsGenericType ? source.Type.GetGenericArguments()[0] : null;
            if (elementType == null)
                return false;

            // Compose the predicate/selector into the source so the sub-translation
            // binds it against the subquery's own alias.
            if (lambdaArg != null)
            {
                // Count/First/Last carry a PREDICATE lambda (append as Where); Sum/Min/Max/Average
                // carry a SELECTOR lambda (append as Select).
                source = methodName is nameof(Queryable.Count) or nameof(Queryable.LongCount)
                        or nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                        or nameof(Queryable.Last) or nameof(Queryable.LastOrDefault)
                    ? Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(lambdaArg))
                    : Expression.Call(typeof(Queryable), nameof(Queryable.Select), new[] { elementType, lambdaArg.Body.Type }, source, Expression.Quote(lambdaArg));
            }

            // Last/LastOrDefault = First of the reversed ordering; fail closed without an ordering.
            if (methodName is nameof(Queryable.Last) or nameof(Queryable.LastOrDefault))
            {
                source = QueryTranslator.ReverseQueryableOrderings(source, out var hadOrdering);
                if (!hadOrdering)
                    throw new NormUnsupportedFeatureException(
                        $"{methodName}() over a correlated subquery requires an OrderBy — 'last' is undefined without an ordering.");
            }

            ReserveQueryRootClosureSlot(node.Arguments[0]);
            // Global/tenant filters must reach the subquery root — the provider's
            // top-level rewrite never descends into projection lambdas, and an
            // unfiltered subquery aggregates ACROSS tenants.
            source = QueryTranslator.ApplySubqueryRootFilters(_ctx, source, out var foldedFilterValues);
            if (foldedFilterValues)
            {
                var placeholder = $"{_provider.ParamPrefix}cp{SharedCompiledParams.Count}_gf_unused";
                SharedParams[placeholder] = DBNull.Value;
                SharedCompiledParams.Add(placeholder);
            }
            source = ExpressionToSqlVisitor.QueryCallMaterializer.Materialize(source);

            var mapping = _ctx.GetMapping(elementType);
            var correlated = new Dictionary<ParameterExpression, (nORM.Mapping.TableMapping Mapping, string Alias)>();
            var freeCollector = new FreeParameterCollector();
            freeCollector.Visit(node);
            foreach (var free in freeCollector.Free)
            {
                // A free DbContext parameter (compiled queries) is the Query receiver,
                // not a row reference — never a correlation target.
                if (typeof(DbContext).IsAssignableFrom(free.Type))
                    continue;
                // A compiled query's free VALUE parameter is not a row reference either:
                // correlating it maps it to the outer alias, where a bare parameter
                // renders as nothing and corrupts the SQL. Leave it free so the
                // sub-translation binds it as a compiled parameter slot.
                if (OuterRowParameters != null && !OuterRowParameters.Contains(free))
                    continue;
                correlated[free] = (_mapping, _outerAlias);
            }

            // Both dicts seed with the outer's entries so @p numbering (derived from
            // _params.Count inside the sub-translation's visitor contexts) and @cp
            // numbering continue globally — a restarted name collides with an outer
            // slot and the name-deduping copy-back merges two distinct values into
            // one parameter. Seeds copy back onto themselves.
            var tempParams = new Dictionary<string, object>(SharedParams);
            var tempCompiled = new List<string>(SharedCompiledParams);
            using var sub = QueryTranslator.Create(_ctx, mapping, tempParams, SharedParams.Count, correlated,
                new HashSet<string>(), tempCompiled, new Dictionary<ParameterExpression, string>(), correlated.Count, recursionDepth: 1);
            var subPlan = sub.Translate(source);
            foreach (var kvp in tempParams)
                SharedParams[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!SharedCompiledParams.Contains(compiled))
                    SharedCompiledParams.Add(compiled);
            }
            // Merge value-converter registrations the sub-translation minted for
            // closure values compared against converter columns inside the
            // subquery (e.g. `s.Region == capturedRegion`). Without this the plan
            // binds the raw CLR value instead of the provider representation and
            // the comparison silently matches nothing.
            if (subPlan.ParameterConverters is { Count: > 0 } && SharedParamConverters != null)
            {
                foreach (var kvp in subPlan.ParameterConverters)
                    SharedParamConverters[kvp.Key] = kvp.Value;
            }

            var sql = subPlan.Sql;
            var fromIdx = ExpressionToSqlVisitor.FindTopLevelFromIndex(sql);
            if (fromIdx < 0)
                return false;
            var head = sql.Substring("SELECT ".Length, fromIdx - "SELECT ".Length).Trim();
            var tail = sql.Substring(fromIdx);
            var orderIdx = tail.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderIdx >= 0)
                tail = tail.Substring(0, orderIdx);

            if (isFirst)
            {
                // First/FirstOrDefault: keep the ORDER BY (unlike the aggregate slice) so the
                // selected row is deterministic and limit to one row. The source must project
                // a single scalar; otherwise fall through to the client-eval diagnostic.
                var firstHead = head.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase)
                    ? head.Substring("DISTINCT ".Length).Trim()
                    : head;
                if (ExpressionToSqlVisitor.HasTopLevelComma(firstHead))
                    return false;
                sb.Append(_provider.BuildScalarLimitedSubquery(sql));
                return true;
            }

            if (methodName is nameof(Queryable.Count) or nameof(Queryable.LongCount))
            {
                var operand = "*";
                if (head.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
                {
                    var projected = head.Substring("DISTINCT ".Length).Trim();
                    if (!ExpressionToSqlVisitor.HasTopLevelComma(projected))
                        operand = "DISTINCT " + projected;
                }
                sb.Append("(SELECT COUNT(").Append(operand).Append(')').Append(tail).Append(')');
                return true;
            }

            var distinct = false;
            if (head.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
            {
                distinct = true;
                head = head.Substring("DISTINCT ".Length).Trim();
            }
            if (ExpressionToSqlVisitor.HasTopLevelComma(head))
                return false;

            var operandType = lambdaArg?.Body.Type ?? elementType;
            var aggName = methodName switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                _ => "AVG",
            };
            var underlying = Nullable.GetUnderlyingType(operandType) ?? operandType;
            string scalarAggCall;
            if (underlying == typeof(decimal))
            {
                // Full-precision decimal hook - sister to the other aggregate emit paths.
                scalarAggCall = _provider.DecimalAggregateSql(aggName, head, distinct);
            }
            else
            {
                if (aggName == "AVG")
                    head = _provider.AverageAggregateOperand(head, operandType);
                scalarAggCall = $"{aggName}({(distinct ? "DISTINCT " : string.Empty)}{head})";
            }

            sb.Append("(SELECT ").Append(scalarAggCall).Append(tail).Append(')');
            return true;
        }

        private void ReserveQueryRootClosureSlot(Expression source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                if (mce.Method.Name == "Query")
                {
                    var receiver = mce.Object ?? (mce.Arguments.Count > 0 ? mce.Arguments[0] : null);
                    if (receiver is MemberExpression receiverMember
                        && QueryTranslator.TryGetConstantValue(receiverMember, out _))
                    {
                        ReserveUnusedSharedSlot();
                    }
                    return;
                }
                if (mce.Arguments.Count == 0) return;
                current = mce.Arguments[0];
            }
            if (current is MemberExpression root
                && typeof(IQueryable).IsAssignableFrom(root.Type)
                && QueryTranslator.TryGetConstantValue(root, out _))
            {
                ReserveUnusedSharedSlot();
            }
        }

        private void ReserveUnusedSharedSlot()
        {
            var placeholder = $"{_provider.ParamPrefix}cp{SharedCompiledParams!.Count}_ctx_unused";
            if (SharedParams != null) SharedParams[placeholder] = DBNull.Value;
            SharedCompiledParams.Add(placeholder);
        }

        private sealed class FreeParameterCollector : ExpressionVisitor
        {
            private readonly HashSet<ParameterExpression> _bound = new();
            public HashSet<ParameterExpression> Free { get; } = new();

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                foreach (var p in node.Parameters) _bound.Add(p);
                return base.VisitLambda(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (!_bound.Contains(node)) Free.Add(node);
                return node;
            }
        }

        /// <summary>
        /// Renders an aggregate-selector member against the aggregated element: a bare
        /// column (resolved via the element's mapping, so [Column] renames apply), or a
        /// reference-navigation chain of any depth emitted as nested correlated scalars
        /// (the same shape the reference-nav feature uses in projections). Convert
        /// wrappers (nullable casts) are transparent. Returns null for computed
        /// selectors, which the caller reports.
        /// </summary>
        private string? TryRenderDependentSelector(Expression body, ParameterExpression elementParam, string elementAlias, Type elementType)
        {
            while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                body = convert.Operand;
            if (body is not MemberExpression member)
                return null;

            TableMapping elementMapping;
            if (_ctx == null)
                return null;
            try
            {
                elementMapping = _ctx.GetMapping(elementType);
            }
            catch
            {
                return null;
            }

            return RenderElementColumnSql(member, elementParam, elementAlias, elementMapping, depth: 0);
        }

        private string? RenderElementColumnSql(MemberExpression member, ParameterExpression elementParam, string elementAlias, TableMapping elementMapping, int depth)
        {
            if (depth > 4)
                return null;

            var owner = member.Expression;
            while (owner is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                owner = convert.Operand;

            if (owner == elementParam)
            {
                return elementMapping.ColumnsByName.TryGetValue(member.Member.Name, out var column)
                    ? $"{elementAlias}.{column.EscCol}"
                    : null;
            }

            // A navigation hop: the member lives on a principal reached through the owner
            // chain. Emit `(SELECT p.Col FROM Principal p WHERE p.PK = <fk value>)` where
            // the FK value renders recursively against the element (nested chains nest
            // the subqueries, mirroring the reference-nav scalar emit).
            if (owner is not MemberExpression navMember || _ctx == null)
                return null;
            var principalType = navMember.Type;
            if (!principalType.IsClass || principalType == typeof(string))
                return null;
            TableMapping principalMap;
            try
            {
                principalMap = _ctx.GetMapping(principalType);
            }
            catch
            {
                return null;
            }
            if (principalMap.KeyColumns.Length != 1)
                return null;
            if (!principalMap.ColumnsByName.TryGetValue(member.Member.Name, out var targetColumn))
                return null;

            var navOwner = navMember.Expression;
            while (navOwner is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                navOwner = convert.Operand;
            TableMapping navOwnerMapping;
            if (navOwner == elementParam)
            {
                navOwnerMapping = elementMapping;
            }
            else if (navOwner is MemberExpression deeperNav && deeperNav.Type.IsClass && deeperNav.Type != typeof(string))
            {
                try
                {
                    navOwnerMapping = _ctx.GetMapping(deeperNav.Type);
                }
                catch
                {
                    return null;
                }
            }
            else
            {
                return null;
            }

            var fk = ExpressionToSqlVisitor.FindReferenceNavForeignKey(navOwnerMapping, navMember.Member.Name, principalType, principalMap);
            if (fk == null)
                return null;

            var fkMember = Expression.MakeMemberAccess(navOwner == elementParam ? elementParam : navOwner, fk.Prop);
            string? fkValueSql;
            if (navOwner == elementParam)
            {
                fkValueSql = $"{elementAlias}.{fk.EscCol}";
            }
            else
            {
                fkValueSql = RenderElementColumnSql((MemberExpression)fkMember, elementParam, elementAlias, elementMapping, depth + 1);
            }
            if (fkValueSql == null)
                return null;

            var principalAlias = _provider.Escape("__navp" + depth.ToString(System.Globalization.CultureInfo.InvariantCulture));
            // Same visibility rule as the scalar nav emits: a filtered-out principal
            // reads as missing, so its values never enter the aggregate.
            string? principalFilterSql = null;
            var combinedFilter = GlobalFilterFragment.CombineWithTenant(_ctx!, principalMap.Type);
            if (combinedFilter != null)
                principalFilterSql = RenderNavigationFilter(combinedFilter, principalAlias);
            return $"(SELECT {principalAlias}.{targetColumn.EscCol} FROM {QueryTranslator.TemporalTableSource(principalMap)} {principalAlias} " +
                   $"WHERE {principalAlias}.{principalMap.KeyColumns[0].EscCol} = {fkValueSql}" +
                   (principalFilterSql != null ? $" AND {principalFilterSql}" : string.Empty) + ")";
        }
    }
}
