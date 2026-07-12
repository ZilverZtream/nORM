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

            return TryVisitDirectNavigationScalarAggregate(node, sb);
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
            var globalFilter = _ctx != null ? GlobalFilterFragment.Combine(_ctx, depType) : null;
            var globalFilterSql = globalFilter != null ? RenderNavigationFilter(globalFilter, depAlias) : null;
            // Use predicate-overload Count(predicate) sugar when the unwrapped filter is the
            // first / only filter and the outer call is Count() — keeps the SQL compact.
            // For Any/All, AND the filter into the EXISTS / NOT EXISTS subquery's WHERE.

            sb.Append('(').Append("SELECT ");
            if (node.Method.Name is nameof(Queryable.Any))
            {
                sb.Append("CASE WHEN EXISTS(SELECT 1 FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, relation, depAlias, outerAlias);
                if (globalFilterSql != null) sb.Append(" AND ").Append(globalFilterSql);
                if (extraFilterSql != null) sb.Append(" AND ").Append(extraFilterSql);
                sb.Append(") THEN 1 ELSE 0 END");
            }
            else if (node.Method.Name is nameof(Queryable.All))
            {
                sb.Append("CASE WHEN NOT EXISTS(SELECT 1 FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
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
                sb.Append("COUNT(*) FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ");
                AppendNavigationRelationPredicate(sb, relation, depAlias, outerAlias);
                if (globalFilterSql != null) sb.Append(" AND ").Append(globalFilterSql);
                if (extraFilterSql != null) sb.Append(" AND ").Append(extraFilterSql);
            }
            sb.Append(')');
        }

        private void AppendNavigationRelationPredicate(StringBuilder sb, TableMapping.Relation relation, string dependentAlias, string principalAlias)
        {
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                sb.Append(dependentAlias).Append('.').Append(_provider.Escape(relation.ForeignKeys[i].Prop.Name))
                  .Append(" = ").Append(principalAlias).Append('.').Append(_provider.Escape(relation.PrincipalKeys[i].Prop.Name));
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
            var hop1EscTable  = intermediateMapping.EscTable;
            var hop1Alias     = _provider.Escape("__mhn1");
            var hop2EscTable  = hop2DepMapping.EscTable;
            var hop2Alias     = _provider.Escape("__mhn2");
            var hop2FilterSql = hop2Filter != null
                ? RenderNavigationFilter(hop2Filter, hop2Alias)
                : null;

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
                sb.Append("))");
            }
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

                case BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } logical:
                    var logicalOp = logical.NodeType == ExpressionType.AndAlso ? "AND" : "OR";
                    return $"({RenderNavigationFilterBody(logical.Left, elementParam, depAlias)} {logicalOp} {RenderNavigationFilterBody(logical.Right, elementParam, depAlias)})";

                case BinaryExpression be:
                    var lhs = RenderFilterSide(be.Left, elementParam, depAlias);
                    var rhs = RenderFilterSide(be.Right, elementParam, depAlias);
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
                        "Navigation filter inside a projection subquery supports comparisons, `!`, bare boolean flags, " +
                        "and `&&`/`||` compositions. For more complex shapes, wrap with `ClientEvaluationPolicy.Allow`.");
            }
        }

        private string RenderFilterSide(Expression expr, ParameterExpression elementParam, string depAlias)
        {
            // Member access on the element parameter → column on the dependent.
            if (expr is MemberExpression me && me.Expression == elementParam)
            {
                var colAttr = me.Member.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), inherit: false)
                    .Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>().FirstOrDefault();
                var colName = colAttr?.Name ?? me.Member.Name;
                return $"{depAlias}.{_provider.Escape(colName)}";
            }
            // Constant or closure-captured value → literal-ize. The projection-subquery
            // emit doesn't have access to the parameter manager here, so inline as a SQL
            // literal — only handles simple ints, doubles, strings, bools. Anything else
            // falls through to the throw.
            if (expr is ConstantExpression ce)
                return FormatLiteral(ce.Value);
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert } u && u.Operand is ConstantExpression ce2)
                return FormatLiteral(ce2.Value);
            if (expr is MemberExpression closureMe && QueryTranslator.TryGetConstantValue(closureMe, out var closureVal))
                return FormatLiteral(closureVal);
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
            var depAlias = _provider.Escape("__nav");

            // Resolve the selector to a column name. Only a simple member access is supported
            // here (matching efba58f scope) — `c => c.X` not `c => c.X + 1`.
            string selectorSql;
            if (selectorLambda.Body is MemberExpression me)
            {
                var colAttr = me.Member.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), inherit: false)
                    .Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>().FirstOrDefault();
                var colName = colAttr?.Name ?? me.Member.Name;
                selectorSql = $"{depAlias}.{_provider.Escape(colName)}";
            }
            else
            {
                throw new InvalidOperationException(
                    "Navigation aggregate Select(c => …).Sum/Min/Max/Average in a projection currently supports only a bare member access selector (`c => c.X`). " +
                    "Computed selectors (e.g. `c => c.A + c.B`) aren't yet routed through the correlated subquery emit — wrap with `ClientEvaluationPolicy.Allow` or aggregate after materialising.");
            }

            var sqlAgg = methodName switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                nameof(Queryable.Average) => "AVG",
                _ => methodName.ToUpperInvariant()
            };

            // Decimal columns store as TEXT in SQLite; SQL aggregates inherit
            // storage class so MIN/MAX/SUM/AVG would lex-compare mixed-
            // magnitude values. Coerce to REAL so the inner aggregate uses
            // numeric semantics. Sister to HandleDirectAggregate (2002200)
            // and OrderBy (c8b8c6b) -- same precision-tradeoff caveat
            // documented at those sites.
            var aggSelType = Nullable.GetUnderlyingType(selectorLambda.Body.Type) ?? selectorLambda.Body.Type;
            if (aggSelType == typeof(decimal))
            {
                selectorSql = _provider.NormalizeDecimalForCompare(selectorSql);
            }

            sb.Append('(').Append("SELECT ").Append(sqlAgg).Append('(').Append(selectorSql).Append(')')
              .Append(" FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
              .Append(" WHERE ");
            AppendNavigationRelationPredicate(sb, relation, depAlias, _outerAlias);
            if (extraFilter != null)
            {
                var filterSql = RenderNavigationFilter(extraFilter, depAlias);
                sb.Append(" AND ").Append(filterSql);
            }
            sb.Append(')');
        }
    }
}
