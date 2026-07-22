using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SequenceEqualTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments.Count != 2)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual comparer overloads are not provider-mobile; compare materialized sequences in CLR when a custom comparer is required.");
                }

                if (ExtractOrderByKeys(node.Arguments[0]).Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual requires the queryable source to have an explicit OrderBy/ThenBy chain for provider-mobile sequence comparison.");
                }

                if (TryTranslateLocalSequenceEqual(t, node, out var translated))
                    return translated;

                if (ExtractOrderByKeys(node.Arguments[1]).Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual requires the second queryable source to have an explicit OrderBy/ThenBy chain for provider-mobile sequence comparison.");
                }

                var leftPlan = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var leftMapping);
                t.MergeSubPlanParameters(leftPlan);
                var rightPlan = t.TranslateInSubContext(node.Arguments[1], leftMapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var rightMapping);
                t.MergeSubPlanParameters(rightPlan);

                if (leftMapping.Columns.Length != rightMapping.Columns.Length)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual requires both sources to project the same provider-mobile row shape.");
                }

                var leftAlias = t.EscapeAlias("__seql" + t._joinCounter++);
                var rightAlias = t.EscapeAlias("__seqr" + t._joinCounter++);
                var rn = t._provider.Escape("__norm_seq_rn");
                var leftNumbered = BuildNumberedSequence(t, node.Arguments[0], leftPlan.Sql, leftMapping, leftAlias, rn);
                var rightNumbered = BuildNumberedSequence(t, node.Arguments[1], rightPlan.Sql, rightMapping, rightAlias, rn);
                var d1 = t._provider.Escape("__seqd1");
                var d2 = t._provider.Escape("__seqd2");
                var diff = t._provider.Escape("__seqdiff");

                t._sql.Append("SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM (SELECT * FROM (")
                    .Append(leftNumbered).Append(" EXCEPT ").Append(rightNumbered).Append(") AS ").Append(d1)
                    .Append(" UNION ALL SELECT * FROM (")
                    .Append(rightNumbered).Append(" EXCEPT ").Append(leftNumbered).Append(") AS ").Append(d2)
                    .Append(") AS ").Append(diff).Append(") THEN 1 ELSE 0 END");
                t._isAggregate = true;
                t._singleResult = true;
                return node;
            }

            private static bool TryTranslateLocalSequenceEqual(QueryTranslator t, MethodCallExpression node, out Expression translated)
            {
                translated = node;
                if (!TryGetConstantValue(node.Arguments[1], out var rightValue)
                    || rightValue is not System.Collections.IEnumerable rightEnumerable
                    || rightValue is IQueryable)
                {
                    return false;
                }

                var rightRows = new List<object>();
                foreach (var item in rightEnumerable)
                {
                    if (item is null)
                    {
                        throw new NormUnsupportedFeatureException(
                            "SequenceEqual against a local sequence containing null rows is not provider-mobile; compare materialized sequences in CLR.");
                    }
                    rightRows.Add(item);
                }

                var leftPlan = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var leftMapping);
                t.MergeSubPlanParameters(leftPlan);

                if (rightRows.Count == 0)
                {
                    var emptyAlias = t.EscapeAlias("__seqempty" + t._joinCounter++);
                    t._sql.Append("SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM (")
                        .Append(RemoveTrailingOrderBy(node.Arguments[0], leftPlan.Sql))
                        .Append(") AS ").Append(emptyAlias)
                        .Append(") THEN 1 ELSE 0 END");
                    t._isAggregate = true;
                    t._singleResult = true;
                    return true;
                }

                var leftAlias = t.EscapeAlias("__seql" + t._joinCounter++);
                var rn = t._provider.Escape("__norm_seq_rn");
                var leftNumbered = BuildNumberedSequence(t, node.Arguments[0], leftPlan.Sql, leftMapping, leftAlias, rn);
                var rightNumbered = BuildLocalNumberedSequence(t, leftMapping, rightRows, rn);
                var rightWrappedAlias1 = t._provider.Escape("__seqlocal1");
                var rightWrappedAlias2 = t._provider.Escape("__seqlocal2");
                var rightWrapped1 = "SELECT * FROM (" + rightNumbered + ") AS " + rightWrappedAlias1;
                var rightWrapped2 = "SELECT * FROM (" + rightNumbered + ") AS " + rightWrappedAlias2;
                var d1 = t._provider.Escape("__seqd1");
                var d2 = t._provider.Escape("__seqd2");
                var diff = t._provider.Escape("__seqdiff");

                t._sql.Append("SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM (SELECT * FROM (")
                    .Append(leftNumbered).Append(" EXCEPT ").Append(rightWrapped1).Append(") AS ").Append(d1)
                    .Append(" UNION ALL SELECT * FROM (")
                    .Append(rightWrapped2).Append(" EXCEPT ").Append(leftNumbered).Append(") AS ").Append(d2)
                    .Append(") AS ").Append(diff).Append(") THEN 1 ELSE 0 END");
                t._isAggregate = true;
                t._singleResult = true;
                return true;
            }

            private static string BuildLocalNumberedSequence(
                QueryTranslator t,
                TableMapping mapping,
                IReadOnlyList<object> rows,
                string rowNumberAlias)
            {
                var sb = PooledStringBuilder.Rent();
                try
                {
                    for (var rowIndex = 0; rowIndex < rows.Count; rowIndex++)
                    {
                        if (rowIndex > 0)
                            sb.Append(" UNION ALL ");
                        sb.Append("SELECT ").Append((rowIndex + 1).ToString(System.Globalization.CultureInfo.InvariantCulture))
                            .Append(" AS ").Append(rowNumberAlias);
                        foreach (var column in mapping.Columns)
                        {
                            var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
                            t.AddLiteralParameter(pName, column.Prop.GetValue(rows[rowIndex]));
                            sb.Append(", ").Append(pName).Append(" AS ").Append(column.EscCol);
                        }
                    }
                    return sb.ToString();
                }
                finally
                {
                    PooledStringBuilder.Return(sb);
                }
            }

            private static string BuildNumberedSequence(
                QueryTranslator t,
                Expression source,
                string sourceSql,
                TableMapping mapping,
                string alias,
                string rowNumberAlias)
            {
                var orderBy = BuildSequenceOrderBy(t, source, mapping, alias);
                var selectCols = string.Join(", ", mapping.Columns.Select(c => $"{alias}.{c.EscCol} AS {c.EscCol}"));
                var sql = RemoveTrailingOrderBy(source, sourceSql);
                return "SELECT ROW_NUMBER() OVER (ORDER BY " + PooledStringBuilder.JoinOrderBy(orderBy) + ") AS " + rowNumberAlias +
                       ", " + selectCols + " FROM (" + sql + ") AS " + alias;
            }

            private static List<(string col, bool asc)> BuildSequenceOrderBy(
                QueryTranslator t,
                Expression source,
                TableMapping mapping,
                string alias)
            {
                var result = new List<(string col, bool asc)>();
                foreach (var (keyLambda, ascending) in ExtractOrderByKeys(source))
                    result.Add((BuildSql(t, keyLambda.Parameters[0], keyLambda.Body, mapping, alias), ascending));
                return result;
            }

            private static string BuildSql(
                QueryTranslator t,
                ParameterExpression parameter,
                Expression expression,
                TableMapping mapping,
                string alias)
            {
                t._correlatedParams[parameter] = (mapping, alias);
                var vctx = new VisitorContext(t._ctx, mapping, t._provider, parameter, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var sql = visitor.Translate(expression);
                foreach (var kvp in visitor.GetParameters())
                    t._params[kvp.Key] = kvp.Value;
                if (t._params.Count > t._parameterManager.Index)
                    t._parameterManager.Index = t._params.Count;
                FastExpressionVisitorPool.Return(visitor);
                var type = Nullable.GetUnderlyingType(expression.Type) ?? expression.Type;
                return type == typeof(decimal) ? t._provider.ExactDecimalKeySql(sql) : sql;
            }

            private static string RemoveTrailingOrderBy(Expression source, string sql)
            {
                if (SourceHasTakeOrSkip(source))
                    return sql;
                return RemoveTrailingOrderByUnlessPaged(sql);
            }
        }
    }
}
