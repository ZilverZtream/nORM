using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Mapping;

namespace nORM.Query
{
    /// <summary>
    /// Responsible for constructing SQL JOIN clauses and managing join specific
    /// projection setup. Extracted from <see cref="QueryTranslator"/>.
    /// </summary>
    internal static class JoinBuilder
    {
        public static string BuildJoinClause(
            LambdaExpression? projection,
            TableMapping outerMapping,
            string outerAlias,
            TableMapping innerMapping,
            string innerAlias,
            string joinType,
            string outerKeySql,
            string innerKeySql,
            string? orderBy = null)
        {
            using var joinSql = new OptimizedSqlBuilder(256);

            if (projection?.Body is NewExpression newExpr)
            {
                var neededColumns = ExtractNeededColumns(newExpr, outerMapping, innerMapping, outerAlias, innerAlias);
                if (neededColumns.Count == 0)
                {
                    var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                    joinSql.AppendSelect(System.ReadOnlySpan<char>.Empty);
                    joinSql.AppendJoin(", ", outerCols.Concat(innerCols));
                    joinSql.Append(' ');
                }
                else
                {
                    joinSql.AppendSelect(System.ReadOnlySpan<char>.Empty);
                    joinSql.AppendJoin(", ", neededColumns);
                    joinSql.Append(' ');
                }
            }
            else
            {
                var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                joinSql.AppendSelect(System.ReadOnlySpan<char>.Empty);
                joinSql.AppendJoin(", ", outerCols.Concat(innerCols));
                joinSql.Append(' ');
            }

            joinSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");
            joinSql.Append($"{joinType} {innerMapping.EscTable} {innerAlias} ");
            joinSql.Append($"ON {outerKeySql} = {innerKeySql}");
            if (orderBy != null)
                joinSql.Append($" ORDER BY {orderBy}");

            return joinSql.ToSqlString();
        }

        public static void SetupJoinProjection(
            LambdaExpression? resultSelector,
            TableMapping outerMapping,
            TableMapping innerMapping,
            string outerAlias,
            string innerAlias,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlatedParams,
            ref LambdaExpression? projection)
        {
            projection = resultSelector;
            if (resultSelector != null)
            {
                if (!correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                    correlatedParams[resultSelector.Parameters[0]] = (outerMapping, outerAlias);
                if (!correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                    correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
            }
        }

        public static List<string> ExtractNeededColumns(NewExpression newExpr, TableMapping outerMapping, TableMapping innerMapping, string outerAlias, string innerAlias)
        {
            var neededColumns = new List<string>();

            foreach (var arg in newExpr.Arguments)
            {
                if (arg is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression paramExpr)
                {
                    var isOuterTable = paramExpr.Type == outerMapping.Type;
                    var mapping = isOuterTable ? outerMapping : innerMapping;
                    var alias = isOuterTable ? outerAlias : innerAlias;

                    if (mapping.ColumnsByName.TryGetValue(memberExpr.Member.Name, out var column))
                    {
                        var colSql = $"{alias}.{column.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                }
                else if (arg is ParameterExpression param)
                {
                    var isOuter = param.Type == outerMapping.Type;
                    var mapping = isOuter ? outerMapping : innerMapping;
                    var alias = isOuter ? outerAlias : innerAlias;
                    foreach (var col in mapping.Columns)
                    {
                        var colSql = $"{alias}.{col.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                }
            }

            return neededColumns;
        }


        /// <summary>
        /// Zero-copy variant: appends SELECT/FROM/JOIN directly into the provided builder.
        /// </summary>
        public static void BuildJoinClauseInto(
    OptimizedSqlBuilder joinSql,
    System.Linq.Expressions.LambdaExpression? projection,
    nORM.Mapping.TableMapping outerMapping,
    string outerAlias,
    nORM.Mapping.TableMapping innerMapping,
    string innerAlias,
    string joinType,
    string outerKeySql,
    string innerKeySql,
    string? orderBy = null)
        {
            // Pre-reserve space to minimize buffer growth
            var estimatedSize = 200 + outerMapping.Columns.Length * 25 + innerMapping.Columns.Length * 25;
            joinSql.Reserve(estimatedSize);

            joinSql.AppendSelect(System.ReadOnlySpan<char>.Empty);
            bool wroteAny = false;

            if (projection?.Body is System.Linq.Expressions.NewExpression newExpr)
            {
                var needed = ExtractNeededColumns(newExpr, outerMapping, innerMapping, outerAlias, innerAlias);
                if (needed.Count > 0)
                {
                    // Append already-qualified fragments from needed
                    for (int i = 0; i < needed.Count; i++)
                    {
                        if (i > 0) joinSql.Append(", ");
                        joinSql.Append(needed[i]);
                    }
                    wroteAny = true;
                }
            }

            if (!wroteAny)
            {
                // Fallback: append all columns from both tables without allocations
                bool first = true;
                for (int i = 0; i < outerMapping.Columns.Length; i++)
                {
                    if (!first) joinSql.Append(", ");
                    joinSql.Append(outerAlias).Append('.').Append(outerMapping.Columns[i].EscCol);
                    first = false;
                }
                for (int i = 0; i < innerMapping.Columns.Length; i++)
                {
                    if (!first) joinSql.Append(", ");
                    joinSql.Append(innerAlias).Append('.').Append(innerMapping.Columns[i].EscCol);
                }
            }

            joinSql.Append(' ');
            joinSql.Append("FROM ").Append(outerMapping.EscTable).Append(' ').Append(outerAlias).Append(' ');
            joinSql.Append(joinType).Append(' ').Append(innerMapping.EscTable).Append(' ').Append(innerAlias).Append(' ');
            joinSql.Append("ON ").Append(outerKeySql).Append(" = ").Append(innerKeySql);
            if (!string.IsNullOrEmpty(orderBy))
                joinSql.Append(" ORDER BY ").Append(orderBy!);
        }

    }
}