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

            List<string> neededColumns = null!;
            if (projection?.Body is NewExpression newExpr)
            {
                neededColumns = ExtractNeededColumns(newExpr, outerMapping, innerMapping, outerAlias, innerAlias);
            }
            else if (projection?.Body is MemberInitExpression initExpr)
            {
                neededColumns = ExtractNeededColumns(initExpr, outerMapping, innerMapping, outerAlias, innerAlias);
            }

            if (projection != null && neededColumns.Count > 0)
            {
                joinSql.AppendSelect(System.ReadOnlySpan<char>.Empty);
                joinSql.InnerBuilder.AppendJoin(", ", neededColumns);
                joinSql.Append(' ');
            }
            else
            {
                var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                joinSql.AppendSelect(System.ReadOnlySpan<char>.Empty);
                joinSql.InnerBuilder.AppendJoin(", ", outerCols.Concat(innerCols));
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
            => ExtractNeededColumns(newExpr.Arguments, outerMapping, innerMapping, outerAlias, innerAlias);

        public static List<string> ExtractNeededColumns(MemberInitExpression initExpr, TableMapping outerMapping, TableMapping innerMapping, string outerAlias, string innerAlias)
        {
            var exprs = initExpr.Bindings.OfType<MemberAssignment>().Select(b => b.Expression);
            return ExtractNeededColumns(exprs, outerMapping, innerMapping, outerAlias, innerAlias);
        }

        private static List<string> ExtractNeededColumns(IEnumerable<Expression> args, TableMapping outerMapping, TableMapping innerMapping, string outerAlias, string innerAlias)
        {
            var neededColumns = new List<string>();

            foreach (var arg in args)
            {
                if (arg is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression paramExpr)
                {
                    // Determine which table this parameter refers to
                    TableMapping mapping;
                    string alias;

                    if (paramExpr.Type == outerMapping.Type)
                    {
                        mapping = outerMapping;
                        alias = outerAlias;
                    }
                    else if (paramExpr.Type == innerMapping.Type)
                    {
                        mapping = innerMapping;
                        alias = innerAlias;
                    }
                    else
                    {
                        // Skip if we can't determine the table
                        continue;
                    }

                    if (mapping.ColumnsByName.TryGetValue(memberExpr.Member.Name, out var column))
                    {
                        var colSql = $"{alias}.{column.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                    else
                    {
                        // If column not found, this might be a computed or unmapped property
                        // Log warning and skip
                        System.Diagnostics.Debug.WriteLine($"Warning: Column '{memberExpr.Member.Name}' not found in mapping for type '{mapping.Type.Name}'");
                    }
                }
                else if (arg is ParameterExpression param)
                {
                    // This represents selecting all columns from a table
                    TableMapping mapping;
                    string alias;

                    if (param.Type == outerMapping.Type)
                    {
                        mapping = outerMapping;
                        alias = outerAlias;
                    }
                    else if (param.Type == innerMapping.Type)
                    {
                        mapping = innerMapping;
                        alias = innerAlias;
                    }
                    else
                    {
                        continue;
                    }

                    foreach (var col in mapping.Columns)
                    {
                        var colSql = $"{alias}.{col.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                }
                else if (arg is ConstantExpression || arg is UnaryExpression)
                {
                    // Constants or conversions don't add columns
                    continue;
                }
                else
                {
                    // For complex expressions, we might need all columns
                    // This is a safe fallback but not optimal
                    foreach (var col in outerMapping.Columns)
                    {
                        var colSql = $"{outerAlias}.{col.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                    foreach (var col in innerMapping.Columns)
                    {
                        var colSql = $"{innerAlias}.{col.EscCol}";
                        if (!neededColumns.Contains(colSql))
                            neededColumns.Add(colSql);
                    }
                }
            }

            return neededColumns;
        }
    }
}
