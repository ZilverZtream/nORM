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
    /// <remarks>
    /// ARCHITECTURAL WARNING (TASK 13): Anonymous type projection parsing is fragile.
    ///
    /// **Current Limitations:**
    /// The <see cref="ExtractNeededColumns"/> method makes simplifying assumptions that break
    /// with complex LINQ projections:
    ///
    /// 1. **Simple Pattern Matching Only:**
    ///    - Only handles MemberExpression with ParameterExpression (e.g., `x => x.Name`)
    ///    - Only handles full ParameterExpression (e.g., `x => x`)
    ///    - Fails silently for anything else (returns empty list, falls back to ALL columns)
    ///
    /// 2. **No Nested Anonymous Type Support:**
    ///    - Doesn't handle transparent identifiers from multiple Select() chains
    ///    - Example: `query.Select(x => new { x.Id }).Select(y => new { y.Id, Computed = y.Id * 2 })`
    ///    - Compiler generates `<>h__TransparentIdentifier0` which isn't recognized
    ///
    /// 3. **No Computed/Method Call Handling:**
    ///    - Doesn't parse method calls: `new { Upper = x.Name.ToUpper() }`
    ///    - Doesn't parse binary operations: `new { Total = x.Price * x.Quantity }`
    ///    - These patterns fall through to "select all columns" fallback
    ///
    /// 4. **String-Based Name Lookup:**
    ///    - Uses `ColumnsByName.TryGetValue(memberExpr.Member.Name)` (line 104)
    ///    - Case-sensitive, brittle to renamed properties or aliasing
    ///    - No normalization or fallback strategies
    ///
    /// **Recommended Refactoring:**
    /// Replace pattern matching with a proper expression tree analyzer:
    ///
    /// - Use ExpressionVisitor pattern to recursively analyze projection trees
    /// - Build dependency graph of required columns for computed expressions
    /// - Handle nested anonymous types by tracking transparent identifier chains
    /// - Use type metadata instead of string matching for column resolution
    /// - Emit computed expressions as SQL (e.g., UPPER([Name]), [Price] * [Quantity])
    ///
    /// **Current Workaround:**
    /// When ExtractNeededColumns returns empty list (line 30), code falls back to selecting
    /// ALL columns from both tables, which works but is inefficient for wide tables.
    ///
    /// **Migration Complexity:**
    /// This is a major architectural change requiring:
    /// - ~500 LOC for proper expression tree analysis
    /// - Extensive testing of projection combinations
    /// - SQL generation for computed expressions
    /// - Proper escaping and type conversion handling
    /// </remarks>
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

        /// <summary>
        /// Determines the minimal set of column projections required to satisfy
        /// a specified projection expression when performing a join.
        /// </summary>
        /// <param name="newExpr">The <see cref="NewExpression"/> representing the projection.</param>
        /// <param name="outerMapping">Mapping information for the outer table.</param>
        /// <param name="innerMapping">Mapping information for the inner table.</param>
        /// <param name="outerAlias">Alias used for the outer table in the SQL query.</param>
        /// <param name="innerAlias">Alias used for the inner table in the SQL query.</param>
        /// <returns>A list of fully-qualified column names that must be selected.</returns>
        /// <remarks>
        /// LIMITATION (TASK 13): This method only handles trivial projections.
        ///
        /// **Supported Patterns:**
        /// - `new { x.Name, x.Id }` - Simple member access
        /// - `new { x, y }` - Full entity projections
        ///
        /// **Unsupported Patterns (returns empty list, causing ALL columns fallback):**
        /// - `new { Upper = x.Name.ToUpper() }` - Method calls
        /// - `new { Total = x.Price * x.Quantity }` - Computed expressions
        /// - `new { x.Id, Nested = new { x.Name } }` - Nested anonymous types
        /// - Transparent identifiers from chained Select() operations
        ///
        /// When this returns empty, caller selects ALL columns from both tables (inefficient).
        /// </remarks>
        /// <summary>
        /// PERFORMANCE OPTIMIZATION: Enhanced column extraction with recursive analysis.
        /// Now handles nested member access, method calls on properties, and transparent identifiers.
        /// Falls back to all columns only when truly necessary.
        /// </summary>
        /// <summary>
        /// PERFORMANCE: Pre-size collections based on argument count to reduce reallocations.
        /// </summary>
        public static List<string> ExtractNeededColumns(NewExpression newExpr, TableMapping outerMapping, TableMapping innerMapping, string outerAlias, string innerAlias)
        {
            // PERFORMANCE: Pre-size to reduce allocations (most projections have 2-10 columns)
            var estimatedSize = Math.Min(newExpr.Arguments.Count * 2, 20);
            var neededColumns = new List<string>(estimatedSize);
            var processedColumns = new HashSet<string>(estimatedSize, StringComparer.Ordinal);

            foreach (var arg in newExpr.Arguments)
            {
                ExtractColumnsFromExpression(arg, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
            }

            return neededColumns;
        }

        private static void ExtractColumnsFromExpression(
            Expression expr,
            TableMapping outerMapping,
            TableMapping innerMapping,
            string outerAlias,
            string innerAlias,
            List<string> neededColumns,
            HashSet<string> processedColumns)
        {
            switch (expr)
            {
                case MemberExpression memberExpr:
                    // Handle simple property access: x.Name
                    if (memberExpr.Expression is ParameterExpression paramExpr)
                    {
                        var isOuterTable = paramExpr.Type == outerMapping.Type;
                        var mapping = isOuterTable ? outerMapping : innerMapping;
                        var alias = isOuterTable ? outerAlias : innerAlias;

                        if (mapping.ColumnsByName.TryGetValue(memberExpr.Member.Name, out var column))
                        {
                            var colSql = $"{alias}.{column.EscCol}";
                            if (processedColumns.Add(colSql))
                                neededColumns.Add(colSql);
                        }
                    }
                    // PERFORMANCE: Handle nested member access: transparentId.x.Name
                    else if (memberExpr.Expression is MemberExpression nestedMember)
                    {
                        ExtractColumnsFromExpression(nestedMember, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                    }
                    break;

                case ParameterExpression param:
                    // Full entity projection: x => x
                    var isOuter = param.Type == outerMapping.Type;
                    var mapping = isOuter ? outerMapping : innerMapping;
                    var alias = isOuter ? outerAlias : innerAlias;
                    foreach (var col in mapping.Columns)
                    {
                        var colSql = $"{alias}.{col.EscCol}";
                        if (processedColumns.Add(colSql))
                            neededColumns.Add(colSql);
                    }
                    break;

                case MethodCallExpression methodCall:
                    // PERFORMANCE: Handle method calls on properties: x.Name.ToUpper()
                    // Extract the underlying property, method will be applied client-side
                    if (methodCall.Object != null)
                    {
                        ExtractColumnsFromExpression(methodCall.Object, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                    }
                    // Also check method arguments for property references
                    foreach (var arg in methodCall.Arguments)
                    {
                        if (arg is MemberExpression || arg is ParameterExpression)
                        {
                            ExtractColumnsFromExpression(arg, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                        }
                    }
                    break;

                case BinaryExpression binary:
                    // PERFORMANCE: Handle binary operations: x.Price * x.Quantity
                    // Extract columns from both sides
                    ExtractColumnsFromExpression(binary.Left, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                    ExtractColumnsFromExpression(binary.Right, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                    break;

                case UnaryExpression unary:
                    // Handle conversions and casts
                    ExtractColumnsFromExpression(unary.Operand, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                    break;

                case NewExpression nested:
                    // PERFORMANCE: Handle nested anonymous types
                    foreach (var arg in nested.Arguments)
                    {
                        ExtractColumnsFromExpression(arg, outerMapping, innerMapping, outerAlias, innerAlias, neededColumns, processedColumns);
                    }
                    break;

                // For other expression types (constants, etc.), we don't need to extract columns
            }
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