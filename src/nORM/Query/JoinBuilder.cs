using System;
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
            var visitor = new ColumnExtractionVisitor(outerMapping, innerMapping, outerAlias, innerAlias, estimatedSize);
            visitor.Visit(newExpr);
            return visitor.GetColumns();
        }

        /// <summary>
        /// Expression visitor that walks projection trees to collect required column references.
        /// Handles nested anonymous types, transparent identifiers, and computed expressions.
        /// </summary>
        private sealed class ColumnExtractionVisitor : ExpressionVisitor
        {
            private readonly TableMapping _outerMapping;
            private readonly TableMapping _innerMapping;
            private readonly string _outerAlias;
            private readonly string _innerAlias;
            private readonly List<string> _neededColumns;
            private readonly HashSet<string> _processedColumns;

            public ColumnExtractionVisitor(TableMapping outerMapping, TableMapping innerMapping, string outerAlias, string innerAlias, int estimatedSize)
            {
                _outerMapping = outerMapping;
                _innerMapping = innerMapping;
                _outerAlias = outerAlias;
                _innerAlias = innerAlias;
                _neededColumns = new List<string>(estimatedSize);
                _processedColumns = new HashSet<string>(estimatedSize, StringComparer.Ordinal);
            }

            public List<string> GetColumns() => _neededColumns;

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression is ParameterExpression param)
                {
                    // Direct member access: x => x.Name
                    if (!TryAddMemberColumn(param.Type, node.Member.Name))
                    {
                        // If the member is itself an entity (transparent identifier property)
                        TryAddFullEntity(node.Type);
                    }
                }
                else if (node.Expression is MemberExpression || node.Expression is NewExpression || node.Expression is MemberInitExpression)
                {
                    // Nested member access: transparentId.x.Name or anonymous type chains
                    Visit(node.Expression);
                    // If the leaf resolves to an entity type, include its columns
                    TryAddFullEntity(node.Type);
                }
                else
                {
                    TryAddFullEntity(node.Type);
                }

                return base.VisitMember(node);
            }

            protected override Expression VisitMemberInit(MemberInitExpression node)
            {
                foreach (var binding in node.Bindings.OfType<MemberAssignment>())
                {
                    Visit(binding.Expression);
                }

                return base.VisitMemberInit(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                // Inspect instance and arguments for column references
                if (node.Object != null)
                    Visit(node.Object);

                foreach (var arg in node.Arguments)
                    Visit(arg);

                return node;
            }

            protected override Expression VisitBinary(BinaryExpression node)
            {
                Visit(node.Left);
                Visit(node.Right);
                return node;
            }

            protected override Expression VisitUnary(UnaryExpression node)
            {
                Visit(node.Operand);
                return node;
            }

            protected override Expression VisitNew(NewExpression node)
            {
                foreach (var arg in node.Arguments)
                    Visit(arg);

                return node;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                // Full entity projection: x => x
                TryAddFullEntity(node.Type);
                return node;
            }

            private bool TryAddMemberColumn(Type declaringType, string memberName)
            {
                var mapping = ResolveMapping(declaringType, out var alias);
                if (mapping == null)
                    return false;

                if (mapping.ColumnsByName.TryGetValue(memberName, out var column))
                {
                    AddColumn(alias!, column.EscCol);
                    return true;
                }

                return false;
            }

            private void TryAddFullEntity(Type type)
            {
                var mapping = ResolveMapping(type, out var alias);
                if (mapping == null)
                    return;

                foreach (var col in mapping.Columns)
                    AddColumn(alias!, col.EscCol);
            }

            private TableMapping? ResolveMapping(Type type, out string? alias)
            {
                if (type == _outerMapping.Type)
                {
                    alias = _outerAlias;
                    return _outerMapping;
                }
                if (type == _innerMapping.Type)
                {
                    alias = _innerAlias;
                    return _innerMapping;
                }

                alias = null;
                return null;
            }

            private void AddColumn(string alias, string escCol)
            {
                var colSql = $"{alias}.{escCol}";
                if (_processedColumns.Add(colSql))
                    _neededColumns.Add(colSql);
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