using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using System.Collections.Frozen;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        /// <summary>
        /// Emits a correlated scalar subquery for the
        /// `parent.Children.Select(c =&gt; c.Value).Sum/Min/Max/Average()` shape:
        /// <code>(SELECT AGG(selector_sql) FROM ChildTable T_n WHERE T_n.FK = T_outer.PK)</code>
        /// The selector lambda is translated against the child mapping in a sub-visitor so
        /// arbitrary projection expressions (computed selectors, COALESCE chains, etc.) flow
        /// through naturally.
        /// </summary>
        private void EmitNavigationScalarAggregateSubquery(
            string aggregateName,
            ParameterExpression parentParam,
            (TableMapping Mapping, string Alias) parentInfo,
            TableMapping.Relation relation,
            LambdaExpression selectorLambda)
        {
            var sqlAgg = aggregateName switch
            {
                nameof(Queryable.Sum)     => "SUM",
                nameof(Queryable.Min)     => "MIN",
                nameof(Queryable.Max)     => "MAX",
                nameof(Queryable.Average) => "AVG",
                _ => throw new NormQueryException($"Unexpected navigation aggregate '{aggregateName}'.")
            };

            var childMapping = _ctx.GetMapping(relation.DependentType);
            // Escape the alias BEFORE handing it to the sub-visitor: PostgreSQL folds unquoted
            // identifiers to lowercase, so a selector emitting the raw alias would not match the
            // quoted alias in the FROM clause ("missing FROM-clause entry").
            var subAlias = _provider.Escape($"T_nav_{Guid.NewGuid().ToString("N").Substring(0, 8)}");

            // Translate the selector lambda against the child mapping. Routing through a
            // dedicated child-parameter binding lets nested member access / arithmetic /
            // COALESCE all flow through the regular expression-to-SQL pipeline.
            using var subVisitor = new ExpressionToSqlVisitor();
            var subCtx = new VisitorContext(
                _ctx, childMapping, _provider,
                selectorLambda.Parameters[0], subAlias,
                _parameterMappings, _compiledParams, _paramConverters, _paramMap, _recursionDepth + 1, _paramIndex);
            subVisitor.Initialize(in subCtx);
            subVisitor.UseSharedParameterDictionary(_paramSink);
            var selectorSql = subVisitor.Translate(selectorLambda.Body);
            _paramIndex = subVisitor.ParamIndex;

            // C# Average over ints is a double; SQL Server's AVG(int) truncates to int, so its
            // provider hook casts integral operands to FLOAT (identity elsewhere) — same hook as
            // the other aggregate emit paths.
            if (sqlAgg == "AVG")
                selectorSql = _provider.AverageAggregateOperand(selectorSql, selectorLambda.Body.Type);

            _sql.Append("(SELECT ").Append(sqlAgg).Append('(').Append(selectorSql).Append(')')
                .Append(" FROM ").Append(childMapping.EscTable).Append(' ').Append(subAlias)
                .Append(" WHERE ");
            AppendRelationPredicate(_sql, relation, subAlias, parentInfo.Alias);
            _sql.Append(')');
        }

        private static void AppendRelationPredicate(OptimizedSqlBuilder sql, TableMapping.Relation relation, string childAlias, string parentAlias)
        {
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                if (i > 0)
                    sql.Append(" AND ");
                sql.Append(childAlias).Append('.').Append(relation.ForeignKeys[i].EscCol)
                    .Append(" = ")
                    .Append(parentAlias).Append('.').Append(relation.PrincipalKeys[i].EscCol);
            }
        }

        private MethodCallExpression RewriteNavigationAggregate(
            MethodCallExpression originalCall,
            ParameterExpression parentParam,
            TableMapping.Relation relation)
        {
            var depType = relation.DependentType;

            // Materialize the dependent IQueryable upfront so the sub-translator sees a
            // ConstantExpression<IQueryable<Child>> at the root - that is the shape its
            // VisitConstant recognizes as the query source. Building the expression as
            // `Expression.Call(NormQueryable.Query, ctxConstant)` would emit the DbContext
            // as a SQL parameter instead.
            var queryMethod = typeof(NormQueryable).GetMethod(nameof(NormQueryable.Query))!
                .MakeGenericMethod(depType);
            var dependentQuery = queryMethod.Invoke(null, new object[] { _ctx })!;
            var sourceExpr = (Expression)Expression.Constant(dependentQuery, typeof(IQueryable<>).MakeGenericType(depType));

            // Build the FK = PK predicate against the dependent's property.
            var childParam = Expression.Parameter(depType, "__nav_" + Guid.NewGuid().ToString("N").Substring(0, 8));
            Expression? predicateBody = null;
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                Expression fkAccess = Expression.Property(childParam, relation.ForeignKeys[i].Prop);
                Expression pkAccess = Expression.Property(parentParam, relation.PrincipalKeys[i].Prop);
                // Promote nullable mismatches so Expression.Equal type-checks (e.g. nullable FK
                // referring to a non-nullable PK).
                if (fkAccess.Type != pkAccess.Type)
                {
                    var common = Nullable.GetUnderlyingType(fkAccess.Type) ?? fkAccess.Type;
                    if (fkAccess.Type != common) fkAccess = Expression.Convert(fkAccess, common);
                    if (pkAccess.Type != common) pkAccess = Expression.Convert(pkAccess, common);
                }

                var equality = Expression.Equal(fkAccess, pkAccess);
                predicateBody = predicateBody is null ? equality : Expression.AndAlso(predicateBody, equality);
            }
            var fkPredicate = Expression.Lambda(predicateBody!, childParam);

            sourceExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                new[] { depType }, sourceExpr, Expression.Quote(fkPredicate));

            // Apply the dependent entity's global filters (soft-delete, tenant) inside the correlated
            // subquery, so a navigation predicate (Any/All/Count/LongCount) counts only visible rows.
            // Without this, c.Orders.Any() over-includes a parent whose only orders are soft-deleted,
            // and c.Orders.Count() counts filtered-out rows.
            var childGlobalFilter = GlobalFilterFragment.Combine(_ctx, depType);
            if (childGlobalFilter != null)
                sourceExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { depType }, sourceExpr, Expression.Quote(childGlobalFilter));

            // Re-emit the aggregate call (Any/All/Count/LongCount) against the synthesized
            // Queryable source. The original predicate (if any) is the second arg.
            var methodName = originalCall.Method.Name;
            if (originalCall.Arguments.Count > 1)
            {
                var innerPredicate = StripQuotes(originalCall.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(
                        $"{methodName}() on a navigation collection requires a lambda predicate argument.");
                var queryableMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.GetParameters().Length == 2)
                    .MakeGenericMethod(depType);
                return Expression.Call(queryableMethod, sourceExpr, Expression.Quote(innerPredicate));
            }
            else
            {
                var queryableMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.GetParameters().Length == 1)
                    .MakeGenericMethod(depType);
                return Expression.Call(queryableMethod, sourceExpr);
            }
        }

        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            // Materialize any `NormQueryable.Query<T>(ctx)` calls inside the source into
            // ConstantExpression(IQueryable) so the sub-translator's VisitConstant picks up
            // the query as the entity source instead of emitting `@p<n>` for the ctx instance.
            source = QueryCallMaterializer.Materialize(source);
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            // Both Parameters and CompiledParameters use SEPARATE dicts/lists for the sub-
            // translator. QueryTranslator.Dispose() calls ParameterManager.Reset() which
            // Clear()s these collections — sharing them with the outer would wipe the
            // outer's accumulated params and compiled-param registrations the moment the
            // sub-translator goes out of `using`. Copy both back before dispose.
            var tempParams = new Dictionary<string, object>();
            var tempCompiled = new List<string>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex, _parameterMappings, new HashSet<string>(), tempCompiled, _paramMap, _parameterMappings.Count, recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }
            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }
        private void BuildScalarCountSubquery(Expression source, LambdaExpression? predicate)
        {
            ThrowIfUnsupportedScalarAggregateSource(source, "Count");
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            source = QueryCallMaterializer.Materialize(source);
            var (head, tail) = TranslateScalarAggregateSource(source, "Count");

            // A single distinct scalar column must stay distinct inside COUNT; an
            // entity-wide DISTINCT is a no-op for the row count (rows are key-unique).
            var countOperand = "*";
            if (head.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
            {
                var projected = head.Substring("DISTINCT ".Length).Trim();
                if (!HasTopLevelComma(projected))
                    countOperand = "DISTINCT " + projected;
            }
            _sql.Append("(SELECT COUNT(").Append(countOperand).Append(')');
            _sql.Append(tail);
            _sql.Append(")");
        }

        /// <summary>
        /// Emits a correlated scalar-aggregate subquery for an explicit queryable source in a
        /// predicate — <c>ctx.Query&lt;B&gt;().Where(b =&gt; b.Fk == a.Id).Sum(b =&gt; b.Amt)</c> —
        /// the sibling of the navigation-collection aggregate emit. The selector appends as a
        /// Select so it translates against the subquery's own alias, then the single-column
        /// SELECT head is wrapped in the aggregate function. Outer references correlate through
        /// the shared parameter mappings. Empty subqueries yield SQL NULL (matching the
        /// navigation-aggregate surface), which comparison predicates treat as no match.
        /// </summary>
        private void BuildScalarAggregateSubquery(Expression source, LambdaExpression? selector, string aggregateName)
        {
            ThrowIfUnsupportedScalarAggregateSource(source, aggregateName);
            var sqlAgg = aggregateName switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                _ => "AVG",
            };
            Type operandType;
            if (selector != null)
            {
                var et = GetElementType(source);
                operandType = selector.Body.Type;
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Select),
                    new[] { et, operandType }, source, Expression.Quote(selector));
            }
            else
            {
                operandType = GetElementType(source);
            }
            source = QueryCallMaterializer.Materialize(source);
            var (head, tail) = TranslateScalarAggregateSource(source, aggregateName);

            var distinct = false;
            if (head.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
            {
                distinct = true;
                head = head.Substring("DISTINCT ".Length).Trim();
            }
            if (HasTopLevelComma(head))
                throw new NormUnsupportedFeatureException(
                    $"{aggregateName}() over a correlated subquery requires a single scalar projection; " +
                    "project the value with Select(x => x.Member) first.");
            if (sqlAgg == "AVG")
                head = _provider.AverageAggregateOperand(head, operandType);

            _sql.Append("(SELECT ").Append(sqlAgg).Append('(');
            if (distinct)
                _sql.Append("DISTINCT ");
            _sql.Append(head).Append(')').Append(tail).Append(')');
        }

        /// <summary>
        /// Translates a correlated-aggregate source in a sub-translator (correlated params
        /// shared, so outer references resolve to the outer alias) and splits the SQL at its
        /// top-level FROM, stripping the trailing ORDER BY (meaningless inside a scalar
        /// subquery and disallowed by some providers). Returns the SELECT head and the
        /// FROM-onward tail.
        /// </summary>
        private (string Head, string Tail) TranslateScalarAggregateSource(Expression source, string aggregateName)
        {
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var tempParams = new Dictionary<string, object>();
            var tempCompiled = new List<string>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex, _parameterMappings, new HashSet<string>(), tempCompiled, _paramMap, _parameterMappings.Count, recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }

            var sql = subPlan.Sql;
            var fromIdx = FindTopLevelFromIndex(sql);
            if (fromIdx < 0)
                throw new NormQueryException($"Could not rewrite {aggregateName}() subquery: no top-level FROM clause found.");
            var head = sql.Substring("SELECT ".Length, fromIdx - "SELECT ".Length).Trim();
            var tail = sql.Substring(fromIdx);
            var orderIdx = tail.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderIdx >= 0)
                tail = tail.Substring(0, orderIdx);
            return (head, tail);
        }

        /// <summary>
        /// Correlated scalar aggregates slice the sub-plan at its top-level FROM and wrap the
        /// projection in the aggregate function, which is only sound for a plain
        /// filtered/projected/ordered source: windows would apply their paging AFTER the
        /// aggregate (LIMIT limits the one-row result, not the aggregated inputs), and set
        /// ops, grouping, and client-tail reshapes change the row set the slice sees.
        /// </summary>
        private static void ThrowIfUnsupportedScalarAggregateSource(Expression source, string methodName)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case "Where":
                    case "Select":
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                    case "Distinct":
                    case "AsNoTracking":
                    case "Query": // the `ctx.Query<T>()` chain root
                        break;
                    default:
                        throw new NormUnsupportedFeatureException(
                            $"{methodName}() over a correlated subquery supports Where/Select/OrderBy/Distinct sources; " +
                            $"'{mce.Method.Name}' windows or reshapes the subquery's rows and has no sound scalar-aggregate " +
                            "form. Materialize the inner query first or restructure the aggregate.");
                }
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
        }

        /// <summary>
        /// Finds the first <c> FROM </c> at parenthesis depth zero, skipping string literals,
        /// so a projection containing a nested subquery does not split the SQL mid-expression.
        /// </summary>
        private static int FindTopLevelFromIndex(string sql)
        {
            var depth = 0;
            var inString = false;
            for (var i = 0; i < sql.Length; i++)
            {
                var c = sql[i];
                if (inString)
                {
                    if (c == '\'') inString = false;
                    continue;
                }
                switch (c)
                {
                    case '\'': inString = true; break;
                    case '(': depth++; break;
                    case ')': depth--; break;
                    case ' ' when depth == 0
                        && i + 6 <= sql.Length
                        && string.Compare(sql, i, " FROM ", 0, 6, StringComparison.OrdinalIgnoreCase) == 0:
                        return i;
                }
            }
            return -1;
        }

        private static bool HasTopLevelComma(string sql)
        {
            var depth = 0;
            var inString = false;
            foreach (var c in sql)
            {
                if (inString)
                {
                    if (c == '\'') inString = false;
                    continue;
                }
                switch (c)
                {
                    case '\'': inString = true; break;
                    case '(': depth++; break;
                    case ')': depth--; break;
                    case ',' when depth == 0: return true;
                }
            }
            return false;
        }
        private void BuildIn(Expression source, Expression value)
        {
            // Extract expression from closure-captured IQueryable.
            if (TryGetConstantValue(source, out var srcConstValue) && srcConstValue is System.Linq.IQueryable iqSrc)
                source = iqSrc.Expression!;
            // Mirror BuildExists (line ~1584): materialize any `NormQueryable.Query<T>(ctx)`
            // calls inside the source into ConstantExpression(IQueryable) so the sub-translator's
            // VisitConstant picks up the query as the entity source instead of trying to bind
            // the DbContext as a SQL parameter (which throws "No mapping exists from object
            // type nORM.Core.DbContext"). Without this, `Where(p => ctx.Query<O>().Select(o
            // => o.Id).Contains(p.Id))` — the semi-join-via-Contains idiom — silently
            // fails to translate.
            source = QueryCallMaterializer.Materialize(source);

            // Compile-time null: ConstantExpression{null} OR Convert(null, T?) (UnaryExpression).
            bool isNullValue = (value is ConstantExpression { Value: null })
                || (value is UnaryExpression { NodeType: ExpressionType.Convert } ueNull
                    && ueNull.Operand is ConstantExpression { Value: null });
            if (isNullValue)
            {
                BuildNullExistsForContains(source);
                return;
            }

            // Build the IN (subquery) with a fresh correlated dict so inner lambda params get T0
            // regardless of what the outer _parameterMappings contains.
            // Use a separate tempParams dict: QueryTranslator.Dispose() clears its Parameters dict —
            // if shared with _params, collected params are lost.
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var freshCorrelatedForIn = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            var tempParams = new Dictionary<string, object>();
            var tempCompiled = new List<string>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex,
                freshCorrelatedForIn, new HashSet<string>(), tempCompiled, _paramMap, 0,
                recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            // Copy collected params and compiled-param registrations to outer BEFORE
            // subTranslator.Dispose() clears those collections.
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }

            // SQL NULL IN (...) is UNKNOWN (not TRUE); emit null-safe OR pattern for nullable value types.
            bool isNullable = !value.Type.IsValueType || Nullable.GetUnderlyingType(value.Type) != null;

            if (!isNullable)
            {
                Visit(value);
                _sql.Append(" IN (");
                _sql.Append(subPlan.Sql);
                _sql.Append(")");
                return;
            }

            // Runtime nullable: (val IN (subq) OR (val IS NULL AND EXISTS(null-filtered subq)))
            var valueSql = GetSql(value);
            _sql.Append("(");
            _sql.Append(valueSql);
            _sql.Append(" IN (");
            _sql.Append(subPlan.Sql);
            _sql.Append(") OR (");
            _sql.Append(valueSql);
            _sql.Append(" IS NULL AND ");
            BuildNullExistsForContains(source);
            _sql.Append("))");
        }

        // Emits EXISTS(SELECT ... FROM source WHERE col IS NULL).
        // Uses a fresh correlated dict so the EXISTS translator starts clean with T0 for all params.
        private void BuildNullExistsForContains(Expression source)
        {
            // Walk back through Select calls to find the entity-level query and selector.
            LambdaExpression? innerSelector = null;
            var cursor = source;
            while (cursor is MethodCallExpression mce && mce.Method.Name == "Select")
            {
                innerSelector = StripQuotes(mce.Arguments[1]) as LambdaExpression;
                cursor = mce.Arguments[0];
            }

            // Append a WHERE col IS NULL filter at the entity level.
            Expression filteredSource;
            Type entityType;
            if (innerSelector == null)
            {
                // No Select: the source IS the entity query; filter entities where they are null.
                entityType = GetElementType(source);
                var p = Expression.Parameter(entityType, "__nc");
                var isNull = Expression.Equal(p, Expression.Constant(null, entityType));
                filteredSource = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { entityType }, source, Expression.Quote(Expression.Lambda(isNull, p)));
            }
            else
            {
                // Has Select: add a null-check on the projected column at the entity level.
                // WhereTranslator does not increment _joinCounter, so innerSelector.Parameters[0]
                // and cursor's lambda params all get T0 in the fresh-dict EXISTS translator.
                entityType = innerSelector.Parameters[0].Type;
                var nullCheck = Expression.Lambda(
                    Expression.Equal(innerSelector.Body, Expression.Constant(null, innerSelector.ReturnType)),
                    innerSelector.Parameters[0]);
                filteredSource = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { entityType }, cursor, Expression.Quote(nullCheck));
            }

            // Fresh correlated dict and separate tempParams so EXISTS translator never sees
            // stale aliases, and Dispose() clearing its Parameters dict doesn't affect _params.
            var freshCorrelated = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            var rootType = GetRootElementType(filteredSource);
            var mapping = _ctx.GetMapping(rootType);
            var existsTempParams = new Dictionary<string, object>();
            var existsTempCompiled = new List<string>();
            using var existsTranslator = QueryTranslator.Create(
                _ctx, mapping, existsTempParams, _paramIndex,
                freshCorrelated, new HashSet<string>(),
                existsTempCompiled, _paramMap, 0,
                recursionDepth: _recursionDepth + 1);
            var existsPlan = existsTranslator.Translate(filteredSource);
            _paramIndex = existsTranslator.ParameterIndex;
            // Copy collected params and compiled-param registrations to outer BEFORE
            // existsTranslator.Dispose() clears those collections.
            foreach (var kvp in existsTempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in existsTempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }
            _sql.Append("EXISTS(");
            _sql.Append(existsPlan.Sql);
            _sql.Append(")");
        }
    }
}
