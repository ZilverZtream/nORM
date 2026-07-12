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
            var subAlias = $"T_nav_{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            // Translate the selector lambda against the child mapping. Routing through a
            // dedicated child-parameter binding lets nested member access / arithmetic /
            // COALESCE all flow through the regular expression-to-SQL pipeline.
            using var subVisitor = new ExpressionToSqlVisitor();
            var subCtx = new VisitorContext(
                _ctx, childMapping, _provider,
                selectorLambda.Parameters[0], subAlias,
                _parameterMappings, _compiledParams, _paramMap, _recursionDepth + 1, _paramIndex);
            subVisitor.Initialize(in subCtx);
            subVisitor.UseSharedParameterDictionary(_paramSink);
            var selectorSql = subVisitor.Translate(selectorLambda.Body);
            _paramIndex = subVisitor.ParamIndex;

            _sql.Append("(SELECT ").Append(sqlAgg).Append('(').Append(selectorSql).Append(')')
                .Append(" FROM ").Append(childMapping.EscTable).Append(' ').Append(_provider.Escape(subAlias))
                .Append(" WHERE ");
            AppendRelationPredicate(_sql, relation, _provider.Escape(subAlias), parentInfo.Alias);
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
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
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

            // Rewrite the entity SELECT into SELECT COUNT(*) by replacing everything before the
            // first ` FROM `. Strip any trailing ORDER BY which is meaningless inside a scalar
            // subquery and disallowed by some providers.
            var sql = subPlan.Sql;
            var fromIdx = sql.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIdx < 0)
                throw new NormQueryException("Could not rewrite Count() subquery: no FROM clause found.");
            var tail = sql.Substring(fromIdx);
            var orderIdx = tail.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderIdx >= 0)
                tail = tail.Substring(0, orderIdx);
            _sql.Append("(SELECT COUNT(*)");
            _sql.Append(tail);
            _sql.Append(")");
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
