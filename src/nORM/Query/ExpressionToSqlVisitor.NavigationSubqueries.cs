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
        /// Emits a predicate-context aggregate over an OWNED (OwnsMany) or MANY-TO-MANY collection —
        /// <c>Where(p =&gt; p.Tags.Any())</c>, <c>Where(o =&gt; o.Lines.Count() &gt; 1)</c>. These collections
        /// live in separate mapping structures from ordinary relations, so the relation-based rewrite above
        /// can't reach them; the rewrite also needs a CLR FK property (owned FKs are often shadow columns)
        /// and an IQueryable source (m2m has none), so this emits the correlated subquery SQL directly.
        /// Only the unfiltered Count/LongCount/Any shapes over a single-key link are emitted; predicates,
        /// <c>All</c>, composite keys, and collections whose element type has a global/tenant filter fail
        /// loud (rendering that filter here is a follow-up) — never silent-wrong. Returns false when the
        /// receiver isn't such a collection, letting the caller continue.
        /// </summary>
        private bool TryEmitNonRelationCollectionPredicateAggregate(MethodCallExpression node)
        {
            var method = node.Method.Name;
            if (method is not (nameof(Queryable.Any) or nameof(Queryable.Count)
                               or nameof(Queryable.LongCount) or nameof(Queryable.All)))
                return false;
            if (_ctx == null
                || node.Arguments.Count < 1
                || node.Arguments[0] is not MemberExpression navMember
                || navMember.Expression is not ParameterExpression navParent
                || !_parameterMappings.TryGetValue(navParent, out var info))
                return false;

            var mapping = info.Mapping;
            var owned = mapping.OwnedCollections.FirstOrDefault(o => o.NavigationProperty.Name == navMember.Member.Name);
            var jtm = owned == null
                ? mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == navMember.Member.Name)
                : null;
            if (owned == null && jtm == null)
                return false;

            var kind = owned != null ? "owned" : "many-to-many";
            // From here it IS such a collection: emit or fail loud, never fall through to a silent-wrong path.
            if (method is nameof(Queryable.All))
                throw new NormUnsupportedFeatureException(
                    $"All(...) over the {kind} collection '{navMember.Member.Name}' in a predicate isn't supported yet. " +
                    "Evaluate it after materialising, or filter with a separate query.");
            if (node.Arguments.Count > 1)
                throw new NormUnsupportedFeatureException(
                    $"A filtered {method}(...) over the {kind} collection '{navMember.Member.Name}' in a predicate isn't supported yet. " +
                    "Filter after materialising, or use a separate query.");

            var parentAlias = info.Alias;
            var depAlias = _provider.Escape("__nav");

            if (owned != null)
            {
                if (mapping.KeyColumns.Length != 1)
                    throw new NormUnsupportedFeatureException(
                        $"Aggregating the owned collection '{navMember.Member.Name}' on an entity with a composite key isn't supported yet.");
                if (GlobalFilterFragment.CombineWithTenant(_ctx, owned.OwnedType) != null)
                    throw new NormUnsupportedFeatureException(
                        $"Aggregating the owned collection '{navMember.Member.Name}' whose item type has a global or tenant filter isn't supported in a predicate yet. " +
                        "Use a separate query.");
                QueryTranslator.RecordReferencedTable(owned.TableName);
                var where = $"{depAlias}.{owned.EscForeignKeyColumn} = {parentAlias}.{mapping.KeyColumns[0].EscCol}";
                if (method is nameof(Queryable.Any))
                    _sql.Append("EXISTS(SELECT 1 FROM ").Append(owned.EscTable).Append(' ').Append(depAlias).Append(" WHERE ").Append(where).Append(')');
                else
                    _sql.Append("(SELECT COUNT(*) FROM ").Append(owned.EscTable).Append(' ').Append(depAlias).Append(" WHERE ").Append(where).Append(')');
                return true;
            }

            if (jtm!.LeftKeyColumns.Count != 1 || jtm.RightKeyColumns.Count != 1)
                throw new NormUnsupportedFeatureException(
                    $"Aggregating the many-to-many collection '{navMember.Member.Name}' with a composite key isn't supported yet.");
            if (GlobalFilterFragment.CombineWithTenant(_ctx, jtm.RightType) != null)
                throw new NormUnsupportedFeatureException(
                    $"Aggregating the many-to-many collection '{navMember.Member.Name}' whose related type has a global or tenant filter isn't supported in a predicate yet. " +
                    "Use a separate query.");
            var rightMap = _ctx.GetMapping(jtm.RightType);
            var jAlias = _provider.Escape("__m2mj");
            var rAlias = _provider.Escape("__m2mr");
            QueryTranslator.RecordReferencedTable(jtm.TableName);
            QueryTranslator.RecordReferencedTable(rightMap.TableName);
            // Join the bridge table to the related table (so orphaned bridge rows don't over-count, matching
            // the loaded collection). With no related-side filter this is a straight existence/row count.
            var fromJoin = $"{jtm.EscTableName} {jAlias} JOIN {QueryTranslator.TemporalTableSource(rightMap)} {rAlias} " +
                           $"ON {rAlias}.{jtm.RightKeyColumns[0].EscCol} = {jAlias}.{jtm.EscRightFkColumn}";
            var m2mWhere = $"{jAlias}.{jtm.EscLeftFkColumn} = {parentAlias}.{jtm.LeftKeyColumns[0].EscCol}";
            if (method is nameof(Queryable.Any))
                _sql.Append("EXISTS(SELECT 1 FROM ").Append(fromJoin).Append(" WHERE ").Append(m2mWhere).Append(')');
            else
                _sql.Append("(SELECT COUNT(*) FROM ").Append(fromJoin).Append(" WHERE ").Append(m2mWhere).Append(')');
            return true;
        }

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
                .Append(" FROM ").Append(QueryTranslator.TemporalTableSource(childMapping)).Append(' ').Append(subAlias)
                .Append(" WHERE ");
            AppendRelationPredicate(_sql, relation, subAlias, parentInfo.Alias);
            // Row visibility (soft-delete global filters, tenant) restricts which
            // dependent rows enter the aggregate — same rule as the projection-side
            // navigation aggregate emits.
            var visibilitySql = RenderPrincipalGlobalFilterSql(childMapping, subAlias);
            if (visibilitySql != null)
                _sql.Append(" AND ").Append(visibilitySql);
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

        /// <summary>
        /// Reserves one placeholder compiled-param slot when a correlated subquery's
        /// chain root consumes a closure member structurally — the DbContext receiver
        /// inside <c>ctx.Query&lt;T&gt;()</c> or a captured IQueryable local. The
        /// ParameterValueExtractor walks those members in document order like any
        /// other closure (the root comes FIRST in its walk of the subtree), so
        /// without a placeholder the extracted DbContext/queryable value binds to
        /// the first real compiled parameter and shifts every one after it.
        /// </summary>
        private void ReserveQuerySourceRootClosureSlot(Expression source)
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
                        ReserveUnusedCompiledSlot();
                    }
                    return;
                }
                if (mce.Arguments.Count == 0) return;
                current = mce.Arguments[0];
            }
            if (current is MemberExpression rootMember
                && typeof(IQueryable).IsAssignableFrom(rootMember.Type)
                && QueryTranslator.TryGetConstantValue(rootMember, out _))
            {
                ReserveUnusedCompiledSlot();
            }
        }

        private void ReserveUnusedCompiledSlot()
        {
            // The `_ctx_unused` suffix keeps the binder's DBNull override (it still
            // ends in `_unused`) WITHOUT tripping the fold-no-cache rule: unlike a
            // folded value, nothing execution-specific is baked into the SQL — the
            // slot exists purely for extractor alignment, so the plan stays cacheable.
            var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_ctx_unused";
            _params[placeholder] = DBNull.Value;
            _compiledParams.Add(placeholder);
        }

        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            ReserveQuerySourceRootClosureSlot(source);
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            // Materialize any `NormQueryable.Query<T>(ctx)` calls inside the source into
            // ConstantExpression(IQueryable) so the sub-translator's VisitConstant picks up
            // the query as the entity source instead of emitting `@p<n>` for the ctx instance.
            source = ApplySubqueryRootFiltersWithFoldSignal(source);
            source = QueryCallMaterializer.Materialize(source);
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            // Both Parameters and CompiledParameters use SEPARATE dicts/lists for the sub-
            // translator. QueryTranslator.Dispose() calls ParameterManager.Reset() which
            // Clear()s these collections — sharing them with the outer would wipe the
            // outer's accumulated params and compiled-param registrations the moment the
            // sub-translator goes out of `using`. Copy both back before dispose.
            // Seed with the outer's entries so @p numbering (derived from _params.Count
            // inside the sub-translation's visitor contexts) continues globally: a
            // restarted @p name collides with an outer slot and the name-deduping
            // copy-back MERGES two distinct values into one parameter. The seeded
            // entries copy back onto themselves.
            var tempParams = new Dictionary<string, object>(_params);
            // Seed with the outer's compiled-param names so the sub-translator's
            // @cp numbering continues globally instead of restarting at @cp0: a
            // restarted name collides with an outer (or sibling/nested) subquery's
            // slot and the name-deduping copy-back below then MERGES two distinct
            // closures into one parameter — the second closure silently binds the
            // first one's value. The seeded prefix is deduped away on copy-back.
            var tempCompiled = new List<string>(_compiledParams);
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
            MergeSubPlanConverters(subPlan);
            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }

        /// <summary>
        /// Merges value-converter registrations the sub-translation minted for closure
        /// values compared against converter columns inside the subquery. Dropping them
        /// binds the raw CLR value instead of the provider representation (silent wrong
        /// match). Mirrors the param and compiled-name copy-back.
        /// </summary>
        private void MergeSubPlanConverters(QueryPlan subPlan)
        {
            if (subPlan.ParameterConverters is not { Count: > 0 } converters)
                return;
            foreach (var kvp in converters)
                _paramConverters[kvp.Key] = kvp.Value;
        }
        /// <summary>
        /// Applies the context's global/tenant filters to the subquery's ROOT (the
        /// provider's top-level filter rewrite never reaches roots inside lambdas —
        /// without this a correlated subquery reads ACROSS tenants). When the injected
        /// filter had to inline closure-dependent values, a `_gf_unused` compiled name
        /// marks the plan fold-no-cache through the established `_unused` channel.
        /// </summary>
        private Expression ApplySubqueryRootFiltersWithFoldSignal(Expression source)
        {
            var filtered = QueryTranslator.ApplySubqueryRootFilters(_ctx, source, out var folded);
            if (folded)
            {
                var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_gf_unused";
                _params[placeholder] = DBNull.Value;
                _compiledParams.Add(placeholder);
            }
            return filtered;
        }

        private void BuildScalarCountSubquery(Expression source, LambdaExpression? predicate)
        {
            ThrowIfUnsupportedScalarAggregateSource(source, "Count");
            ReserveQuerySourceRootClosureSlot(source);
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            source = ApplySubqueryRootFiltersWithFoldSignal(source);
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
            ReserveQuerySourceRootClosureSlot(source);
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
            source = ApplySubqueryRootFiltersWithFoldSignal(source);
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
            var sql = TranslateCorrelatedSubquerySql(source);
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
        /// Sub-translates a correlated subquery source and returns its full SQL. Outer
        /// references correlate through the shared parameter mappings; @p and @cp numbering
        /// continues globally (both dicts are seeded with the outer's entries so a restarted
        /// name cannot collide with an outer/sibling slot and silently merge two values).
        /// The sub-plan's value converters are merged back so provider bindings survive.
        /// </summary>
        private string TranslateCorrelatedSubquerySql(Expression source)
        {
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var tempParams = new Dictionary<string, object>(_params);
            var tempCompiled = new List<string>(_compiledParams);
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
            MergeSubPlanConverters(subPlan);
            return subPlan.Sql;
        }

        /// <summary>
        /// Emits a correlated <c>First</c>/<c>FirstOrDefault</c> scalar subquery for an explicit
        /// queryable source in a predicate — <c>ctx.Query&lt;B&gt;().Where(b =&gt; b.Fk == a.Id)
        /// .OrderByDescending(b =&gt; b.V).Select(b =&gt; b.V).First()</c>. The source must project a
        /// single scalar; its ORDER BY is kept (unlike the aggregate slice) so the row selected is
        /// deterministic, and the provider limits the result to one row (<c>LIMIT 1</c> / SQL Server
        /// <c>TOP 1</c>). An empty subquery yields SQL NULL — matching the correlated-aggregate
        /// surface and EF's translation (LINQ-to-Objects would instead throw on First over empty).
        /// </summary>
        private void BuildScalarFirstSubquery(Expression source, LambdaExpression? predicate, string methodName, Expression? indexExpr = null)
        {
            ThrowIfUnsupportedScalarAggregateSource(source, methodName);
            ReserveQuerySourceRootClosureSlot(source);
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            // Last/LastOrDefault = First of the reversed ordering. Without an ordering "last" is
            // undefined in SQL, so fail closed rather than silently returning the first row.
            if (methodName is nameof(Queryable.Last) or nameof(Queryable.LastOrDefault))
            {
                source = QueryTranslator.ReverseQueryableOrderings(source, out var hadOrdering);
                if (!hadOrdering)
                    throw new NormUnsupportedFeatureException(
                        $"{methodName}() over a correlated subquery requires an OrderBy — 'last' is undefined without an ordering.");
            }
            // ElementAt/ElementAtOrDefault: skip N rows then take one. An index into an unordered
            // set is undefined (and SQL Server's OFFSET/FETCH requires ORDER BY), so fail closed.
            if (indexExpr != null && !QueryTranslator.HasQueryableOrdering(source))
                throw new NormUnsupportedFeatureException(
                    $"{methodName}() over a correlated subquery requires an OrderBy — element position is undefined without an ordering.");

            source = ApplySubqueryRootFiltersWithFoldSignal(source);
            source = QueryCallMaterializer.Materialize(source);

            var sql = TranslateCorrelatedSubquerySql(source);
            var fromIdx = FindTopLevelFromIndex(sql);
            if (fromIdx < 0)
                throw new NormQueryException($"Could not rewrite {methodName}() subquery: no top-level FROM clause found.");
            var head = sql.Substring("SELECT ".Length, fromIdx - "SELECT ".Length).Trim();
            if (head.StartsWith("DISTINCT ", StringComparison.OrdinalIgnoreCase))
                head = head.Substring("DISTINCT ".Length).Trim();
            if (HasTopLevelComma(head))
                throw new NormUnsupportedFeatureException(
                    $"{methodName}() over a correlated subquery requires a single scalar projection; " +
                    "project the value with Select(x => x.Member) first.");

            // The offset (ElementAt) is a plain outer-context value; emit it after the subquery so
            // its closure slot, if any, follows the subquery's in document order.
            var offsetSql = indexExpr != null ? GetSql(indexExpr) : null;
            _sql.Append(_provider.BuildScalarLimitedSubquery(sql, offsetSql));
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
        internal static int FindTopLevelFromIndex(string sql)
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

        internal static bool HasTopLevelComma(string sql)
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
            ReserveQuerySourceRootClosureSlot(source);
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
            source = ApplySubqueryRootFiltersWithFoldSignal(source);
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
            // Seeded with the outer's entries so @p numbering (derived from _params.Count
            // inside the sub-translation's visitor contexts) continues globally — a
            // restarted name collides with an outer slot and the copy-back merges two
            // distinct values into one parameter.
            var tempParams = new Dictionary<string, object>(_params);
            // Seed with the outer's compiled-param names so the sub-translator's
            // @cp numbering continues globally instead of restarting at @cp0: a
            // restarted name collides with an outer (or sibling/nested) subquery's
            // slot and the name-deduping copy-back below then MERGES two distinct
            // closures into one parameter — the second closure silently binds the
            // first one's value. The seeded prefix is deduped away on copy-back.
            var tempCompiled = new List<string>(_compiledParams);
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
            MergeSubPlanConverters(subPlan);

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
            // Seeded with the outer's entries so @p / @cp numbering continues globally —
            // a restarted name collides with an outer slot and the copy-back merges two
            // distinct values into one parameter. Seeds copy back onto themselves.
            var existsTempParams = new Dictionary<string, object>(_params);
            var existsTempCompiled = new List<string>(_compiledParams);
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
            MergeSubPlanConverters(existsPlan);
            _sql.Append("EXISTS(");
            _sql.Append(existsPlan.Sql);
            _sql.Append(")");
        }
    }
}
