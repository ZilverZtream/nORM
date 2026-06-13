using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private Expression HandleInnerJoin(MethodCallExpression node)
        {
            if (node.Arguments.Count < 5)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join operation requires 5 arguments"));
            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;
            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join selectors must be lambda expressions"));
            // Detect `Select(proj).Distinct().Join(...)` and similar outer sources that
            // would need a subquery wrap (the join's outer key selector references the
            // projected element rather than a column on the entity). nORM doesn't yet
            // emit `FROM (SELECT DISTINCT ... FROM tbl) AS T0 INNER JOIN ...` — without
            // the wrap the outer key resolves to an empty fragment and SQLite throws a
            // `near "=": syntax error`. Surface a clear exception with two concrete
            // workarounds: materialize-then-Contains or pre-filter via Where.
            if (outerQuery is MethodCallExpression outerMce
                && outerMce.Method.Name == nameof(Queryable.Distinct))
            {
                throw new NormUnsupportedFeatureException(
                    "Join over a `.Distinct()` outer source isn't supported — nORM doesn't yet emit " +
                    "the subquery wrap (`FROM (SELECT DISTINCT ... FROM tbl) AS T0 INNER JOIN ...`) " +
                    "that this shape needs. Workarounds: " +
                    "(1) materialize the distinct keys first and feed them through Contains: " +
                    "`var keys = await ctx.Query<L>().Select(l => l.Code).Distinct().ToListAsync();` " +
                    "then `ctx.Query<R>().Where(r => keys.Contains(r.Code)).ToListAsync()`; " +
                    "(2) push the join through first and apply DISTINCT to the result: " +
                    "`ctx.Query<L>().Join(ctx.Query<R>(), l => l.Code, r => r.Code, (l, r) => new {...}).Distinct()`.");
            }
            // Join over a Take/Skip-windowed outer source — wrap the windowed outer as a
            // derived table so the join matches only the LIMITed/Skipped outer rows.
            // Sister of the post-Take/Skip silent-wrongness family.
            string? outerFromOverride = null;
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var outerAlias = EscapeAlias("T0");
            if (SourceHasTakeOrSkip(outerQuery))
            {
                var subOuter = TranslateInSubContext(outerQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subOuterMap);
                _mapping = subOuterMap;
                MergeSubPlanParameters(subOuter);
                outerFromOverride = "(" + subOuter.Sql + ") AS " + outerAlias;
            }
            else
            {
                Visit(outerQuery);
            }
            var currentOuterType = CurrentPostMaterializeElementType ?? _projection?.Body.Type ?? _mapping.Type;
            var projectsWholeEntity = resultSelector.Body is NewExpression wholeEntityNew
                                      && wholeEntityNew.Arguments.Any(a => a == resultSelector.Parameters[0] || a == resultSelector.Parameters[1]);
            if ((IsPostMaterializeTailMode
                 || projectsWholeEntity
                 || outerQuery is MethodCallExpression chainedJoinSource && chainedJoinSource.Method.Name == nameof(Queryable.Join))
                && currentOuterType == outerKeySelector.Parameters[0].Type)
            {
                AppendPostMaterializeInnerJoin(innerQuery, outerKeySelector, innerKeySelector, resultSelector);
                return node;
            }
            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == outerKeySelector.Parameters[0].Type)
            {
                AppendPostMaterializeGroupJoin(innerQuery, outerKeySelector, innerKeySelector, resultSelector);
                return node;
            }
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            var sqlOuterKeySelector = ExpandProjection(outerKeySelector);
            if (!_correlatedParams.ContainsKey(sqlOuterKeySelector.Parameters[0]))
                _correlatedParams[sqlOuterKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, sqlOuterKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(sqlOuterKeySelector.Body);
            // Use AddLiteralParameter (see HandleGroupBy / OrderByTranslator): inline constants
            // in a key selector (e.g. COALESCE fallback) must not be re-flagged in
            // _compiledParams or BindPlanParameters treats them as runtime closures and skips
            // the value at execution time.
            foreach (var kvp in outerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(outerKeyVisitor);
            if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);
            var innerOnConditions = ExtractInnerWhereConditions(innerQuery, innerMapping, innerAlias);
            var additionalOnSql = innerOnConditions.Count > 0
                ? string.Join(" AND ", innerOnConditions.Select(c => "(" + c + ")"))
                : null;
            var transparentExpansion = ComposeTransparentIdentifierSelector(resultSelector);
            JoinBuilder.SetupJoinProjection(resultSelector, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
            _transparentIdentifier = transparentExpansion;
            RegisterTransparentIdentifierTail(transparentExpansion, innerMapping, innerAlias);
            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(
                _sql,
                _projection,
                _mapping,
                outerAlias,
                innerMapping,
                innerAlias,
                "INNER JOIN",
                outerKeySql,
                innerKeySql,
                distinct: _isDistinct,
                outerFromOverride: outerFromOverride,
                additionalOnConditions: additionalOnSql,
                translateProjectionExpression: TranslateJoinProjectionExpression,
                escapeProjectionAlias: _provider.Escape);
            return node;
        }

        private string TranslateJoinProjectionExpression(Expression expression)
        {
            if (_projection == null || _projection.Parameters.Count == 0)
                throw new NormQueryException("Join projection translation requires a result selector.");

            var parameter = _projection.Parameters[0];
            if (!_correlatedParams.TryGetValue(parameter, out var info))
                info = (_mapping, EscapeAlias("T0"));

            var vctx = new VisitorContext(
                _ctx,
                info.Mapping,
                _provider,
                parameter,
                info.Alias,
                _correlatedParams,
                _compiledParams,
                _paramMap,
                _recursionDepth,
                _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            try
            {
                var sql = visitor.Translate(expression);
                foreach (var kvp in visitor.GetParameters())
                {
                    AddLiteralParameter(kvp.Key, kvp.Value);
                    AdvanceParameterIndexPast(kvp.Key);
                }

                return sql;
            }
            finally
            {
                FastExpressionVisitorPool.Return(visitor);
            }
        }

        private void RewritePrebuiltJoinProjectionIfNeeded()
        {
            if (_projection == null
                || _groupJoinInfo != null
                || _isAggregate
                || _sql.Length == 0)
            {
                return;
            }

            var sql = _sql.ToSqlString();
            if (!sql.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)
                || !sql.Contains(" JOIN ", StringComparison.OrdinalIgnoreCase)
                || sql.Contains(" UNION ", StringComparison.OrdinalIgnoreCase)
                || sql.Contains(" INTERSECT ", StringComparison.OrdinalIgnoreCase)
                || sql.Contains(" EXCEPT ", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (!TryBuildJoinProjectionSelectList(_projection.Body, out var selectList))
                return;

            var fromIndex = sql.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIndex <= "SELECT ".Length || string.IsNullOrWhiteSpace(selectList))
                return;

            var selectPrefix = sql.StartsWith("SELECT DISTINCT ", StringComparison.OrdinalIgnoreCase)
                ? "SELECT DISTINCT "
                : "SELECT ";

            _sql.Clear();
            _sql.Append(selectPrefix).Append(selectList).Append(sql.AsSpan(fromIndex));
        }

        private bool TryBuildJoinProjectionSelectList(Expression projectionBody, out string selectList)
        {
            using var builder = new OptimizedSqlBuilder(256);
            selectList = string.Empty;

            if (projectionBody is MemberInitExpression memberInit)
            {
                var wroteAny = false;
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        continue;

                    if (ContainsWholeEntityParameter(assignment.Expression))
                        return false;

                    if (wroteAny)
                        builder.Append(", ");

                    builder.Append(TranslateJoinProjectionExpression(assignment.Expression))
                        .Append(" AS ")
                        .Append(_provider.Escape(assignment.Member.Name));
                    wroteAny = true;
                }

                if (!wroteAny)
                    return false;

                selectList = builder.ToSqlString();
                return true;
            }

            if (projectionBody is NewExpression newExpr)
            {
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var argument = newExpr.Arguments[i];
                    if (ContainsWholeEntityParameter(argument))
                        return false;

                    if (i > 0)
                        builder.Append(", ");

                    var alias = newExpr.Members?[i].Name ?? $"Item{i + 1}";
                    builder.Append(TranslateJoinProjectionExpression(argument))
                        .Append(" AS ")
                        .Append(_provider.Escape(alias));
                }

                if (newExpr.Arguments.Count == 0)
                    return false;

                selectList = builder.ToSqlString();
                return true;
            }

            if (ContainsWholeEntityParameter(projectionBody))
                return false;

            selectList = TranslateJoinProjectionExpression(projectionBody);
            return !string.IsNullOrWhiteSpace(selectList);
        }

        private static bool ContainsWholeEntityParameter(Expression expression)
        {
            if (expression is ParameterExpression)
                return true;

            var visitor = new WholeEntityParameterVisitor();
            visitor.Visit(expression);
            return visitor.Found;
        }

        private sealed class WholeEntityParameterVisitor : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                Found = true;
                return node;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression is ParameterExpression)
                    return node;

                return base.VisitMember(node);
            }
        }

        private Expression HandleGroupJoin(MethodCallExpression node)
        {
            if (node.Arguments.Count < 5)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join operation requires 5 arguments"));
            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;
            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Join selectors must be lambda expressions"));
            // Sister of the HandleInnerJoin guard above: `Select(proj).Distinct().GroupJoin(...)`
            // also needs the subquery wrap (`FROM (SELECT DISTINCT ... FROM tbl) AS T0 LEFT
            // JOIN ...`) that nORM doesn't yet emit. Without it the outer key resolves to an
            // empty fragment and execution dies inside the group-join materializer with an
            // opaque "Unexpected error in MaterializeGroupJoin". Surface the same actionable
            // exception so callers can apply the same Contains / post-join-Distinct rewrite.
            if (outerQuery is MethodCallExpression outerMce
                && outerMce.Method.Name == nameof(Queryable.Distinct))
            {
                throw new NormUnsupportedFeatureException(
                    "GroupJoin over a `.Distinct()` outer source isn't supported — nORM doesn't yet emit " +
                    "the subquery wrap (`FROM (SELECT DISTINCT ... FROM tbl) AS T0 LEFT JOIN ...`) " +
                    "that this shape needs. Workarounds: " +
                    "(1) materialize the distinct keys first and project the right-side groups via Contains: " +
                    "`var keys = await ctx.Query<L>().Select(l => l.Code).Distinct().ToListAsync();` " +
                    "then `var rs = await ctx.Query<R>().Where(r => keys.Contains(r.Code)).ToListAsync();` " +
                    "and group client-side with `keys.Select(k => new { k, Rs = rs.Where(r => r.Code == k).ToList() })`; " +
                    "(2) push the GroupJoin through first and apply DISTINCT to the result.");
            }
            // GroupJoin over a Take/Skip-windowed outer source — wrap the windowed outer
            // as a derived table so LEFT JOIN runs against the windowed rows only.
            // Sister of HandleInnerJoin's windowed branch above.
            string? gjOuterFromOverride = null;
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var outerAlias = EscapeAlias("T0");
            if (SourceHasTakeOrSkip(outerQuery))
            {
                var subOuter = TranslateInSubContext(outerQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subOuterMap);
                _mapping = subOuterMap;
                MergeSubPlanParameters(subOuter);
                gjOuterFromOverride = "(" + subOuter.Sql + ") AS " + outerAlias;
            }
            else
            {
                Visit(outerQuery);
            }
            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == outerKeySelector.Parameters[0].Type)
            {
                AppendPostMaterializeGroupJoin(innerQuery, outerKeySelector, innerKeySelector, resultSelector);
                return node;
            }
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            var sqlOuterKeySelector = ExpandProjection(outerKeySelector);
            if (!_correlatedParams.ContainsKey(sqlOuterKeySelector.Parameters[0]))
                _correlatedParams[sqlOuterKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, sqlOuterKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(sqlOuterKeySelector.Body);
            // See HandleJoin: AddLiteralParameter so inline constants in the key selector
            // aren't mis-flagged as compiled-runtime placeholders.
            foreach (var kvp in outerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(outerKeyVisitor);
            if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);
            JoinBuilder.SetupJoinProjection(null, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
            // Preserve the result selector so downstream Where/OrderBy on the projected
            // anonymous type (e.g. `(p, cs) => new {Name=p.Name, Count=cs.Count()}` →
            // later `.OrderBy(r => r.Name)`) can expand `r.Name` back through it. We
            // cannot reuse `_projection` for this — the materialiser would try to build
            // a 2-parameter projection materialiser and crash; the GroupJoin materialiser
            // path uses the compiled `GroupJoinInfo.ResultSelector` Func instead.
            _groupJoinResultSelector = resultSelector;
            _groupJoinExpansionSelector = ComposeGroupJoinExpansionSelector(resultSelector);
            // The result selector's parameter instances are DIFFERENT from the key-selector's
            // (each lambda has its own scope). Pre-register them in _correlatedParams so a
            // downstream OrderBy/Where that's ExpandProjection-ed through this selector
            // resolves `p.Name` against the outer alias (T0) rather than auto-registering
            // with `_joinCounter` (which is now the inner alias index, producing wrong-table
            // references like `T1.Name`).
            if (resultSelector.Parameters.Count >= 1
                && !_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                _correlatedParams[resultSelector.Parameters[0]] = (_mapping, outerAlias);
            if (resultSelector.Parameters.Count >= 2
                && !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
            if (_groupJoinExpansionSelector.Parameters.Count > resultSelector.Parameters.Count)
            {
                var composedInner = _groupJoinExpansionSelector.Parameters[^1];
                if (!_correlatedParams.ContainsKey(composedInner))
                    _correlatedParams[composedInner] = (innerMapping, innerAlias);
            }
            // Do NOT embed ORDER BY in the SQL string. Instead, insert the outer key as the
            // first ORDER BY entry so that Build() generates exactly one ORDER BY clause.
            // This prevents double ORDER BY when downstream .OrderBy() is chained, and ensures
            // outer-key contiguity (needed for streaming group segmentation) is always first.
            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(_sql, _projection, _mapping, outerAlias, innerMapping, innerAlias, "LEFT JOIN", outerKeySql, innerKeySql, orderBy: null, distinct: _isDistinct, outerFromOverride: gjOuterFromOverride);
            // Insert outer-key sort at the front of _orderBy so it is always first.
            _orderBy.Insert(0, (outerKeySql, true));
            var outerType = outerKeySelector.Parameters[0].Type;
            var innerType = innerKeySelector.Parameters[0].Type;
            var resultType = resultSelector.Body.Type;
            var innerKeyColumn = innerMapping.Columns.FirstOrDefault(c =>
                ExtractPropertyName(innerKeySelector.Body) == c.PropName);
            if (innerKeyColumn != null)
            {
                var outerKeyFunc = CreateObjectKeySelector(outerKeySelector);
                var resultSelectorFunc = CompileGroupJoinResultSelector(resultSelector);
                _groupJoinInfo = new GroupJoinInfo(
                    outerType,
                    innerType,
                    resultType,
                    outerKeyFunc,
                    innerKeyColumn,
                    resultSelectorFunc
                );
            }
            return node;
        }
        /// <summary>
        /// Translates LINQ SelectMany operations into SQL JOIN clauses, handling collection flattening
        /// and transparent identifier management.
        /// </summary>
        /// <remarks>
        /// <para><b>Supported Scenarios:</b></para>
        /// <list type="number">
        /// <item><description>
        /// <b>Simple Navigation Property:</b> <c>blogs.SelectMany(b => b.Posts)</c>
        /// Translates to: <c>INNER JOIN Posts ON blogs.Id = Posts.BlogId</c>
        /// </description></item>
        /// <item><description>
        /// <b>Filtered Navigation:</b> <c>blogs.SelectMany(b => b.Posts.Where(p => p.Active))</c>
        /// Translates to: <c>INNER JOIN Posts ON blogs.Id = Posts.BlogId WHERE Posts.Active = 1</c>
        /// </description></item>
        /// <item><description>
        /// <b>With Result Selector:</b> <c>blogs.SelectMany(b => b.Posts, (b, p) => new { b.Name, p.Title })</c>
        /// Creates transparent identifier for downstream operations and projects both entities
        /// </description></item>
        /// <item><description>
        /// <b>Cross Join:</b> <c>blogs.SelectMany(b => allCategories)</c>
        /// Translates to: <c>CROSS JOIN Categories</c>
        /// </description></item>
        /// </list>
        ///
        /// <para><b>Transparent Identifier Handling:</b></para>
        /// <para>
        /// When a result selector is provided, both the outer (collection) and inner (element) parameters
        /// are registered in <c>_correlatedParams</c>, allowing subsequent Select/Where operations to
        /// reference both entities through compiler-generated transparent identifier types
        /// (e.g., <c>&lt;&gt;h__TransparentIdentifier0</c>).
        /// </para>
        ///
        /// <para>Example:</para>
        /// <code>
        /// ctx.Query&lt;Blog&gt;()
        ///    .SelectMany(b => b.Posts, (b, p) => new { Blog = b, Post = p })
        ///    .Select(x => new { x.Blog.Name, x.Post.Title })
        /// </code>
        /// <para>
        /// The transparent identifier <c>x</c> maintains references to both <c>b</c> and <c>p</c>,
        /// which are tracked in <c>_correlatedParams</c> with their respective table aliases.
        /// </para>
        /// </remarks>
        private Expression HandleSelectMany(MethodCallExpression node)
        {
            // SelectMany can be used in different ways:
            // 1. SelectMany(collectionSelector) - flattens collections
            // 2. SelectMany(collectionSelector, resultSelector) - joins and projects
            if (node.Arguments.Count < 2)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "SelectMany requires at least 2 arguments"));
            var sourceQuery = node.Arguments[0];
            var collectionSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                                   ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Collection selector must be a lambda expression"));

            // SelectMany over a Take/Skip-windowed outer source — wrap the windowed outer
            // as a derived table so the flatten/join runs only against the LIMITed rows.
            // Sister of HandleInnerJoin / HandleGroupJoin windowed-outer branches (0de442d).
            string? smOuterFromOverride = null;
            if (SourceHasTakeOrSkip(sourceQuery))
            {
                var subSmOuter = TranslateInSubContext(sourceQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subSmMap);
                _mapping = subSmMap;
                MergeSubPlanParameters(subSmOuter);
                smOuterFromOverride = subSmOuter.Sql;
                // Skip the Visit(sourceQuery) below — we've already translated it in the
                // sub-context. The rest of the method uses _mapping (already set above)
                // and emits the join SQL by hand.
            }

            // Detect the query-syntax left join shape: `from p in P join c in C on p.K equals
            // c.K into grp from c in grp.DefaultIfEmpty() select projection(p, c)` lowers to
            // GroupJoin().SelectMany(t => t.grp.DefaultIfEmpty(), (t, c) => proj). Rewrite as
            // an explicit LEFT JOIN with the GroupJoin's keys, skipping MaterializeGroupJoin.
            var smResultSelector = node.Arguments.Count > 2
                ? StripQuotes(node.Arguments[2]) as LambdaExpression
                : null;
            if (smResultSelector != null
                && smResultSelector.Parameters.Count == 2
                && sourceQuery is MethodCallExpression gj
                && gj.Method.Name == nameof(Queryable.GroupJoin)
                && gj.Arguments.Count >= 5
                && collectionSelector.Body is MethodCallExpression dfe
                && dfe.Method.Name == nameof(Queryable.DefaultIfEmpty)
                && dfe.Arguments.Count >= 1
                && dfe.Arguments[0] is MemberExpression grpMember
                && grpMember.Expression == collectionSelector.Parameters[0]
                && StripQuotes(gj.Arguments[4]) is LambdaExpression gjResultSel
                && gjResultSel.Body is NewExpression gjNew
                && gjNew.Members != null)
            {
                // Find which member of the GroupJoin's anonymous result holds the outer entity:
                // it's the one whose name is NOT the grp member referenced by DefaultIfEmpty.
                string? outerMemberName = null;
                for (int i = 0; i < gjNew.Members.Count; i++)
                {
                    if (gjNew.Members[i].Name != grpMember.Member.Name)
                    {
                        outerMemberName = gjNew.Members[i].Name;
                        break;
                    }
                }
                if (outerMemberName != null
                    && StripQuotes(gj.Arguments[2]) is LambdaExpression gjOuterKeySel
                    && StripQuotes(gj.Arguments[3]) is LambdaExpression gjInnerKeySel)
                {
                    HandleQuerySyntaxLeftJoin(
                        gj.Arguments[0], gj.Arguments[1],
                        gjOuterKeySel, gjInnerKeySel,
                        gjResultSel, collectionSelector, smResultSelector, outerMemberName);
                    return node;
                }
            }

            // Visit the source query first to establish base mapping
            if (smOuterFromOverride == null)
                Visit(sourceQuery);
            var outerMapping = _mapping;
            var outerAlias = EscapeAlias("T0");
            // Track the outer parameter for correlated references
            if (!_correlatedParams.ContainsKey(collectionSelector.Parameters[0]))
                _correlatedParams[collectionSelector.Parameters[0]] = (outerMapping, outerAlias);
            // Determine if a result selector is provided
            var resultSelector = node.Arguments.Count > 2
                ? StripQuotes(node.Arguments[2]) as LambdaExpression
                : null;

            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == collectionSelector.Parameters[0].Type)
            {
                AppendPostMaterializeSelectMany(collectionSelector, resultSelector);
                return node;
            }

            var collectionBody = UnwrapSelectManyDefaultIfEmpty(collectionSelector.Body, out var useLeftJoin);

            if (TryHandleNavigationSelectMany(outerMapping, outerAlias, collectionBody, resultSelector, useLeftJoin, smOuterFromOverride))
                return node;

            if (TryHandleCorrelatedSelectMany(outerMapping, outerAlias, collectionBody, resultSelector, smOuterFromOverride))
                return node;

            HandleCrossSelectMany(outerMapping, outerAlias, collectionSelector, resultSelector, useLeftJoin, smOuterFromOverride);
            return node;
        }

        private static Expression UnwrapSelectManyDefaultIfEmpty(Expression collectionBody, out bool useLeftJoin)
        {
            useLeftJoin = false;
            if (collectionBody is MethodCallExpression defaultIfEmptyCall &&
                defaultIfEmptyCall.Method.Name == nameof(Queryable.DefaultIfEmpty) &&
                defaultIfEmptyCall.Arguments.Count >= 1)
            {
                useLeftJoin = true;
                return defaultIfEmptyCall.Arguments[0];
            }

            return collectionBody;
        }

        private bool TryHandleNavigationSelectMany(
            TableMapping outerMapping,
            string outerAlias,
            Expression collectionBody,
            LambdaExpression? resultSelector,
            bool useLeftJoin,
            string? outerFromOverride)
        {
            TableMapping.Relation? relation = null;
            LambdaExpression? filterPredicate = null;
            MemberExpression? navigationMember = null;

            if (collectionBody is MemberExpression memberExpr &&
                outerMapping.Relations.TryGetValue(memberExpr.Member.Name, out relation))
            {
                navigationMember = memberExpr;
            }
            else if (collectionBody is MethodCallExpression methodCall &&
                     methodCall.Method.Name == nameof(Queryable.Where) &&
                     methodCall.Arguments.Count == 2 &&
                     methodCall.Arguments[0] is MemberExpression navMember &&
                     outerMapping.Relations.TryGetValue(navMember.Member.Name, out relation))
            {
                navigationMember = navMember;
                filterPredicate = StripQuotes(methodCall.Arguments[1]) as LambdaExpression;
            }

            if (navigationMember == null || relation == null)
                return false;

            var innerMapping = TrackMapping(relation.DependentType);
            var innerAlias = EscapeAlias("T" + (++_joinCounter));

            RegisterSelectManyResultSelectorParameters(resultSelector, outerMapping, outerAlias, innerMapping, innerAlias);
            var composedProjection = ComposeSelectManyProjection(resultSelector);
            var effectiveProjection = composedProjection ?? _projection;

            _sql.Clear();
            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            if (!AppendNavigationSelectManyColumns(effectiveProjection, resultSelector, outerMapping, innerMapping, outerAlias, innerAlias))
                AppendSelectManyAllColumns(outerMapping, outerAlias, innerMapping, innerAlias);

            _sql.Append(' ');
            AppendSelectManyOuterFrom(outerMapping, outerAlias, outerFromOverride);
            var joinType = useLeftJoin ? "LEFT JOIN" : "INNER JOIN";
            _sql.Append(joinType).Append(' ').Append(innerMapping.EscTable).Append(' ').Append(innerAlias).Append(' ');
            _sql.Append("ON ");
            for (var keyIndex = 0; keyIndex < relation.PrincipalKeys.Count; keyIndex++)
            {
                if (keyIndex > 0)
                    _sql.Append(" AND ");
                _sql.Append(outerAlias).Append('.').Append(relation.PrincipalKeys[keyIndex].EscCol)
                    .Append(" = ").Append(innerAlias).Append('.').Append(relation.ForeignKeys[keyIndex].EscCol);
            }

            if (filterPredicate != null)
                AppendSelectManyFilterPredicate(filterPredicate, innerMapping, innerAlias);

            if (resultSelector != null)
            {
                _projection = composedProjection ?? resultSelector;
            }
            else
            {
                _mapping = innerMapping;
                _rootType = innerMapping.Type;
            }

            return true;
        }

        private bool TryHandleCorrelatedSelectMany(
            TableMapping outerMapping,
            string outerAlias,
            Expression collectionBody,
            LambdaExpression? resultSelector,
            string? outerFromOverride)
        {
            if (collectionBody is not MethodCallExpression corrWhereCall
                || corrWhereCall.Method.Name != nameof(Queryable.Where)
                || corrWhereCall.Arguments.Count != 2
                || corrWhereCall.Arguments[0] is MemberExpression
                || StripQuotes(corrWhereCall.Arguments[1]) is not LambdaExpression corrPredLambda)
            {
                return false;
            }

            var corrInnerType = GetElementType(collectionBody);
            var corrInnerMapping = TrackMapping(corrInnerType);
            var corrInnerAlias = EscapeAlias("T" + (++_joinCounter));

            RegisterSelectManyResultSelectorParameters(resultSelector, outerMapping, outerAlias, corrInnerMapping, corrInnerAlias);

            var corrInnerParam = corrPredLambda.Parameters[0];
            if (!_correlatedParams.ContainsKey(corrInnerParam))
                _correlatedParams[corrInnerParam] = (corrInnerMapping, corrInnerAlias);

            var vctxCorr = new VisitorContext(_ctx, corrInnerMapping, _provider, corrInnerParam, corrInnerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var corrPredVisitor = FastExpressionVisitorPool.Get(in vctxCorr);
            var corrPredSql = corrPredVisitor.Translate(corrPredLambda.Body);
            foreach (var kvp in corrPredVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            FastExpressionVisitorPool.Return(corrPredVisitor);

            _sql.Clear();
            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            var corrProjExpr = (_projection?.Body as NewExpression) ?? (resultSelector?.Body as NewExpression);
            if (!AppendProjectedSelectManyColumns(corrProjExpr, outerMapping, corrInnerMapping, outerAlias, corrInnerAlias)
                && !AppendInnerOnlySelectManyColumns(resultSelector, corrInnerMapping, corrInnerAlias))
            {
                AppendSelectManyAllColumns(outerMapping, outerAlias, corrInnerMapping, corrInnerAlias);
            }

            _sql.Append(' ');
            AppendSelectManyOuterFrom(outerMapping, outerAlias, outerFromOverride);
            _sql.Append("INNER JOIN ").Append(corrInnerMapping.EscTable).Append(' ').Append(corrInnerAlias).Append(' ');
            _sql.Append("ON ").Append(corrPredSql);

            if (resultSelector != null)
            {
                var transparentExpansion = ComposeTransparentIdentifierSelector(resultSelector);
                _transparentIdentifier = transparentExpansion;
                RegisterTransparentIdentifierTail(transparentExpansion, corrInnerMapping, corrInnerAlias);
                if (_projection == null)
                    _projection = resultSelector;
            }
            else
            {
                _mapping = corrInnerMapping;
                _rootType = corrInnerMapping.Type;
            }

            return true;
        }

        private void HandleCrossSelectMany(
            TableMapping outerMapping,
            string outerAlias,
            LambdaExpression collectionSelector,
            LambdaExpression? resultSelector,
            bool useLeftJoin,
            string? outerFromOverride)
        {
            var innerType = GetElementType(collectionSelector.Body);
            var crossMapping = TrackMapping(innerType);
            var crossAlias = EscapeAlias("T" + (++_joinCounter));

            RegisterSelectManyResultSelectorParameters(resultSelector, outerMapping, outerAlias, crossMapping, crossAlias);
            using var crossSql = new OptimizedSqlBuilder(CrossJoinSqlInitialCapacity);
            var crossComposedProjection = ComposeSelectManyProjection(resultSelector);

            NewExpression? projectionNewExpr = (crossComposedProjection?.Body as NewExpression)
                ?? (_projection?.Body as NewExpression)
                ?? (resultSelector?.Body as NewExpression);
            if (projectionNewExpr != null)
            {
                var neededColumns = JoinBuilder.ExtractNeededColumns(projectionNewExpr, outerMapping, crossMapping, outerAlias, crossAlias);
                if (neededColumns.Count == 0)
                    AppendCrossSelectManyAllColumns(crossSql, outerMapping, outerAlias, crossMapping, crossAlias);
                else
                    AppendCrossSelectManyColumns(crossSql, neededColumns);
            }
            else if (resultSelector == null)
            {
                var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                crossSql.AppendJoin(", ", innerCols);
                crossSql.Append(' ');
            }
            else
            {
                AppendCrossSelectManyAllColumns(crossSql, outerMapping, outerAlias, crossMapping, crossAlias);
            }

            if (outerFromOverride != null)
                crossSql.Append("FROM (").Append(outerFromOverride).Append(") AS ").Append(outerAlias).Append(' ');
            else
                crossSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");

            if (useLeftJoin)
                crossSql.Append($"LEFT JOIN {crossMapping.EscTable} {crossAlias} ON 1=1");
            else
                crossSql.Append($"CROSS JOIN {crossMapping.EscTable} {crossAlias}");

            _sql.Clear();
            _sql.Append(crossSql.ToSqlString());
            if (resultSelector != null)
            {
                var transparentExpansion = ComposeTransparentIdentifierSelector(resultSelector);
                _transparentIdentifier = transparentExpansion;
                RegisterTransparentIdentifierTail(transparentExpansion, crossMapping, crossAlias);
                if (_projection == null)
                    _projection = resultSelector;
            }
            else
            {
                _mapping = crossMapping;
            }
        }

        private void RegisterSelectManyResultSelectorParameters(
            LambdaExpression? resultSelector,
            TableMapping outerMapping,
            string outerAlias,
            TableMapping innerMapping,
            string innerAlias)
        {
            if (resultSelector == null || resultSelector.Parameters.Count <= 1)
                return;

            if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                _correlatedParams[resultSelector.Parameters[0]] = (outerMapping, outerAlias);
            if (!_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
        }

        private LambdaExpression? ComposeSelectManyProjection(LambdaExpression? resultSelector)
        {
            if (resultSelector == null || _projection == null)
                return null;

            var savedProjection = _projection;
            var savedTransparent = _transparentIdentifier;
            _transparentIdentifier = ComposeTransparentIdentifierSelector(resultSelector);
            var expanded = ExpandProjection(savedProjection);
            _transparentIdentifier = savedTransparent;
            return ReferenceEquals(expanded, savedProjection) ? null : expanded;
        }

        private bool AppendNavigationSelectManyColumns(
            LambdaExpression? effectiveProjection,
            LambdaExpression? resultSelector,
            TableMapping outerMapping,
            TableMapping innerMapping,
            string outerAlias,
            string innerAlias)
        {
            if (effectiveProjection?.Body is NewExpression newExpr)
                return AppendProjectedSelectManyColumns(newExpr, outerMapping, innerMapping, outerAlias, innerAlias);

            if (AppendInnerOnlySelectManyColumns(resultSelector, innerMapping, innerAlias))
                return true;

            if (resultSelector?.Body is NewExpression resultNewExpr)
                return AppendProjectedSelectManyColumns(resultNewExpr, outerMapping, innerMapping, outerAlias, innerAlias);

            if (resultSelector?.Body is MemberExpression memberSel
                && TableMapping.TryGetMemberAccessRoot(memberSel, out var memParam))
            {
                TableMapping? selMapping = null;
                string? selAlias = null;
                if (resultSelector.Parameters.Count > 0 && memParam == resultSelector.Parameters[0])
                {
                    selMapping = outerMapping;
                    selAlias = outerAlias;
                }
                else if (resultSelector.Parameters.Count > 1 && memParam == resultSelector.Parameters[1])
                {
                    selMapping = innerMapping;
                    selAlias = innerAlias;
                }

                if (selMapping != null && selAlias != null
                    && selMapping.TryGetColumnForMemberAccess(memberSel, out var memCol))
                {
                    _sql.Append(selAlias).Append('.').Append(memCol.EscCol);
                    return true;
                }
            }

            return false;
        }

        private bool AppendProjectedSelectManyColumns(
            NewExpression? projection,
            TableMapping outerMapping,
            TableMapping innerMapping,
            string outerAlias,
            string innerAlias)
        {
            if (projection == null)
                return false;

            var neededColumns = JoinBuilder.ExtractNeededColumns(projection, outerMapping, innerMapping, outerAlias, innerAlias);
            if (neededColumns.Count == 0)
                return false;

            for (int i = 0; i < neededColumns.Count; i++)
            {
                if (i > 0) _sql.Append(", ");
                _sql.Append(neededColumns[i]);
            }

            return true;
        }

        private bool AppendInnerOnlySelectManyColumns(
            LambdaExpression? resultSelector,
            TableMapping innerMapping,
            string innerAlias)
        {
            if (resultSelector != null)
                return false;

            for (int i = 0; i < innerMapping.Columns.Length; i++)
            {
                if (i > 0) _sql.Append(", ");
                _sql.Append(innerAlias).Append('.').Append(innerMapping.Columns[i].EscCol);
            }

            return true;
        }

        private void AppendSelectManyAllColumns(
            TableMapping outerMapping,
            string outerAlias,
            TableMapping innerMapping,
            string innerAlias)
        {
            bool first = true;
            for (int i = 0; i < outerMapping.Columns.Length; i++)
            {
                if (!first) _sql.Append(", ");
                _sql.Append(outerAlias).Append('.').Append(outerMapping.Columns[i].EscCol);
                first = false;
            }

            for (int i = 0; i < innerMapping.Columns.Length; i++)
            {
                if (!first) _sql.Append(", ");
                _sql.Append(innerAlias).Append('.').Append(innerMapping.Columns[i].EscCol);
                first = false;
            }
        }

        private void AppendSelectManyFilterPredicate(
            LambdaExpression filterPredicate,
            TableMapping innerMapping,
            string innerAlias)
        {
            var filterParam = filterPredicate.Parameters[0];
            if (!_correlatedParams.ContainsKey(filterParam))
                _correlatedParams[filterParam] = (innerMapping, innerAlias);

            var vctxFilter = new VisitorContext(_ctx, innerMapping, _provider, filterParam, innerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var filterVisitor = FastExpressionVisitorPool.Get(in vctxFilter);
            var filterSql = filterVisitor.Translate(filterPredicate.Body);

            if (_where.Length > 0)
                _where.Append(" AND ");
            _where.Append('(').Append(filterSql).Append(')');

            foreach (var kvp in filterVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            FastExpressionVisitorPool.Return(filterVisitor);
        }

        private void AppendSelectManyOuterFrom(
            TableMapping outerMapping,
            string outerAlias,
            string? outerFromOverride)
        {
            _sql.Append("FROM ");
            if (outerFromOverride != null)
                _sql.Append('(').Append(outerFromOverride).Append(") AS ").Append(outerAlias).Append(' ');
            else
                _sql.Append(outerMapping.EscTable).Append(' ').Append(outerAlias).Append(' ');
        }

        private static void AppendCrossSelectManyAllColumns(
            OptimizedSqlBuilder crossSql,
            TableMapping outerMapping,
            string outerAlias,
            TableMapping innerMapping,
            string innerAlias)
        {
            var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
            var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
            crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
            crossSql.AppendJoin(", ", outerCols.Concat(innerCols));
            crossSql.Append(' ');
        }

        private static void AppendCrossSelectManyColumns(
            OptimizedSqlBuilder crossSql,
            IReadOnlyList<string> neededColumns)
        {
            crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
            crossSql.AppendJoin(", ", neededColumns);
            crossSql.Append(' ');
        }

        /// <summary>
        /// Translates the query-syntax left-join shape (GroupJoin + SelectMany + DefaultIfEmpty)
        /// directly to a LEFT JOIN, bypassing MaterializeGroupJoin. The SelectMany result
        /// selector's parameters get rewritten so its body references the outer / inner
        /// entities directly via the join's aliases.
        /// </summary>
        private void HandleQuerySyntaxLeftJoin(
            Expression outerQuery,
            Expression innerQuery,
            LambdaExpression outerKeySel,
            LambdaExpression innerKeySel,
            LambdaExpression groupJoinResultSelector,
            LambdaExpression collectionSelector,
            LambdaExpression smResultSelector,
            string outerMemberName)
        {
            Visit(outerQuery);
            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == outerKeySel.Parameters[0].Type)
            {
                AppendPostMaterializeGroupJoin(innerQuery, outerKeySel, innerKeySel, groupJoinResultSelector);
                AppendPostMaterializeSelectMany(collectionSelector, smResultSelector);
                return;
            }
            outerKeySel = ExpandProjection(outerKeySel);
            var outerMapping = _mapping;
            var outerAlias = EscapeAlias("T0");
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var innerAlias = EscapeAlias("T" + (++_joinCounter));

            if (!_correlatedParams.ContainsKey(outerKeySel.Parameters[0]))
                _correlatedParams[outerKeySel.Parameters[0]] = (outerMapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, outerMapping, _provider, outerKeySel.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySel.Body);
            // See HandleJoin: AddLiteralParameter for inline-constant key fragments.
            foreach (var kvp in outerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(outerKeyVisitor);

            if (!_correlatedParams.ContainsKey(innerKeySel.Parameters[0]))
                _correlatedParams[innerKeySel.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySel.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySel.Body);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);

            // Rewrite the SelectMany result selector so its body uses fresh outer/inner entity
            // parameters that JoinBuilder can substitute for the join aliases.
            var outerEntityType = outerMapping.Type;
            var freshOuter = Expression.Parameter(outerEntityType, "__lj_outer");
            var freshInner = Expression.Parameter(innerElementType, "__lj_inner");
            var rewriter = new QuerySyntaxLeftJoinRewriter(
                smResultSelector.Parameters[0], smResultSelector.Parameters[1],
                outerMemberName, freshOuter, freshInner);
            var newBody = rewriter.Visit(smResultSelector.Body)!;
            var rewrittenResultSel = Expression.Lambda(newBody, freshOuter, freshInner);

            JoinBuilder.SetupJoinProjection(rewrittenResultSel, outerMapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);

            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(
                _sql,
                _projection,
                outerMapping,
                outerAlias,
                innerMapping,
                innerAlias,
                "LEFT JOIN",
                outerKeySql,
                innerKeySql,
                distinct: _isDistinct,
                translateProjectionExpression: TranslateJoinProjectionExpression,
                escapeProjectionAlias: _provider.Escape);
        }

        private sealed class QuerySyntaxLeftJoinRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _oldTransparent;
            private readonly ParameterExpression _oldInner;
            private readonly string _outerMemberName;
            private readonly ParameterExpression _newOuter;
            private readonly ParameterExpression _newInner;

            public QuerySyntaxLeftJoinRewriter(
                ParameterExpression oldTransparent,
                ParameterExpression oldInner,
                string outerMemberName,
                ParameterExpression newOuter,
                ParameterExpression newInner)
            {
                _oldTransparent = oldTransparent;
                _oldInner = oldInner;
                _outerMemberName = outerMemberName;
                _newOuter = newOuter;
                _newInner = newInner;
            }

            protected override Expression VisitConditional(ConditionalExpression node)
            {
                // `c == null ? null : c.Prop` and mirrored `c != null ? c.Prop : null`:
                // Strip the null-guard entirely — LEFT JOIN already NULLs unmatched rows.
                // `c == null ? fallback : c.Prop` and mirrored `c != null ? c.Prop : fallback`
                // where fallback is a non-null constant: rewrite to COALESCE(c.Prop, fallback).
                // In SQL a LEFT JOIN null propagates through ALL right-side columns, so
                // COALESCE correctly substitutes the fallback for unmatched rows. The one
                // edge case where COALESCE differs from .NET: a matched row where c.Prop
                // happens to be NULL in the DB also gets the fallback in SQL, whereas .NET
                // would return null. That distinction is documented as an acceptable
                // approximation for non-nullable projected columns.
                if (IsInnerParam(node.Test, out bool testIsEqualParam))
                {
                    var propBranch = testIsEqualParam ? node.IfFalse : node.IfTrue;
                    var nullBranch = testIsEqualParam ? node.IfTrue  : node.IfFalse;
                    if (IsNullOrDefault(nullBranch))
                        return Visit(propBranch);
                    return BuildCoalesce(Visit(propBranch), Visit(nullBranch));
                }
                else if (IsEntityNullComparison(node.Test, out bool _, out bool eqOp))
                {
                    var propBranch = eqOp ? node.IfFalse : node.IfTrue;
                    var nullBranch = eqOp ? node.IfTrue  : node.IfFalse;
                    if (IsNullOrDefault(nullBranch))
                        return Visit(propBranch);
                    return BuildCoalesce(Visit(propBranch), Visit(nullBranch));
                }
                return base.VisitConditional(node);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression == _oldTransparent && node.Member.Name == _outerMemberName)
                    return _newOuter;
                return base.VisitMember(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (node == _oldInner) return _newInner;
                return base.VisitParameter(node);
            }

            private bool IsInnerParam(Expression e, out bool isEqual)
            {
                isEqual = false;
                return e == _oldInner || e == _newInner;
            }

            private bool IsEntityNullComparison(Expression test, out bool nullOnLeft, out bool isEqual)
            {
                nullOnLeft = false;
                isEqual = false;
                if (test is not BinaryExpression bin) return false;
                if (bin.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual)) return false;
                isEqual = bin.NodeType == ExpressionType.Equal;
                if ((bin.Left == _oldInner || bin.Left == _newInner)
                    && bin.Right is ConstantExpression { Value: null })
                { nullOnLeft = false; return true; }
                if ((bin.Right == _oldInner || bin.Right == _newInner)
                    && bin.Left is ConstantExpression { Value: null })
                { nullOnLeft = true; return true; }
                return false;
            }

            private static bool IsNullOrDefault(Expression e) =>
                e is ConstantExpression { Value: null }
                || e is DefaultExpression
                || (e is UnaryExpression ue
                    && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked)
                    && ue.Operand is ConstantExpression { Value: null });

            private static Expression BuildCoalesce(Expression valueExpression, Expression fallbackExpression)
            {
                var originalValueType = valueExpression.Type;
                var coalesceLeft = valueExpression;
                if (coalesceLeft.Type.IsValueType && Nullable.GetUnderlyingType(coalesceLeft.Type) == null)
                {
                    coalesceLeft = Expression.Convert(coalesceLeft, typeof(Nullable<>).MakeGenericType(coalesceLeft.Type));
                }

                var rightTargetType = Nullable.GetUnderlyingType(coalesceLeft.Type) ?? coalesceLeft.Type;
                var coalesceRight = ConvertIfNeeded(fallbackExpression, rightTargetType);
                var coalesce = Expression.Coalesce(coalesceLeft, coalesceRight);
                return coalesce.Type == originalValueType
                    ? coalesce
                    : ConvertIfNeeded(coalesce, originalValueType);
            }

            private static Expression ConvertIfNeeded(Expression expression, Type targetType)
            {
                if (expression.Type == targetType)
                    return expression;
                if (expression is ConstantExpression { Value: null })
                    return Expression.Constant(null, targetType);
                return Expression.Convert(expression, targetType);
            }
        }

        // Walks the inner query expression chain and extracts any WHERE predicates,
        // translating them against the given inner alias. Used by HandleInnerJoin to
        // push inner-source WHERE conditions into the JOIN ON clause so they are
        // included in ExecuteDelete/Update subqueries.
        private System.Collections.Generic.List<string> ExtractInnerWhereConditions(
            Expression innerQuery, TableMapping innerMapping, string innerAlias)
        {
            var conditions = new System.Collections.Generic.List<string>();
            var source = innerQuery;
            while (source is MethodCallExpression mce)
            {
                if (mce.Method.Name == "Where"
                    && mce.Arguments.Count == 2
                    && StripQuotes(mce.Arguments[1]) is LambdaExpression predLambda)
                {
                    var param = predLambda.Parameters[0];
                    if (!_correlatedParams.ContainsKey(param))
                        _correlatedParams[param] = (innerMapping, innerAlias);

                    var vctx = new VisitorContext(_ctx, innerMapping, _provider, param, innerAlias,
                        _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                    var visitor = FastExpressionVisitorPool.Get(in vctx);
                    var condSql = visitor.Translate(predLambda.Body);
                    foreach (var kvp in visitor.GetParameters())
                        _params[kvp.Key] = kvp.Value;
                    FastExpressionVisitorPool.Return(visitor);

                    conditions.Add(condSql);
                    source = mce.Arguments[0];
                }
                else
                {
                    break;
                }
            }
            return conditions;
        }
    }
}
