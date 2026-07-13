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

            // SelectMany over a Take/Skip-windowed outer source - wrap the windowed outer
            // as a derived table so the flatten/join runs only against the LIMITed rows.
            // Sister of HandleInnerJoin / HandleGroupJoin windowed-outer branches (0de442d).
            string? smOuterFromOverride = null;
            if (SourceHasTakeOrSkip(sourceQuery))
            {
                var subSmOuter = TranslateInSubContext(sourceQuery, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out var subSmMap);
                _mapping = subSmMap;
                MergeSubPlanParameters(subSmOuter);
                smOuterFromOverride = subSmOuter.Sql;
                // Skip the Visit(sourceQuery) below - we've already translated it in the
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
                // The flattened element becomes the query's root, but downstream operators
                // (Select/Where/OrderBy/aggregates) resolve members against the DEFAULT
                // alias — which is the OUTER table here, producing references like T0.What
                // for a Chore column. Wrap the flatten join as a derived table under the
                // default alias so the element genuinely is the root. Predicates
                // accumulated so far (the outer source's Where and the navigation filter)
                // reference the join's aliases and must fold inside the wrapper.
                if (_where.Length > 0)
                {
                    _sql.Append(" WHERE ").Append(_where.ToString());
                    _where.Clear();
                }
                var flattened = _sql.ToString();
                _sql.Clear();
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                for (int i = 0; i < innerMapping.Columns.Length; i++)
                {
                    if (i > 0) _sql.Append(", ");
                    _sql.Append(outerAlias).Append('.').Append(innerMapping.Columns[i].EscCol);
                }
                _sql.Append(" FROM (").Append(flattened).Append(") AS ").Append(outerAlias);
                _mapping = innerMapping;
                _rootType = innerMapping.Type;
                // Downstream operators auto-register their lambda parameters against
                // T{_joinCounter}; the derived table collapsed the join back into the
                // default root alias, so the counter must say so.
                _joinCounter = 0;
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

            var vctxCorr = new VisitorContext(_ctx, corrInnerMapping, _provider, corrInnerParam, corrInnerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
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

            var vctxFilter = new VisitorContext(_ctx, innerMapping, _provider, filterParam, innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
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
    }
}
