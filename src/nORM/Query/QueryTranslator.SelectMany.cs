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

            if (TryHandleCorrelatedSelectMany(outerMapping, outerAlias, collectionBody, resultSelector, useLeftJoin, smOuterFromOverride))
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

            var onParts = new List<string>(relation.PrincipalKeys.Count);
            for (var keyIndex = 0; keyIndex < relation.PrincipalKeys.Count; keyIndex++)
            {
                onParts.Add(
                    $"{outerAlias}.{relation.PrincipalKeys[keyIndex].EscCol} = {innerAlias}.{relation.ForeignKeys[keyIndex].EscCol}");
            }
            var relationOnSql = string.Join(" AND ", onParts);

            if (resultSelector != null && CanEmitSelectManyProjectionSql(resultSelector))
            {
                // See TryHandleCorrelatedSelectMany: computed selector members emit as
                // translated SQL items so the positional materializer lines up; the
                // needed-columns emission diverged from the constructor arguments as
                // soon as a member was computed. The navigation filter goes into the
                // ON clause — a DefaultIfEmpty LEFT JOIN keeps its unmatched rows only
                // when the filter gates the join, not the result.
                var navRewriter = new QuerySyntaxLeftJoinRewriter(
                    resultSelector.Parameters[0], resultSelector.Parameters[1],
                    string.Empty, resultSelector.Parameters[0], resultSelector.Parameters[1]);
                var rewrittenNavSelector = Expression.Lambda(navRewriter.Visit(resultSelector.Body)!, resultSelector.Parameters);

                var onConditions = new List<string>();
                if (RenderSelectManyInnerGlobalFilterSql(innerMapping, innerAlias) is { } navGlobalFilterSql)
                    onConditions.Add("(" + navGlobalFilterSql + ")");
                if (filterPredicate != null)
                    onConditions.Add("(" + TranslateNavigationFilterPredicate(filterPredicate, innerMapping, innerAlias) + ")");

                var navTransparent = ComposeTransparentIdentifierSelector(resultSelector);
                JoinBuilder.SetupJoinProjection(rewrittenNavSelector, outerMapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
                _transparentIdentifier = navTransparent;
                RegisterTransparentIdentifierTail(navTransparent, innerMapping, innerAlias);

                _sql.Clear();
                JoinBuilder.BuildJoinClauseInto(
                    _sql,
                    _projection,
                    outerMapping,
                    outerAlias,
                    innerMapping,
                    innerAlias,
                    useLeftJoin ? "LEFT JOIN" : "INNER JOIN",
                    outerKeySql: "1",
                    innerKeySql: "1",
                    distinct: _isDistinct,
                    outerFromOverride: outerFromOverride != null ? "(" + outerFromOverride + ") AS " + outerAlias : null,
                    additionalOnConditions: onConditions.Count > 0 ? string.Join(" AND ", onConditions) : null,
                    translateProjectionExpression: TranslateJoinProjectionExpression,
                    escapeProjectionAlias: _provider.Escape,
                    provider: _provider,
                    keyClrType: null,
                    onSqlOverride: relationOnSql);
                return true;
            }

            var composedProjection = ComposeSelectManyProjection(resultSelector);
            var effectiveProjection = composedProjection ?? _projection;

            _sql.Clear();
            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            if (!AppendNavigationSelectManyColumns(effectiveProjection, resultSelector, outerMapping, innerMapping, outerAlias, innerAlias))
                AppendSelectManyAllColumns(outerMapping, outerAlias, innerMapping, innerAlias);

            _sql.Append(' ');
            AppendSelectManyOuterFrom(outerMapping, outerAlias, outerFromOverride);
            var joinType = useLeftJoin ? "LEFT JOIN" : "INNER JOIN";
            _sql.Append(joinType).Append(' ').Append(TemporalTableSource(innerMapping)).Append(' ').Append(innerAlias).Append(' ');
            _sql.Append("ON ").Append(relationOnSql);

            // The child's global filters (soft-delete, tenant) must gate the flattened rows.
            // Without a result selector the flatten's RESULT type is the child itself and the
            // provider-level rewrite wraps the whole query in the child's filter; a result
            // selector projects an unmapped shape that rewrite cannot express, so the filter
            // goes into the ON clause here (ON, not WHERE, so a DefaultIfEmpty LEFT JOIN
            // keeps its unmatched rows).
            if (resultSelector != null
                && RenderSelectManyInnerGlobalFilterSql(innerMapping, innerAlias) is { } childFilterSql)
            {
                _sql.Append(" AND (").Append(childFilterSql).Append(')');
            }

            if (filterPredicate != null)
            {
                if (useLeftJoin)
                {
                    // The navigation filter gates the JOIN, not the result set: in the
                    // WHERE clause it would evaluate UNKNOWN over the unmatched rows'
                    // NULLs and silently drop the very rows DefaultIfEmpty keeps.
                    _sql.Append(" AND (").Append(TranslateNavigationFilterPredicate(filterPredicate, innerMapping, innerAlias)).Append(')');
                }
                else
                {
                    AppendSelectManyFilterPredicate(filterPredicate, innerMapping, innerAlias);
                }
            }

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
                if (useLeftJoin)
                    _flattenedLeftJoinEntityResult = true;
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
            bool useLeftJoin,
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

            // The collection source (`ctx.Query<Child>()` / a captured IQueryable local)
            // roots in closure member accesses that ParameterValueExtractor collects at
            // execution time. Translation consumes the source structurally without
            // visiting it, so matching placeholder slots must be reserved here or every
            // later @cp binding shifts onto the wrong closure value.
            ReserveClosureSlots(corrWhereCall.Arguments[0]);

            var vctxCorr = new VisitorContext(_ctx, corrInnerMapping, _provider, corrInnerParam, corrInnerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var corrPredVisitor = FastExpressionVisitorPool.Get(in vctxCorr);
            var corrPredSql = corrPredVisitor.Translate(corrPredLambda.Body);
            foreach (var kvp in corrPredVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            FastExpressionVisitorPool.Return(corrPredVisitor);

            if (resultSelector != null && CanEmitSelectManyProjectionSql(resultSelector))
            {
                // The projection emits one SQL item per selector member (computed
                // members become expressions, null-guard conditionals strip to the
                // member or a COALESCE — the LEFT JOIN already NULLs the unmatched
                // side), so the positional constructor materializer lines up by
                // construction. The needed-columns emission it replaces produced a
                // SELECT list that diverged from the constructor arguments as soon
                // as a member was computed.
                var rewriter = new QuerySyntaxLeftJoinRewriter(
                    resultSelector.Parameters[0], resultSelector.Parameters[1],
                    string.Empty, resultSelector.Parameters[0], resultSelector.Parameters[1]);
                var rewrittenSelector = Expression.Lambda(rewriter.Visit(resultSelector.Body)!, resultSelector.Parameters);

                var corrFilterSql = RenderSelectManyInnerGlobalFilterSql(corrInnerMapping, corrInnerAlias);
                var transparentExpansion = ComposeTransparentIdentifierSelector(resultSelector);
                JoinBuilder.SetupJoinProjection(rewrittenSelector, outerMapping, corrInnerMapping, outerAlias, corrInnerAlias, _correlatedParams, ref _projection);
                _transparentIdentifier = transparentExpansion;
                RegisterTransparentIdentifierTail(transparentExpansion, corrInnerMapping, corrInnerAlias);

                _sql.Clear();
                JoinBuilder.BuildJoinClauseInto(
                    _sql,
                    _projection,
                    outerMapping,
                    outerAlias,
                    corrInnerMapping,
                    corrInnerAlias,
                    useLeftJoin ? "LEFT JOIN" : "INNER JOIN",
                    outerKeySql: "1",
                    innerKeySql: "1",
                    distinct: _isDistinct,
                    outerFromOverride: outerFromOverride != null ? "(" + outerFromOverride + ") AS " + outerAlias : null,
                    additionalOnConditions: corrFilterSql != null ? "(" + corrFilterSql + ")" : null,
                    translateProjectionExpression: TranslateJoinProjectionExpression,
                    escapeProjectionAlias: _provider.Escape,
                    provider: _provider,
                    keyClrType: null,
                    onSqlOverride: corrPredSql);
                return true;
            }

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
            _sql.Append(useLeftJoin ? "LEFT JOIN " : "INNER JOIN ").Append(TemporalTableSource(corrInnerMapping)).Append(' ').Append(corrInnerAlias).Append(' ');
            _sql.Append("ON ").Append(corrPredSql);
            // See TryHandleNavigationSelectMany: a result selector's unmapped projection
            // shape means the provider-level rewrite cannot carry the inner's filter.
            if (resultSelector != null
                && RenderSelectManyInnerGlobalFilterSql(corrInnerMapping, corrInnerAlias) is { } corrWholeFilterSql)
            {
                _sql.Append(" AND (").Append(corrWholeFilterSql).Append(')');
            }

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
                if (useLeftJoin)
                    _flattenedLeftJoinEntityResult = true;
            }

            return true;
        }

        /// <summary>
        /// A SelectMany result selector routes through the join projection emission
        /// (one translated SQL item per member) only when every member is an
        /// expression the SQL visitor can render. Bare entity parameters
        /// (`(p, c) => new { p, c }`) keep the legacy whole-entity column emission.
        /// </summary>
        private static bool CanEmitSelectManyProjectionSql(LambdaExpression resultSelector)
            => resultSelector.Body is NewExpression newExpr
               && newExpr.Arguments.Count > 0
               && newExpr.Arguments.All(a => a is not ParameterExpression);

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
            // See TryHandleCorrelatedSelectMany: the captured queryable source's closure
            // members need placeholder compiled-param slots for extractor alignment.
            ReserveClosureSlots(collectionSelector.Body);

            if (resultSelector != null && CanEmitSelectManyProjectionSql(resultSelector))
            {
                // See TryHandleCorrelatedSelectMany: computed selector members emit as
                // translated SQL items so the positional materializer lines up. A cross
                // join is an inner join on a tautology; DefaultIfEmpty keeps unmatched
                // outers via LEFT JOIN on the same tautology.
                var crossRewriter = new QuerySyntaxLeftJoinRewriter(
                    resultSelector.Parameters[0], resultSelector.Parameters[1],
                    string.Empty, resultSelector.Parameters[0], resultSelector.Parameters[1]);
                var rewrittenCrossSelector = Expression.Lambda(crossRewriter.Visit(resultSelector.Body)!, resultSelector.Parameters);

                var crossJoinFilterSql = RenderSelectManyInnerGlobalFilterSql(crossMapping, crossAlias);
                var crossTransparent = ComposeTransparentIdentifierSelector(resultSelector);
                JoinBuilder.SetupJoinProjection(rewrittenCrossSelector, outerMapping, crossMapping, outerAlias, crossAlias, _correlatedParams, ref _projection);
                _transparentIdentifier = crossTransparent;
                RegisterTransparentIdentifierTail(crossTransparent, crossMapping, crossAlias);

                _sql.Clear();
                JoinBuilder.BuildJoinClauseInto(
                    _sql,
                    _projection,
                    outerMapping,
                    outerAlias,
                    crossMapping,
                    crossAlias,
                    useLeftJoin ? "LEFT JOIN" : "INNER JOIN",
                    outerKeySql: "1",
                    innerKeySql: "1",
                    distinct: _isDistinct,
                    outerFromOverride: outerFromOverride != null ? "(" + outerFromOverride + ") AS " + outerAlias : null,
                    additionalOnConditions: crossJoinFilterSql != null ? "(" + crossJoinFilterSql + ")" : null,
                    translateProjectionExpression: TranslateJoinProjectionExpression,
                    escapeProjectionAlias: _provider.Escape,
                    provider: _provider,
                    keyClrType: null,
                    onSqlOverride: "1=1");
                return;
            }

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
                crossSql.Append($"FROM {TemporalTableSource(outerMapping)} {outerAlias} ");

            // See TryHandleNavigationSelectMany: a result selector's unmapped projection
            // shape means the provider-level rewrite cannot carry the inner's filter. A
            // filtered CROSS JOIN becomes an INNER JOIN on the filter — same rowset.
            var crossFilterSql = resultSelector != null
                ? RenderSelectManyInnerGlobalFilterSql(crossMapping, crossAlias)
                : null;
            if (useLeftJoin)
            {
                crossSql.Append($"LEFT JOIN {TemporalTableSource(crossMapping)} {crossAlias} ON 1=1");
                if (crossFilterSql != null)
                    crossSql.Append($" AND ({crossFilterSql})");
            }
            else if (crossFilterSql != null)
            {
                crossSql.Append($"INNER JOIN {TemporalTableSource(crossMapping)} {crossAlias} ON ({crossFilterSql})");
            }
            else
            {
                crossSql.Append($"CROSS JOIN {TemporalTableSource(crossMapping)} {crossAlias}");
            }

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
                if (useLeftJoin)
                    _flattenedLeftJoinEntityResult = true;
            }
        }

        /// <summary>
        /// Translates the inner entity's combined global filter against the join alias.
        /// Every SelectMany arm needs this when a result selector is present: the
        /// provider-level filter rewrite wraps only queries whose RESULT element type is
        /// the (mapped) inner entity, and it never descends into the lambda arguments
        /// where correlated/cross query sources live.
        /// </summary>
        private string? RenderSelectManyInnerGlobalFilterSql(TableMapping innerMapping, string innerAlias)
        {
            if (_ctx == null)
                return null;
            // Full row visibility (global filters AND tenant): the inner source sits
            // inside the collection-selector lambda, which the provider's top-level
            // filter/tenant rewrite never reaches.
            var combined = GlobalFilterFragment.CombineWithTenant(_ctx, innerMapping.Type);
            if (combined == null)
                return null;
            var vctx = new VisitorContext(_ctx, innerMapping, _provider, combined.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            try
            {
                var sql = visitor.Translate(combined.Body);
                foreach (var kvp in visitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                return sql;
            }
            finally
            {
                FastExpressionVisitorPool.Return(visitor);
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
            var filterSql = TranslateNavigationFilterPredicate(filterPredicate, innerMapping, innerAlias);
            if (_where.Length > 0)
                _where.Append(" AND ");
            _where.Append('(').Append(filterSql).Append(')');
        }

        /// <summary>
        /// Translates a navigation filter (`p.Kids.Where(c => ...)`) against the
        /// flatten join's inner alias. The caller decides placement: WHERE for an
        /// inner-join flatten, ON for a DefaultIfEmpty LEFT JOIN.
        /// </summary>
        private string TranslateNavigationFilterPredicate(
            LambdaExpression filterPredicate,
            TableMapping innerMapping,
            string innerAlias)
        {
            var filterParam = filterPredicate.Parameters[0];
            if (!_correlatedParams.ContainsKey(filterParam))
                _correlatedParams[filterParam] = (innerMapping, innerAlias);

            var vctxFilter = new VisitorContext(_ctx, innerMapping, _provider, filterParam, innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var filterVisitor = FastExpressionVisitorPool.Get(in vctxFilter);
            try
            {
                var filterSql = filterVisitor.Translate(filterPredicate.Body);
                foreach (var kvp in filterVisitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                return filterSql;
            }
            finally
            {
                FastExpressionVisitorPool.Return(filterVisitor);
            }
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
                _sql.Append(TemporalTableSource(outerMapping)).Append(' ').Append(outerAlias).Append(' ');
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
        /// Reserves one unused compiled-param placeholder per closure member access in
        /// <paramref name="expression"/>. ParameterValueExtractor walks every closure
        /// member in the query tree at execution time; an expression that translation
        /// consumes structurally (a SelectMany collection source like
        /// <c>ctx.Query&lt;Child&gt;()</c> or a captured IQueryable local) never gets
        /// visited by the SQL visitor, so without placeholders the extracted value
        /// list shifts and later @cp parameters bind the wrong closure values.
        /// </summary>
        private void ReserveClosureSlots(Expression expression)
            => new ClosureSlotReserver(this).Visit(expression);

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure slot reservation evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure slot reservation reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ClosureSlotReserver : ExpressionVisitor
        {
            private readonly QueryTranslator _t;
            public ClosureSlotReserver(QueryTranslator t) => _t = t;

            protected override Expression VisitConstant(ConstantExpression node) => node;

            protected override Expression VisitMember(MemberExpression node)
            {
                // Mirrors ParameterValueExtractor.VisitMember: one slot per closure
                // member, no descent into its children.
                if (TryGetConstantValue(node, out _))
                {
                    var placeholder = $"{_t._provider.ParamPrefix}cp{_t._compiledParams.Count}_unused";
                    _t._params[placeholder] = DBNull.Value;
                    _t._compiledParams.Add(placeholder);
                    return node;
                }
                return base.VisitMember(node);
            }
        }
    }
}
