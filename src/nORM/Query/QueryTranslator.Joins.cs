using System;
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
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);
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
            JoinBuilder.SetupJoinProjection(resultSelector, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(_sql, _projection, _mapping, outerAlias, innerMapping, innerAlias, "INNER JOIN", outerKeySql, innerKeySql, distinct: _isDistinct, outerFromOverride: outerFromOverride);
            return node;
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
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);
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
                        smResultSelector, outerMemberName);
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

            // Check for filtered navigation property: b => b.Posts.Where(p => p.Active)
            // Also check for DefaultIfEmpty for LEFT JOIN: b => b.Posts.DefaultIfEmpty()
            TableMapping.Relation? relation = null;
            LambdaExpression? filterPredicate = null;
            MemberExpression? navigationMember = null;
            bool useLeftJoin = false;

            // Check for DefaultIfEmpty wrapper
            Expression collectionBody = collectionSelector.Body;
            if (collectionBody is MethodCallExpression defaultIfEmptyCall &&
                defaultIfEmptyCall.Method.Name == "DefaultIfEmpty" &&
                defaultIfEmptyCall.Arguments.Count >= 1)
            {
                // Unwrap DefaultIfEmpty to get the actual collection
                collectionBody = defaultIfEmptyCall.Arguments[0];
                useLeftJoin = true;
            }

            if (collectionBody is MemberExpression memberExpr &&
                outerMapping.Relations.TryGetValue(memberExpr.Member.Name, out relation))
            {
                // Simple navigation property without filter
                navigationMember = memberExpr;
            }
            else if (collectionBody is MethodCallExpression methodCall &&
                     methodCall.Method.Name == "Where" &&
                     methodCall.Arguments.Count == 2 &&
                     methodCall.Arguments[0] is MemberExpression navMember &&
                     outerMapping.Relations.TryGetValue(navMember.Member.Name, out relation))
            {
                // Filtered navigation property: b.Posts.Where(p => p.Active)
                navigationMember = navMember;
                filterPredicate = StripQuotes(methodCall.Arguments[1]) as LambdaExpression;
            }

            // Navigation property: treat as INNER JOIN
            if (navigationMember != null && relation != null)
            {
                var innerMapping = TrackMapping(relation.DependentType);
                var innerAlias = EscapeAlias("T" + (++_joinCounter));

                // Register both result selector parameters for transparent identifier support
                if (resultSelector != null && resultSelector.Parameters.Count > 1)
                {
                    if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                        _correlatedParams[resultSelector.Parameters[0]] = (outerMapping, outerAlias);
                    if (!_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                        _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
                }

                // When an outer .Select() projection exists alongside a result selector, try to compose
                // them: expand the outer projection through the result selector so the SELECT only
                // fetches the columns that the final materializer actually needs (transparent id case).
                LambdaExpression? composedProjection = null;
                if (resultSelector != null && _projection != null)
                {
                    var savedProjection = _projection;
                    _projection = resultSelector;
                    var expanded = ExpandProjection(savedProjection);
                    _projection = savedProjection;
                    if (!ReferenceEquals(expanded, savedProjection))
                        composedProjection = expanded;
                }
                var effectiveProjection = composedProjection ?? _projection;

                _sql.Clear();
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                bool wroteColumns = false;

                if (effectiveProjection?.Body is NewExpression newExpr)
                {
                    var neededColumns = JoinBuilder.ExtractNeededColumns(newExpr, outerMapping, innerMapping, outerAlias, innerAlias);
                    if (neededColumns.Count > 0)
                    {
                        for (int i = 0; i < neededColumns.Count; i++)
                        {
                            if (i > 0) _sql.Append(", ");
                            _sql.Append(neededColumns[i]);
                        }
                        wroteColumns = true;
                    }
                }
                else if (resultSelector == null)
                {
                    // No result selector — select only inner columns
                    for (int i = 0; i < innerMapping.Columns.Length; i++)
                    {
                        if (i > 0) _sql.Append(", ");
                        _sql.Append(innerAlias).Append('.').Append(innerMapping.Columns[i].EscCol);
                    }
                    wroteColumns = true;
                }
                else if (resultSelector.Body is NewExpression resultNewExpr)
                {
                    var neededCols = JoinBuilder.ExtractNeededColumns(resultNewExpr, outerMapping, innerMapping, outerAlias, innerAlias);
                    if (neededCols.Count > 0)
                    {
                        for (int i = 0; i < neededCols.Count; i++)
                        {
                            if (i > 0) _sql.Append(", ");
                            _sql.Append(neededCols[i]);
                        }
                        wroteColumns = true;
                    }
                }
                // Bare member-access result selector: `(p, c) => c.Tag`. Emit just the
                // matching column from the correct alias. Without this branch the
                // fallback path selects ALL columns and the scalar materialiser reads
                // column 0 — returning the outer's first column instead of the child's.
                else if (resultSelector.Body is MemberExpression memberSel
                         && memberSel.Expression is ParameterExpression memParam)
                {
                    TableMapping? selMapping = null;
                    string? selAlias = null;
                    if (resultSelector.Parameters.Count > 0 && memParam == resultSelector.Parameters[0])
                    {
                        selMapping = outerMapping; selAlias = outerAlias;
                    }
                    else if (resultSelector.Parameters.Count > 1 && memParam == resultSelector.Parameters[1])
                    {
                        selMapping = innerMapping; selAlias = innerAlias;
                    }
                    if (selMapping != null && selAlias != null
                        && selMapping.ColumnsByName.TryGetValue(memberSel.Member.Name, out var memCol))
                    {
                        _sql.Append(selAlias).Append('.').Append(memCol.EscCol);
                        wroteColumns = true;
                    }
                }

                if (!wroteColumns)
                {
                    // Fallback: select all columns from both tables without LINQ/string interpolation
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

                _sql.Append(' ');
                _sql.Append("FROM ");
                if (smOuterFromOverride != null)
                    _sql.Append('(').Append(smOuterFromOverride).Append(") AS ").Append(outerAlias).Append(' ');
                else
                    _sql.Append(outerMapping.EscTable).Append(' ').Append(outerAlias).Append(' ');
                var joinType = useLeftJoin ? "LEFT JOIN" : "INNER JOIN";
                _sql.Append(joinType).Append(' ').Append(innerMapping.EscTable).Append(' ').Append(innerAlias).Append(' ');
                _sql.Append("ON ").Append(outerAlias).Append('.').Append(relation.PrincipalKey.EscCol)
                    .Append(" = ").Append(innerAlias).Append('.').Append(relation.ForeignKey.EscCol);

                // Apply filter predicate if present
                if (filterPredicate != null)
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

                if (resultSelector != null)
                {
                    _projection = composedProjection ?? resultSelector;
                }
                else
                {
                    _mapping = innerMapping;
                    _rootType = innerMapping.Type;
                }
                return node;
            }
            // Otherwise treat as CROSS JOIN
            var innerType = GetElementType(collectionSelector.Body);
            var crossMapping = TrackMapping(innerType);
            var crossAlias = EscapeAlias("T" + (++_joinCounter));

            // Register both result selector parameters for transparent identifier support
            if (resultSelector != null && resultSelector.Parameters.Count > 1)
            {
                if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                    _correlatedParams[resultSelector.Parameters[0]] = (outerMapping, outerAlias);
                if (!_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                    _correlatedParams[resultSelector.Parameters[1]] = (crossMapping, crossAlias);
            }
            using var crossSql = new OptimizedSqlBuilder(CrossJoinSqlInitialCapacity);
            // When an OUTER projection is already in flight (a later .Select() set _projection),
            // it expresses the FINAL row shape the caller wants and supersedes any transparent
            // identifier the SelectMany may have introduced via `(l, r) => new { l, r }`.
            // Otherwise fall back to the SelectMany's own result selector — which is the
            // common shape for `from l in L from r in R select new { l.X, r.Y }` (no later Select).
            NewExpression? projectionNewExpr = (_projection?.Body as NewExpression) ?? (resultSelector?.Body as NewExpression);
            if (projectionNewExpr != null)
            {
                var neededColumns = JoinBuilder.ExtractNeededColumns(projectionNewExpr, outerMapping, crossMapping, outerAlias, crossAlias);
                if (neededColumns.Count == 0)
                {
                    var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                    crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                    crossSql.AppendJoin(", ", outerCols.Concat(innerCols));
                    crossSql.Append(' ');
                }
                else
                {
                    crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                    crossSql.AppendJoin(", ", neededColumns);
                    crossSql.Append(' ');
                }
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
                var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                crossSql.AppendSelect(ReadOnlySpan<char>.Empty);
                crossSql.AppendJoin(", ", outerCols.Concat(innerCols));
                crossSql.Append(' ');
            }
            if (smOuterFromOverride != null)
                crossSql.Append("FROM (").Append(smOuterFromOverride).Append(") AS ").Append(outerAlias).Append(' ');
            else
                crossSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");
            if (useLeftJoin)
            {
                // For DefaultIfEmpty with CROSS JOIN, use LEFT JOIN with trivial condition
                crossSql.Append($"LEFT JOIN {crossMapping.EscTable} {crossAlias} ON 1=1");
            }
            else
            {
                crossSql.Append($"CROSS JOIN {crossMapping.EscTable} {crossAlias}");
            }
            _sql.Clear();
            _sql.Append(crossSql.ToSqlString());
            if (resultSelector != null)
            {
                // Record the transparent-identifier lambda for ExpandProjection — it lets a
                // downstream Where / Select that consumes `t.l` / `t.r` get rewritten back to
                // the join's outer/inner parameters. Keep _projection alone so the outer
                // Select's materializer projection (set BEFORE source visit) survives.
                _transparentIdentifier = resultSelector;
                if (_projection == null)
                    _projection = resultSelector;
            }
            else
            {
                _mapping = crossMapping;
            }
            return node;
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
            LambdaExpression smResultSelector,
            string outerMemberName)
        {
            Visit(outerQuery);
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
            // Detect `inner == null` / `inner != null` defensive null-checks inside the
            // projection. SQL LEFT JOIN naturally NULLs unmatched right-side columns, so
            // the conditional is semantically redundant — but nORM's visitor doesn't yet
            // recognise the entity-parameter-vs-null shape and silently mis-binds the
            // projected column to the inner's first ordinal (returns the inner's Id
            // instead of the named property). Surface a clear error pointing at the
            // idiomatic bare `c.Tag` workaround.
            if (ContainsEntityNullCheck(newBody, freshInner))
            {
                throw new NormUnsupportedFeatureException(
                    "Defensive `inner == null` / `inner != null` checks inside a LEFT JOIN " +
                    "(GroupJoin + SelectMany + DefaultIfEmpty) projection aren't yet supported — " +
                    "nORM mis-binds the projected column when the conditional wraps an inner-row " +
                    "null test. The check is redundant in SQL anyway: LEFT JOIN already NULLs the " +
                    "unmatched right side, so a downstream `c.Tag` projection returns NULL for the " +
                    "DefaultIfEmpty rows automatically. Workaround: drop the conditional and " +
                    "project the column directly — `select new { p.Name, Tag = c.Tag }` instead of " +
                    "`select new { p.Name, Tag = c == null ? null : c.Tag }`.");
            }
            var rewrittenResultSel = Expression.Lambda(newBody, freshOuter, freshInner);

            JoinBuilder.SetupJoinProjection(rewrittenResultSel, outerMapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);

            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(_sql, _projection, outerMapping, outerAlias, innerMapping, innerAlias, "LEFT JOIN", outerKeySql, innerKeySql, distinct: _isDistinct);
        }

        private static bool ContainsEntityNullCheck(Expression body, ParameterExpression entityParam)
        {
            var visitor = new EntityNullCheckDetector(entityParam);
            visitor.Visit(body);
            return visitor.Found;
        }

        private sealed class EntityNullCheckDetector : ExpressionVisitor
        {
            private readonly ParameterExpression _entity;
            public bool Found { get; private set; }

            public EntityNullCheckDetector(ParameterExpression entity) => _entity = entity;

            protected override Expression VisitBinary(BinaryExpression node)
            {
                if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
                {
                    if (IsEntityVsNull(node.Left, node.Right) || IsEntityVsNull(node.Right, node.Left))
                    {
                        Found = true;
                        return node;
                    }
                }
                return base.VisitBinary(node);
            }

            private bool IsEntityVsNull(Expression maybeEntity, Expression maybeNull)
            {
                return maybeEntity == _entity
                    && maybeNull is ConstantExpression { Value: null };
            }
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
        }
    }
}
