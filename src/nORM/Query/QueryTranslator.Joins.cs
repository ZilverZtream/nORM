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
            Visit(outerQuery);
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var outerAlias = EscapeAlias("T0");
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);
            if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);
            foreach (var kvp in outerKeyVisitor.GetParameters())
                AddParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(outerKeyVisitor);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);
            JoinBuilder.SetupJoinProjection(resultSelector, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(_sql, _projection, _mapping, outerAlias, innerMapping, innerAlias, "INNER JOIN", outerKeySql, innerKeySql);
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
            Visit(outerQuery);
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var outerAlias = EscapeAlias("T0");
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);
            if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);
            foreach (var kvp in outerKeyVisitor.GetParameters())
                AddParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(outerKeyVisitor);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);
            JoinBuilder.SetupJoinProjection(null, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
            // Do NOT embed ORDER BY in the SQL string. Instead, insert the outer key as the
            // first ORDER BY entry so that Build() generates exactly one ORDER BY clause.
            // This prevents double ORDER BY when downstream .OrderBy() is chained, and ensures
            // outer-key contiguity (needed for streaming group segmentation) is always first.
            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(_sql, _projection, _mapping, outerAlias, innerMapping, innerAlias, "LEFT JOIN", outerKeySql, innerKeySql, orderBy: null);
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
            // Visit the source query first to establish base mapping
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
                    // No result selector â€” select only inner columns
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
                _sql.Append("FROM ").Append(outerMapping.EscTable).Append(' ').Append(outerAlias).Append(' ');
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
            if (_projection?.Body is NewExpression crossNew)
            {
                var neededColumns = JoinBuilder.ExtractNeededColumns(crossNew, outerMapping, crossMapping, outerAlias, crossAlias);
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
                _projection = resultSelector;
            }
            else
            {
                _mapping = crossMapping;
            }
            return node;
        }
    }
}
