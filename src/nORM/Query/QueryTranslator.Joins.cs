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
            // Join over a Take/Skip-windowed outer source - wrap the windowed outer as a
            // derived table so the join matches only the LIMITed/Skipped outer rows.
            // Sister of the post-Take/Skip silent-wrongness family.
            string? outerFromOverride = null;
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var outerAlias = EscapeAlias("T0");
            var effectiveOuterKeySelector = outerKeySelector;
            var effectiveResultSelector = resultSelector;
            if (outerQuery is MethodCallExpression outerMce
                && outerMce.Method.Name == nameof(Queryable.Distinct))
            {
                if (!TryPrepareDistinctScalarJoinOuter(
                        outerQuery,
                        outerKeySelector,
                        resultSelector,
                        outerAlias,
                        out var distinctOuter))
                {
                    throw new NormUnsupportedFeatureException(
                        "Join over this `.Distinct()` outer source isn't supported yet. nORM supports " +
                        "`Select(mappedColumn).Distinct().Join(...)` by emitting a derived-table join, " +
                        "but this shape is more complex. Workarounds: " +
                        "(1) materialize the distinct keys first and feed them through Contains: " +
                        "`var keys = await ctx.Query<L>().Select(l => l.Code).Distinct().ToListAsync();` " +
                        "then `ctx.Query<R>().Where(r => keys.Contains(r.Code)).ToListAsync()`; " +
                        "(2) push the join through first and apply DISTINCT to the result: " +
                        "`ctx.Query<L>().Join(ctx.Query<R>(), l => l.Code, r => r.Code, (l, r) => new {...}).Distinct()`.");
                }

                _mapping = distinctOuter.Mapping;
                outerFromOverride = distinctOuter.FromSql;
                effectiveOuterKeySelector = distinctOuter.OuterKeySelector;
                effectiveResultSelector = distinctOuter.ResultSelector;
            }
            else if (SourceHasTakeOrSkip(outerQuery))
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
            var projectsWholeEntity = effectiveResultSelector.Body is NewExpression wholeEntityNew
                                      && wholeEntityNew.Arguments.Any(a => a == effectiveResultSelector.Parameters[0] || a == effectiveResultSelector.Parameters[1]);
            if ((IsPostMaterializeTailMode
                 || projectsWholeEntity
                 || outerQuery is MethodCallExpression chainedJoinSource && chainedJoinSource.Method.Name == nameof(Queryable.Join))
                && currentOuterType == effectiveOuterKeySelector.Parameters[0].Type)
            {
                AppendPostMaterializeInnerJoin(innerQuery, effectiveOuterKeySelector, innerKeySelector, effectiveResultSelector);
                return node;
            }
            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == effectiveOuterKeySelector.Parameters[0].Type)
            {
                AppendPostMaterializeGroupJoin(innerQuery, effectiveOuterKeySelector, innerKeySelector, effectiveResultSelector);
                return node;
            }
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            var sqlOuterKeySelector = ExpandProjection(effectiveOuterKeySelector);
            if (!_correlatedParams.ContainsKey(sqlOuterKeySelector.Parameters[0]))
                _correlatedParams[sqlOuterKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, sqlOuterKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
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
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);
            var innerOnConditions = ExtractInnerWhereConditions(innerQuery, innerMapping, innerAlias);
            var additionalOnSql = innerOnConditions.Count > 0
                ? string.Join(" AND ", innerOnConditions.Select(c => "(" + c + ")"))
                : null;
            var transparentExpansion = ComposeTransparentIdentifierSelector(effectiveResultSelector);
            JoinBuilder.SetupJoinProjection(effectiveResultSelector, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
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
                escapeProjectionAlias: _provider.Escape,
                provider: _provider,
                keyClrType: sqlOuterKeySelector.Body.Type);
            return node;
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
                        _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
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
