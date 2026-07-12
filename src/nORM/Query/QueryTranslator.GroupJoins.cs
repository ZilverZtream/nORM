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
            // GroupJoin over a Take/Skip-windowed outer source - wrap the windowed outer
            // as a derived table so LEFT JOIN runs against the windowed rows only.
            // Sister of HandleInnerJoin's windowed branch above.
            string? gjOuterFromOverride = null;
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var outerAlias = EscapeAlias("T0");
            var effectiveOuterKeySelector = outerKeySelector;
            var sqlResultSelector = resultSelector;
            var runtimeResultSelector = resultSelector;
            LambdaExpression? sqlProjectionOverride = null;
            var groupJoinOuterIsEntity = true;
            var groupJoinOuterColumnCount = -1;
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
                        "GroupJoin over this `.Distinct()` outer source isn't supported yet. nORM supports " +
                        "`Select(mappedColumn).Distinct().GroupJoin(...)` by emitting a derived-table " +
                        "left join, but this shape is more complex. Workarounds: " +
                        "(1) materialize the distinct keys first and project the right-side groups via Contains; " +
                        "(2) push the GroupJoin through first and apply DISTINCT to the result.");
                }

                _mapping = distinctOuter.Mapping;
                gjOuterFromOverride = distinctOuter.FromSql;
                effectiveOuterKeySelector = distinctOuter.OuterKeySelector;
                sqlResultSelector = distinctOuter.ResultSelector;
                sqlProjectionOverride = CreateScalarGroupJoinSqlProjection(effectiveOuterKeySelector, innerKeySelector);
                groupJoinOuterIsEntity = false;
                groupJoinOuterColumnCount = 1;
            }
            else if (SourceHasTakeOrSkip(outerQuery))
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
                && CurrentPostMaterializeElementType == effectiveOuterKeySelector.Parameters[0].Type)
            {
                AppendPostMaterializeGroupJoin(innerQuery, effectiveOuterKeySelector, innerKeySelector, sqlResultSelector);
                return node;
            }
            var innerAlias = EscapeAlias("T" + (++_joinCounter));
            var sqlOuterKeySelector = ExpandProjection(effectiveOuterKeySelector);
            if (!_correlatedParams.ContainsKey(sqlOuterKeySelector.Parameters[0]))
                _correlatedParams[sqlOuterKeySelector.Parameters[0]] = (_mapping, outerAlias);
            // Composite (anonymous-type) keys: emit one equality per member ANDed into the ON
            // clause (the generic visitor would mash the members into invalid SQL); string
            // members get the ordinal wrap like scalar keys. Segmentation matches by the
            // COMPILED outer key (anonymous types compare by value), so the runtime needs no
            // composite-specific matching — only the SQL and the ordering must be per-member.
            string? gjCompositeOnSql = null;
            var gjCompositeOuterMemberSqls = new List<string>();
            if (sqlOuterKeySelector.Body is NewExpression gjOuterComposite
                && innerKeySelector.Body is NewExpression gjInnerComposite
                && gjOuterComposite.Arguments.Count == gjInnerComposite.Arguments.Count
                && gjOuterComposite.Arguments.Count > 0)
            {
                if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                    _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
                var gjParts = new List<string>(gjOuterComposite.Arguments.Count);
                for (int ci = 0; ci < gjOuterComposite.Arguments.Count; ci++)
                {
                    var vctxGo = new VisitorContext(_ctx, _mapping, _provider, sqlOuterKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                    var goVisitor = FastExpressionVisitorPool.Get(in vctxGo);
                    var outerMemberSql = goVisitor.Translate(gjOuterComposite.Arguments[ci]);
                    foreach (var kvp in goVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(goVisitor);
                    gjCompositeOuterMemberSqls.Add(outerMemberSql);

                    var vctxGi = new VisitorContext(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                    var giVisitor = FastExpressionVisitorPool.Get(in vctxGi);
                    var innerMemberSql = giVisitor.Translate(gjInnerComposite.Arguments[ci]);
                    foreach (var kvp in giVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(giVisitor);

                    gjParts.Add(JoinBuilder.BuildOnEquality(
                        outerMemberSql, innerMemberSql, _provider, gjOuterComposite.Arguments[ci].Type));
                }
                gjCompositeOnSql = string.Join(" AND ", gjParts);
            }

            var vctxOuter = new VisitorContext(_ctx, _mapping, _provider, sqlOuterKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = gjCompositeOnSql != null ? "1" : outerKeyVisitor.Translate(sqlOuterKeySelector.Body);
            // See HandleJoin: AddLiteralParameter so inline constants in the key selector
            // aren't mis-flagged as compiled-runtime placeholders.
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
            JoinBuilder.SetupJoinProjection(null, _mapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);
            // Preserve the result selector so downstream Where/OrderBy on the projected
            // anonymous type (e.g. `(p, cs) => new {Name=p.Name, Count=cs.Count()}` ->
            // later `.OrderBy(r => r.Name)`) can expand `r.Name` back through it. We
            // cannot reuse `_projection` for this - the materialiser would try to build
            // a 2-parameter projection materialiser and crash; the GroupJoin materialiser
            // path uses the compiled `GroupJoinInfo.ResultSelector` Func instead.
            _groupJoinResultSelector = sqlResultSelector;
            _groupJoinExpansionSelector = ComposeGroupJoinExpansionSelector(sqlResultSelector);
            // The result selector's parameter instances are DIFFERENT from the key-selector's
            // (each lambda has its own scope). Pre-register them in _correlatedParams so a
            // downstream OrderBy/Where that's ExpandProjection-ed through this selector
            // resolves `p.Name` against the outer alias (T0) rather than auto-registering
            // with `_joinCounter` (which is now the inner alias index, producing wrong-table
            // references like `T1.Name`).
            if (sqlResultSelector.Parameters.Count >= 1
                && !_correlatedParams.ContainsKey(sqlResultSelector.Parameters[0]))
                _correlatedParams[sqlResultSelector.Parameters[0]] = (_mapping, outerAlias);
            if (sqlResultSelector.Parameters.Count >= 2
                && !_correlatedParams.ContainsKey(sqlResultSelector.Parameters[1]))
                _correlatedParams[sqlResultSelector.Parameters[1]] = (innerMapping, innerAlias);
            if (_groupJoinExpansionSelector.Parameters.Count > sqlResultSelector.Parameters.Count)
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
            JoinBuilder.BuildJoinClauseInto(
                _sql,
                sqlProjectionOverride ?? _projection,
                _mapping,
                outerAlias,
                innerMapping,
                innerAlias,
                "LEFT JOIN",
                outerKeySql,
                innerKeySql,
                orderBy: null,
                distinct: _isDistinct,
                outerFromOverride: gjOuterFromOverride,
                provider: _provider,
                keyClrType: sqlOuterKeySelector.Body.Type,
                onSqlOverride: gjCompositeOnSql);
            // Insert outer-key sort at the front of _orderBy so it is always first. Composite
            // keys order by each member so streaming group segmentation stays contiguous.
            if (gjCompositeOnSql != null)
            {
                for (int oi = gjCompositeOuterMemberSqls.Count - 1; oi >= 0; oi--)
                    _orderBy.Insert(0, (gjCompositeOuterMemberSqls[oi], true));
            }
            else
                _orderBy.Insert(0, (outerKeySql, true));
            // Ordinal string keys on CI-collation providers: CI ordering leaves case variants
            // ("abc"/"ABC") interleaved within a tie, which would split the client-side group
            // segmentation. Order by the binary key second so byte-equal rows stay contiguous.
            if ((Nullable.GetUnderlyingType(sqlOuterKeySelector.Body.Type) ?? sqlOuterKeySelector.Body.Type) == typeof(string)
                && _provider.DefaultStringEqualityIsCaseInsensitive)
                _orderBy.Insert(1, (_provider.ForceCaseSensitiveStringComparison(outerKeySql), true));
            var outerType = runtimeResultSelector.Parameters[0].Type;
            var innerType = innerKeySelector.Parameters[0].Type;
            var resultType = runtimeResultSelector.Body.Type;
            // The runtime uses the inner key column only as a LEFT-JOIN null probe (segmentation
            // matches by the compiled outer key, and anonymous keys compare by value), so any
            // single member of a composite key suffices: a matched inner row has ALL key members
            // non-null (SQL = with NULL never matches).
            var innerKeyProbeBody = innerKeySelector.Body is NewExpression innerCompositeForProbe
                && innerCompositeForProbe.Arguments.Count > 0
                ? innerCompositeForProbe.Arguments[0]
                : innerKeySelector.Body;
            var innerKeyColumn = innerMapping.Columns.FirstOrDefault(c =>
                ExtractPropertyName(innerKeyProbeBody) == c.PropName);
            if (innerKeyColumn != null)
            {
                var outerKeyFunc = CreateObjectKeySelector(outerKeySelector);
                var resultSelectorFunc = CompileGroupJoinResultSelector(runtimeResultSelector);
                _groupJoinInfo = new GroupJoinInfo(
                    outerType,
                    innerType,
                    resultType,
                    outerKeyFunc,
                    innerKeyColumn,
                    resultSelectorFunc,
                    groupJoinOuterIsEntity,
                    groupJoinOuterColumnCount
                );
            }
            return node;
        }

        private static LambdaExpression CreateScalarGroupJoinSqlProjection(
            LambdaExpression outerKeySelector,
            LambdaExpression innerKeySelector)
        {
            var innerParameter = innerKeySelector.Parameters[0];
            var tupleType = typeof(ValueTuple<,>).MakeGenericType(outerKeySelector.Body.Type, innerParameter.Type);
            var ctor = tupleType.GetConstructor(new[] { outerKeySelector.Body.Type, innerParameter.Type })
                ?? throw new NormQueryException("Unable to build GroupJoin SQL projection.");
            var body = Expression.New(ctor, outerKeySelector.Body, innerParameter);
            return Expression.Lambda(body, outerKeySelector.Parameters[0], innerParameter);
        }
    }
}
