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
            var distinctOuterKeys = false;
            // Entity-shaped `.Distinct()` is a no-op: table rows are key-unique and a
            // dedup-safe chain (no projection or fan-out below it) cannot duplicate
            // them — unwrap so the group join translates against the plain source.
            if (outerQuery is MethodCallExpression entityDistinct
                && entityDistinct.Method.Name == nameof(Queryable.Distinct)
                && entityDistinct.Arguments.Count == 1
                && GetElementType(entityDistinct.Arguments[0]) == outerKeySelector.Parameters[0].Type
                && IsTailWrapEligibleSource(entityDistinct.Arguments[0]))
            {
                outerQuery = entityDistinct.Arguments[0];
            }
            if (outerQuery is MethodCallExpression outerMce
                && outerMce.Method.Name == nameof(Queryable.Distinct))
            {
                if (TryPrepareDistinctScalarJoinOuter(
                        outerQuery,
                        outerKeySelector,
                        resultSelector,
                        outerAlias,
                        out var distinctOuter))
                {
                    _mapping = distinctOuter.Mapping;
                    gjOuterFromOverride = distinctOuter.FromSql;
                    effectiveOuterKeySelector = distinctOuter.OuterKeySelector;
                    sqlResultSelector = distinctOuter.ResultSelector;
                    sqlProjectionOverride = CreateScalarGroupJoinSqlProjection(effectiveOuterKeySelector, innerKeySelector);
                    groupJoinOuterIsEntity = false;
                    groupJoinOuterColumnCount = 1;
                }
                else if (outerMce.Arguments.Count == 1
                    && outerMce.Arguments[0] is MethodCallExpression distinctSelect
                    && distinctSelect.Method.Name == nameof(Queryable.Select)
                    && distinctSelect.Arguments.Count == 2
                    && StripQuotes(distinctSelect.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } distinctProjection)
                {
                    // COMPUTED scalar projection under Distinct: translate the Select-projected
                    // outer as the full entity (the composed-path below handles the projection),
                    // ordered by the computed key then PK, and restore Distinct semantics by
                    // emitting only the first segment per distinct outer key at materialization.
                    // The derived-table branch above cannot express this shape: its outer
                    // references would re-translate the computed body against a derived table
                    // that only exposes the computed output column. Closure captures in the
                    // projection are handled by the outer-key closure lift below (the de-dup
                    // key delegate rebinds per execution like the result selector).
                    outerQuery = outerMce.Arguments[0];
                    distinctOuterKeys = true;
                }
                else
                {
                    throw new NormUnsupportedFeatureException(
                        "GroupJoin over this `.Distinct()` outer source isn't supported yet. nORM supports " +
                        "`Select(...).Distinct().GroupJoin(...)` outers; this shape is more complex. Workarounds: " +
                        "(1) materialize the distinct keys first and project the right-side groups via Contains; " +
                        "(2) push the GroupJoin through first and apply DISTINCT to the result.");
                }
            }
            if (gjOuterFromOverride == null && groupJoinOuterIsEntity)
            {
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
            }
            // A Select-projected outer (scalar, DTO or anonymous shape) without Distinct:
            // GroupJoin still yields one result per outer ROW, and projected keys are NOT
            // distinct, so the outer must materialize as the full entity (PK segmentation
            // keeps duplicate-key outers from fusing) with the projection composed into
            // the key and result selectors client-side.
            LambdaExpression? outerProjection = null;
            if (_projection is { Parameters.Count: 1 } proj
                && proj.Body.Type == resultSelector.Parameters[0].Type
                && proj.Body.Type != _mapping.Type)
            {
                outerProjection = proj;
                _projection = null; // materialize the outer entity; project client-side
                effectiveOuterKeySelector = ComposeThroughOuterProjection(proj, outerKeySelector);
                runtimeResultSelector = ComposeThroughOuterProjection(proj, resultSelector);
                sqlResultSelector = runtimeResultSelector;
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
            // Inner-source WHERE conditions (an explicit filter, or the injected
            // soft-delete/tenant predicates on the inner root) go into the JOIN ON
            // clause — ON, not WHERE, so a filtered-out inner row reads as UNMATCHED
            // and the outer row survives with an empty group instead of vanishing.
            // Without this the grouped rows leak filtered inner rows (another
            // tenant's, or soft-deleted) into the client-side group materializer.
            var gjInnerOnConditions = ExtractInnerWhereConditions(innerQuery, innerMapping, innerAlias);
            var gjAdditionalOnSql = gjInnerOnConditions.Count > 0
                ? string.Join(" AND ", gjInnerOnConditions.Select(c => "(" + c + ")"))
                : null;
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
                additionalOnConditions: gjAdditionalOnSql,
                provider: _provider,
                keyClrType: sqlOuterKeySelector.Body.Type,
                onSqlOverride: gjCompositeOnSql);
            // Insert outer-key sort at the front of _orderBy so it is always first. Composite
            // keys order by each member so streaming group segmentation stays contiguous.
            var keyOrderEntryCount = 1;
            if (gjCompositeOnSql != null)
            {
                for (int oi = gjCompositeOuterMemberSqls.Count - 1; oi >= 0; oi--)
                    _orderBy.Insert(0, (gjCompositeOuterMemberSqls[oi], true));
                keyOrderEntryCount = gjCompositeOuterMemberSqls.Count;
            }
            else
                _orderBy.Insert(0, (outerKeySql, true));
            // Ordinal string keys on CI-collation providers: CI ordering leaves case variants
            // ("abc"/"ABC") interleaved within a tie, which would split the client-side group
            // segmentation. Order by the binary key second so byte-equal rows stay contiguous.
            if ((Nullable.GetUnderlyingType(sqlOuterKeySelector.Body.Type) ?? sqlOuterKeySelector.Body.Type) == typeof(string)
                && _provider.DefaultStringEqualityIsCaseInsensitive)
            {
                _orderBy.Insert(1, (_provider.ForceCaseSensitiveStringComparison(outerKeySql), true));
                keyOrderEntryCount++;
            }
            // GroupJoin yields one result per outer ELEMENT, so distinct outers sharing a key
            // value must not fuse: order by the outer's primary key inside each key tie so
            // every outer row's block is contiguous, and segment on that identity at runtime.
            // Skip a PK column that already IS the join key — SQL Server rejects duplicate
            // ORDER BY columns.
            if (groupJoinOuterIsEntity)
            {
                // PK tiebreak — or, for a keyless outer, EVERY column: segmentation
                // identity below matches this ordering, so rows of the same outer
                // must be contiguous within each join-key tie.
                var tiebreakColumns = _mapping.KeyColumns.Length > 0 ? _mapping.KeyColumns : _mapping.Columns;
                for (int ki = tiebreakColumns.Length - 1; ki >= 0; ki--)
                {
                    var pkSql = $"{outerAlias}.{tiebreakColumns[ki].EscCol}";
                    var alreadyOrdered = false;
                    for (int existing = 0; existing < keyOrderEntryCount; existing++)
                    {
                        if (string.Equals(_orderBy[existing].Item1, pkSql, StringComparison.Ordinal))
                        {
                            alreadyOrdered = true;
                            break;
                        }
                    }
                    if (!alreadyOrdered)
                        _orderBy.Insert(keyOrderEntryCount, (pkSql, true));
                }
            }
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
            // A navigation-member key (e.Dept.Title) is not an inner column, but the probe
            // only answers "did this LEFT JOIN row match?" — the inner PRIMARY KEY answers
            // that for any key shape: a matched row always carries a non-null PK while an
            // unmatched row is all-NULL. Without a probe the plan silently lacked
            // GroupJoinInfo and materialization crashed casting entities to the projection.
            innerKeyColumn ??= innerMapping.KeyColumns.FirstOrDefault();
            // Fail closed instead of skipping GroupJoinInfo: without the LEFT-JOIN
            // match probe the plan silently materialized bare outer entities and
            // crashed casting them to the result type (a computed inner key over a
            // keyless inner entity has no probe candidate).
            if (innerKeyColumn == null)
                throw new NormUnsupportedFeatureException(
                    $"GroupJoin inner key over '{innerMapping.Type.Name}' has no mapped column, and the entity declares " +
                    "no primary key to use as the LEFT-JOIN match probe. Add [Key] or a fluent HasKey to the inner " +
                    "entity, or use a plain column as the inner key.");

            // A composed projected outer materializes the ENTITY, so the runtime key
            // selector must be the entity-composed one, not the projection-typed original.
            // Closure captures inside the key are lifted so the de-dup / segmentation
            // delegate rebinds per execution instead of replaying cached values.
            var runtimeOuterKeySelector = outerProjection != null ? effectiveOuterKeySelector : outerKeySelector;
            var (outerKeyFunc, liftedOuterKey, outerKeySlots) = CompileGroupJoinOuterKeySelector(runtimeOuterKeySelector);
            var (resultSelectorFunc, liftedSelector, closureSlots) = CompileGroupJoinResultSelector(runtimeResultSelector);
            // Segmentation identity: the outer's PK getters — or for a keyless
            // outer, the full column set (structural row identity) so distinct
            // outers sharing a join key value do not fuse. Byte-identical duplicate
            // keyless rows remain indistinguishable (SQL has nothing to tell them
            // apart). Matches the tiebreak ordering inserted above.
            Func<object, object?>? outerIdentityFunc = null;
            if (groupJoinOuterIsEntity)
            {
                var idCols = _mapping.KeyColumns.Length > 0 ? _mapping.KeyColumns : _mapping.Columns;
                outerIdentityFunc = idCols.Length == 1
                    ? idCols[0].Getter
                    : o => new GroupJoinOuterIdentity(Array.ConvertAll(idCols, c => c.Getter(o)));
            }
            _groupJoinInfo = new GroupJoinInfo(
                outerType,
                innerType,
                resultType,
                outerKeyFunc,
                innerKeyColumn,
                resultSelectorFunc,
                groupJoinOuterIsEntity,
                groupJoinOuterColumnCount,
                OuterIdentitySelector: outerIdentityFunc,
                ClosureLiftedResultSelector: liftedSelector,
                ClosureSlotCount: closureSlots,
                DistinctOuterKeys: distinctOuterKeys,
                ClosureLiftedOuterKeySelector: liftedOuterKey,
                OuterKeyClosureSlotCount: outerKeySlots
            );
            return node;
        }

        /// <summary>
        /// Rewrites a lambda whose FIRST parameter is a projected shape into one taking
        /// the projection's source entity instead, by substituting the projection body
        /// for that parameter. Remaining parameters (the GroupJoin inners) pass through.
        /// </summary>
        internal static LambdaExpression ComposeThroughOuterProjection(
            LambdaExpression projection,
            LambdaExpression consumer)
        {
            var newBody = new ParameterReplacer(consumer.Parameters[0], projection.Body).Visit(consumer.Body)!;
            // Simplify member accesses over the substituted projection shape
            // (new { X = e.A }.X -> e.A) so the key translates to plain columns.
            newBody = new ProjectionMemberReplacer().Visit(newBody)!;
            var parameters = new ParameterExpression[consumer.Parameters.Count];
            parameters[0] = projection.Parameters[0];
            for (var i = 1; i < consumer.Parameters.Count; i++)
                parameters[i] = consumer.Parameters[i];
            return Expression.Lambda(newBody, parameters);
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
