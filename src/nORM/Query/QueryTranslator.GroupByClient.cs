using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private static Func<object, object> CreateObjectKeySelector(LambdaExpression keySelector)
        {
            var parameterType = keySelector.Parameters[0].Type;
            var objParam = Expression.Parameter(typeof(object), "obj");
            var castParam = Expression.Convert(objParam, parameterType);
            var body = new ParameterReplacer(keySelector.Parameters[0], castParam).Visit(keySelector.Body)!;
            var convertBody = Expression.Convert(body, typeof(object));
            var lambda = Expression.Lambda<Func<object, object>>(convertBody, objParam);
            ExpressionUtils.ValidateExpression(lambda);
            var timeout = ExpressionUtils.GetCompilationTimeout(lambda);
            using var cts = new CancellationTokenSource(timeout);
            var invoker = ExpressionUtils.CompileWithFallback(lambda, cts.Token);
            return obj =>
            {
                try
                {
                    var result = invoker(obj);
                    return result ?? (object)DBNull.Value;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Error executing key selector for type {parameterType.Name}: {ex.Message}", ex);
                }
            };
        }
        private static (Func<object, IEnumerable<object>, object> Selector,
                        Func<object, IEnumerable<object>, object?[], object>? Lifted,
                        int ClosureSlotCount)
            CompileGroupJoinResultSelector(LambdaExpression resultSelector)
        {
            var outerParam = Expression.Parameter(typeof(object), "outer");
            var innerParam = Expression.Parameter(typeof(IEnumerable<object>), "inners");
            var valuesParam = Expression.Parameter(typeof(object?[]), "closureValues");
            var castOuter = Expression.Convert(outerParam, resultSelector.Parameters[0].Type);
            var innerParamType = resultSelector.Parameters[1].Type;
            var innerGenericArgs = innerParamType.GetGenericArguments();
            if (innerGenericArgs.Length == 0)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    $"GroupJoin result selector parameter '{resultSelector.Parameters[1].Name}' must be a generic IEnumerable<T>, but was '{innerParamType.Name}'."));
            var innerElementType = innerGenericArgs[0];
            var castMethod = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(innerElementType);
            var castInner = Expression.Call(castMethod, innerParam);
            Expression body = resultSelector.Body;
            // Lift closure captures FIRST, over the original body, in the same
            // pre-order traversal GroupJoinClosureValues.Collect uses on the
            // current expression at execution time — slots bind positionally.
            // Without the lift, the compiled delegate holds the FIRST execution's
            // display-class instance and every later execution replays its values
            // out of the plan cache.
            var lifter = new ClosureLiftRewriter(valuesParam);
            body = lifter.Visit(body)!;
            body = new ParameterReplacer(resultSelector.Parameters[0], castOuter).Visit(body)!;
            body = new ParameterReplacer(resultSelector.Parameters[1], castInner).Visit(body)!;
            body = Expression.Convert(body, typeof(object));

            if (lifter.SlotCount == 0)
            {
                var lambda = Expression.Lambda<Func<object, IEnumerable<object>, object>>(body, outerParam, innerParam);
                ExpressionUtils.ValidateExpression(lambda);
                var timeout = ExpressionUtils.GetCompilationTimeout(lambda);
                using var cts = new CancellationTokenSource(timeout);
                return (ExpressionUtils.CompileWithFallback(lambda, cts.Token), null, 0);
            }

            var liftedLambda = Expression.Lambda<Func<object, IEnumerable<object>, object?[], object>>(
                body, outerParam, innerParam, valuesParam);
            ExpressionUtils.ValidateExpression(liftedLambda);
            var liftedTimeout = ExpressionUtils.GetCompilationTimeout(liftedLambda);
            using var liftedCts = new CancellationTokenSource(liftedTimeout);
            var lifted = ExpressionUtils.CompileWithFallback(liftedLambda, liftedCts.Token);

            // Bind the translating expression's own values so the selector is
            // correct even on a path that never rebinds.
            var initialValues = new List<object?>(lifter.SlotCount);
            GroupJoinClosureValues.Collect(resultSelector.Body, initialValues);
            var initial = initialValues.ToArray();
            return ((o, cs) => lifted(o, cs, initial), lifted, lifter.SlotCount);
        }

        /// <summary>
        /// Compiles the GroupJoin outer KEY selector with the same closure-lift
        /// treatment as the result selector: captures become positional slot reads
        /// (pre-order, matching <see cref="GroupJoinClosureValues.Collect"/>) so the
        /// de-dup / segmentation key delegate can be rebound per execution instead of
        /// replaying the first execution's captured values out of the plan cache.
        /// </summary>
        private static (Func<object, object?> Selector,
                        Func<object, object?[], object?>? Lifted,
                        int SlotCount)
            CompileGroupJoinOuterKeySelector(LambdaExpression keySelector)
        {
            var objParam = Expression.Parameter(typeof(object), "obj");
            var valuesParam = Expression.Parameter(typeof(object?[]), "closureValues");
            var castParam = Expression.Convert(objParam, keySelector.Parameters[0].Type);
            var lifter = new ClosureLiftRewriter(valuesParam);
            var body = lifter.Visit(keySelector.Body)!;
            if (lifter.SlotCount == 0)
                return (CreateObjectKeySelector(keySelector), null, 0);

            body = new ParameterReplacer(keySelector.Parameters[0], castParam).Visit(body)!;
            var boxed = Expression.Convert(body, typeof(object));
            var liftedLambda = Expression.Lambda<Func<object, object?[], object?>>(boxed, objParam, valuesParam);
            ExpressionUtils.ValidateExpression(liftedLambda);
            var timeout = ExpressionUtils.GetCompilationTimeout(liftedLambda);
            using var cts = new CancellationTokenSource(timeout);
            var lifted = ExpressionUtils.CompileWithFallback(liftedLambda, cts.Token);

            // Bind the translating expression's own values so the selector is
            // correct even on a path that never rebinds.
            var initialValues = new List<object?>(lifter.SlotCount);
            GroupJoinClosureValues.Collect(keySelector.Body, initialValues);
            var initial = initialValues.ToArray();
            return (o => lifted(o, initial), lifted, lifter.SlotCount);
        }

        /// <summary>
        /// Rewrites closure captures (member accesses rooted in a constant) to
        /// positional reads from the lifted values array, so the compiled GroupJoin
        /// result selector is closure-free and per-execution values can be rebound.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure lifting evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure lifting reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ClosureLiftRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _values;
            public int SlotCount { get; private set; }

            public ClosureLiftRewriter(ParameterExpression values) => _values = values;

            protected override Expression VisitConstant(ConstantExpression node) => node;

            protected override Expression VisitMember(MemberExpression node)
            {
                if (TryGetConstantValue(node, out _))
                {
                    var slot = Expression.ArrayIndex(_values, Expression.Constant(SlotCount++));
                    return Expression.Convert(slot, node.Type);
                }
                return base.VisitMember(node);
            }
        }
        /// <summary>
        /// A streaming/client GroupBy compiles its key selector and runs it over MATERIALIZED entities
        /// whose reference navigations are NOT loaded. `c.Parent.Id` (principal key via a nav) would
        /// dereference a null nav and NullReferenceException; rewrite it to the foreign-key column the
        /// dependent already carries (`c.Parent.Id` -> `c.ParentId`), which is a materialized column.
        /// </summary>
        private LambdaExpression RewriteNavigationKeyToForeignKey(LambdaExpression keySelector)
        {
            TableMapping ownerMap;
            try { ownerMap = _ctx.GetMapping(keySelector.Parameters[0].Type); }
            catch { return keySelector; }
            var rewriter = new NavKeyToForeignKeyRewriter(_ctx, ownerMap);
            var newBody = rewriter.Visit(keySelector.Body);
            return newBody == keySelector.Body ? keySelector : Expression.Lambda(newBody!, keySelector.Parameters);
        }

        /// <summary>
        /// Fails loud (instead of a runtime NullReferenceException) when a client-side grouping key still
        /// dereferences a reference navigation after the FK rewrite — the nav value isn't in the
        /// materialized row, so it cannot be computed client-side.
        /// </summary>
        private void EnsureNoUnresolvedNavigationInKey(LambdaExpression keySelector)
        {
            TableMapping ownerMap;
            try { ownerMap = _ctx.GetMapping(keySelector.Parameters[0].Type); }
            catch { return; }
            var finder = new UnresolvedNavigationFinder(keySelector.Parameters[0], ownerMap);
            finder.Visit(keySelector.Body);
            if (finder.FoundNavigation != null)
                throw new NormUnsupportedFeatureException(
                    $"GroupBy key references navigation property '{finder.FoundNavigation}', which this query shape " +
                    "would have to group client-side over entities whose navigations are not loaded. Group by the " +
                    "foreign-key column (e.g. the '...Id' property) instead, or restructure with an explicit join.");
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class NavKeyToForeignKeyRewriter : ExpressionVisitor
        {
            private readonly DbContext _ctx;
            private readonly TableMapping _ownerMap;
            public NavKeyToForeignKeyRewriter(DbContext ctx, TableMapping ownerMap) { _ctx = ctx; _ownerMap = ownerMap; }

            protected override Expression VisitMember(MemberExpression node)
            {
                // Pattern: <nav>.<principalKey> where <nav> is a reference navigation on the owner.
                if (node.Expression is MemberExpression navMember
                    && navMember.Member is System.Reflection.PropertyInfo navProp
                    && navMember.Expression != null
                    && !_ownerMap.ColumnsByName.ContainsKey(navProp.Name)
                    && navProp.PropertyType.IsClass && navProp.PropertyType != typeof(string))
                {
                    TableMapping? principalMap = null;
                    try { principalMap = _ctx.GetMapping(navProp.PropertyType); } catch { }
                    if (principalMap != null && principalMap.KeyColumns.Length == 1
                        && string.Equals(node.Member.Name, principalMap.KeyColumns[0].PropName, StringComparison.Ordinal))
                    {
                        var fkCol = ExpressionToSqlVisitor.FindReferenceNavForeignKey(_ownerMap, navProp.Name, navProp.PropertyType, principalMap);
                        if (fkCol != null)
                        {
                            Expression fkAccess = Expression.MakeMemberAccess(navMember.Expression, fkCol.Prop);
                            if (fkAccess.Type != node.Type)
                                fkAccess = Expression.Convert(fkAccess, node.Type);
                            return fkAccess;
                        }
                    }
                }
                return base.VisitMember(node);
            }
        }

        private sealed class UnresolvedNavigationFinder : ExpressionVisitor
        {
            private readonly ParameterExpression _param;
            private readonly TableMapping _ownerMap;
            public string? FoundNavigation { get; private set; }
            public UnresolvedNavigationFinder(ParameterExpression param, TableMapping ownerMap) { _param = param; _ownerMap = ownerMap; }

            protected override Expression VisitMember(MemberExpression node)
            {
                // A member access whose receiver is `param.<nonColumn>` is a navigation traversal.
                if (FoundNavigation == null
                    && node.Expression is MemberExpression inner
                    && inner.Expression == _param
                    && inner.Member is System.Reflection.PropertyInfo innerProp
                    && !_ownerMap.ColumnsByName.ContainsKey(innerProp.Name)
                    && innerProp.PropertyType.IsClass && innerProp.PropertyType != typeof(string))
                {
                    FoundNavigation = innerProp.Name;
                }
                return base.VisitMember(node);
            }
        }

        /// <summary>
        /// Compiles <paramref name="keySelectorLambda"/> into a client-side grouping transform
        /// that groups a materialized entity list by key, returning a typed
        /// <c>List&lt;IGrouping&lt;K, V&gt;&gt;</c>.  Stored as
        /// <see cref="_postMaterializeTransform"/> so the normal entity materializer handles
        /// row reading; the transform runs once after all rows are in memory.
        /// </summary>
        private void InstallGroupingTransform(LambdaExpression keySelectorLambda, LambdaExpression? elementSelectorLambda = null)
        {
            // The key selector runs client-side over materialized entities whose reference navigations
            // are not loaded; rewrite `nav.PrincipalKey` to the local FK column and fail loud (not NRE)
            // if a non-key nav member survives.
            keySelectorLambda = RewriteNavigationKeyToForeignKey(keySelectorLambda);
            EnsureNoUnresolvedNavigationInKey(keySelectorLambda);
            var entityType = keySelectorLambda.Parameters[0].Type;
            var keyType = keySelectorLambda.Body.Type;
            var elementType = elementSelectorLambda?.Body.Type ?? entityType;

            // Compile key selector: entity - boxed key (object?)
            var objParam = Expression.Parameter(typeof(object), "e");
            var castEntity = Expression.Convert(objParam, entityType);
            var reboundKey = new ParameterReplacer(keySelectorLambda.Parameters[0], castEntity).Visit(keySelectorLambda.Body)!;
            var boxedKey = keyType.IsValueType ? (Expression)Expression.Convert(reboundKey, typeof(object)) : reboundKey;
            var keyFuncExpr = Expression.Lambda<Func<object, object?>>(boxedKey, objParam);
            ExpressionUtils.ValidateExpression(keyFuncExpr);
            var timeout = ExpressionUtils.GetCompilationTimeout(keyFuncExpr);
            using var cts = new CancellationTokenSource(timeout);
            var keyFunc = ExpressionUtils.CompileWithFallback(keyFuncExpr, cts.Token);

            Func<object, object?> elementFunc;
            if (elementSelectorLambda != null)
            {
                var elementObjParam = Expression.Parameter(typeof(object), "e");
                var castElementEntity = Expression.Convert(elementObjParam, entityType);
                var reboundElement = new ParameterReplacer(elementSelectorLambda.Parameters[0], castElementEntity).Visit(elementSelectorLambda.Body)!;
                var boxedElement = elementType.IsValueType ? (Expression)Expression.Convert(reboundElement, typeof(object)) : reboundElement;
                var elementFuncExpr = Expression.Lambda<Func<object, object?>>(boxedElement, elementObjParam);
                ExpressionUtils.ValidateExpression(elementFuncExpr);
                var elementTimeout = ExpressionUtils.GetCompilationTimeout(elementFuncExpr);
                using var elementCts = new CancellationTokenSource(elementTimeout);
                elementFunc = ExpressionUtils.CompileWithFallback(elementFuncExpr, elementCts.Token);
            }
            else
            {
                elementFunc = static row => row;
            }

            // Reflection pieces needed inside the transform closure
            var groupingType = typeof(IGrouping<,>).MakeGenericType(keyType, elementType);
            var concreteType = typeof(ClientGrouping<,>).MakeGenericType(keyType, elementType);
            var groupingCtor = concreteType.GetConstructor(
                new[] { keyType, typeof(IEnumerable<>).MakeGenericType(elementType) })!;
            var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast))!.MakeGenericMethod(elementType);
            var resultListType = typeof(List<>).MakeGenericType(groupingType);

            // Sentinel used as dictionary key when the actual group key is null,
            // since Dictionary<object,-> does not permit null keys.
            var nullSentinel = new object();

            System.Collections.IList Transform(DbContext ctx, System.Collections.IList rows)
            {
                // Preserve first-seen key order (LINQ GroupBy semantics)
                var keyOrder = new List<object?>();
                var buckets = new Dictionary<object, List<object>>(EqualityComparer<object>.Default);
                foreach (var row in rows)
                {
                    var key = row == null ? null : keyFunc(row);
                    var dictKey = (object?)key ?? nullSentinel;
                    if (!buckets.TryGetValue(dictKey, out var bucket))
                    {
                        bucket = new List<object>();
                        buckets[dictKey] = bucket;
                        keyOrder.Add(key);   // actual key (may be null)
                    }
                    bucket.Add(elementFunc(row!)!);
                }

                var result = (System.Collections.IList)Activator.CreateInstance(resultListType, keyOrder.Count)!;
                foreach (var keyObj in keyOrder)
                {
                    var dictKey = (object?)keyObj ?? nullSentinel;
                    var items = castMethod.Invoke(null, new object?[] { buckets[dictKey] })!;
                    var grouping = groupingCtor.Invoke(new object?[] { keyObj, items })!;
                    result.Add(grouping);
                }
                return result;
            }

            _postMaterializeTransform = Transform;
            // Mark tail mode with the grouped element type so downstream client
            // evaluation (terminals and sequence operators over the IGroupings)
            // sees the correct element shape.
            _postMaterializeElementType = groupingType;
        }
    }
}
