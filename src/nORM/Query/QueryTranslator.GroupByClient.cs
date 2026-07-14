using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using nORM.Core;
using nORM.Internal;

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
        /// Compiles <paramref name="keySelectorLambda"/> into a client-side grouping transform
        /// that groups a materialized entity list by key, returning a typed
        /// <c>List&lt;IGrouping&lt;K, V&gt;&gt;</c>.  Stored as
        /// <see cref="_postMaterializeTransform"/> so the normal entity materializer handles
        /// row reading; the transform runs once after all rows are in memory.
        /// </summary>
        private void InstallGroupingTransform(LambdaExpression keySelectorLambda, LambdaExpression? elementSelectorLambda = null)
        {
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
