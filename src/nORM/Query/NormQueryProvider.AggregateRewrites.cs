using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {        private bool TryRewriteAggregateToSum<TResult>(Expression expression, out TResult result)
        {
            result = default!;
            if (expression is not MethodCallExpression mc
                || mc.Method.DeclaringType != typeof(System.Linq.Queryable)
                || mc.Method.Name != nameof(System.Linq.Queryable.Aggregate))
                return false;

            // Three Aggregate overloads: 2 args (source, func), 3 args (source, seed, func),
            // 4 args (source, seed, func, resultSelector - unsupported).
            LambdaExpression? fold;
            object? seed;
            Type seedType;
            Expression source;
            if (mc.Arguments.Count == 2)
            {
                source = mc.Arguments[0];
                fold = StripQuotesLocal(mc.Arguments[1]) as LambdaExpression;
                seed = null;            // 1-arg form: throw on empty; equivalent to Sum() unless empty
                seedType = mc.Method.GetGenericArguments()[0];
            }
            else if (mc.Arguments.Count == 3)
            {
                source = mc.Arguments[0];
                if (!TryEvaluateConstant(mc.Arguments[1], out seed)) return false;
                fold = StripQuotesLocal(mc.Arguments[2]) as LambdaExpression;
                seedType = mc.Method.GetGenericArguments()[1];
            }
            else
            {
                return false;
            }

            if (fold == null || fold.Parameters.Count != 2)
                return false;

            var accParam = fold.Parameters[0];
            var elemParam = fold.Parameters[1];

            // Min/max fold: handled first so Math.Max/Min calls and Conditional
            // `x > acc ? x : acc` shapes lower to MAX/MIN before the Add check
            // rejects them.
            if (TryRewriteMinMaxAggregate<TResult>(mc, source, fold, accParam, elemParam, seed, mc.Arguments.Count == 2, out result))
                return true;

            if (fold.Body.NodeType != ExpressionType.Add)
                return false;

            // String-concat fold: handled separately so seed-aware-separator
            // (acc + (acc == "" ? "" : sep) + elem) and simple-separator
            // (acc + sep + elem) shapes both lower to GROUP_CONCAT via
            // GroupBy(_=>1) synthesis. Empty source returns the seed for the
            // 3-arg form and throws for the 2-arg form, matching .NET.
            if ((Nullable.GetUnderlyingType(seedType) ?? seedType) == typeof(string)
                && TryRewriteStringConcatAggregate<TResult>(mc, source, fold, accParam, elemParam, seed, mc.Arguments.Count == 2, out result))
            {
                return true;
            }

            var binary = (BinaryExpression)fold.Body;
            Expression sub;
            if (binary.Left == accParam) sub = binary.Right;
            else if (binary.Right == accParam) sub = binary.Left;
            else return false;
            if (ReferencesParameter(sub, accParam)) return false;

            // Build the equivalent Queryable.Sum call. nORM's DirectAggregate
            // translator only emits SUM SQL when given a selector lambda - the
            // no-selector overload Sum(IQueryable<T>) materialises the list
            // and aggregates client-side. Always synthesize the selector form
            // so the aggregation happens on the server.
            //
            // Identity sub over a Select-ed source (the common shape
            // `Select(r => proj).Aggregate(seed, (acc, s) => acc + s)`): peel
            // the Select so the synthesized Sum's selector emits `proj` on
            // the underlying entity rather than a meaningless `s => s` over
            // the projected scalar.
            var tSource = mc.Method.GetGenericArguments()[0];
            Expression sumSource = source;
            Expression sumBody = sub;
            ParameterExpression sumParam = elemParam;
            if (sub == elemParam
                && source is MethodCallExpression selectCall
                && selectCall.Method.DeclaringType == typeof(System.Linq.Queryable)
                && selectCall.Method.Name == nameof(System.Linq.Queryable.Select)
                && selectCall.Arguments.Count == 2
                && StripQuotesLocal(selectCall.Arguments[1]) is LambdaExpression selectLambda)
            {
                sumSource = selectCall.Arguments[0];
                sumParam = selectLambda.Parameters[0];
                sumBody = selectLambda.Body;
            }
            var subType = sumBody.Type;
            var actualTSource = sumSource.Type.IsGenericType
                ? sumSource.Type.GetGenericArguments()[0]
                : tSource;
            var sumLambda = Expression.Lambda(sumBody, sumParam);
            var sumMethod = FindSumOverload(actualTSource, subType);
            if (sumMethod == null) return false;
            var sumCall = Expression.Call(sumMethod, sumSource, Expression.Quote(sumLambda));
            object? sumResult;
            try
            {
                sumResult = Execute(sumCall);
            }
            catch (InvalidOperationException) when (mc.Arguments.Count == 2)
            {
                // Sum on empty returns 0; 1-arg Aggregate on empty must throw. Re-throw to match
                // the .NET semantics.
                throw;
            }

            // Combine sumResult + seed (if any). Use checked arithmetic only for integer types
            // since C# `+` defaults are unchecked at runtime.
            object finalValue = sumResult ?? Activator.CreateInstance(subType)!;
            if (seed != null)
            {
                finalValue = AddSeedToSum(seed, finalValue, seedType);
            }
            result = (TResult)Convert.ChangeType(finalValue, typeof(TResult), System.Globalization.CultureInfo.InvariantCulture)!;
            return true;
        }

        private static Expression StripQuotesLocal(Expression e)
        {
            while (e is UnaryExpression u && u.NodeType == ExpressionType.Quote) e = u.Operand;
            return e;
        }

        private static bool TryEvaluateConstant(Expression e, out object? value)
        {
            if (e is ConstantExpression ce) { value = ce.Value; return true; }
            try { value = Expression.Lambda(e).Compile().DynamicInvoke(); return true; }
            catch { value = null; return false; }
        }

        private static bool ReferencesParameter(Expression e, ParameterExpression p)
        {
            var found = false;
            new ParameterFinderVisitor(p, () => found = true).Visit(e);
            return found;
        }

        private sealed class ParameterFinderVisitor : ExpressionVisitor
        {
            private readonly ParameterExpression _target;
            private readonly Action _onFound;
            public ParameterFinderVisitor(ParameterExpression target, Action onFound) { _target = target; _onFound = onFound; }
            protected override Expression VisitParameter(ParameterExpression node)
            { if (node == _target) _onFound(); return node; }
        }

        // Holder used as the projected anon shape inside the synthesized
        // GroupBy().Select(g => new { V = string.Join(...) }) chain. Real
        // anonymous types would need a runtime emit; a named single-field
        // record is equivalent for the NewExpression-projection translator
        // path and saves the emit cost.
        public sealed class StringConcatAggResult
        {
            public string? V { get; }
            public StringConcatAggResult(string? v) => V = v;
        }

        // Detect string-concat fold shapes and rewrite to
        //   source.GroupBy(_ => 1).Select(g => string.Join(sep, g.Select(<projLambda>))).FirstOrDefault()
        // which the existing IGrouping aggregate translator (QueryTranslator.
        // Aggregates.cs:472) emits as GROUP_CONCAT/STRING_AGG.
        // Detect Aggregate min/max fold shapes:
        //   Conditional: (acc, x) => x [>/<] acc ? x : acc  (or acc[>/<]x ? acc : x)
        //   Method:      (acc, x) => Math.Max(acc, x) / Math.Min(acc, x)
        // Lowers to Queryable.Max / Queryable.Min on the source's element type
        // (peeling the outer Select if the fold body's "element-side" is the
        // elemParam itself, same trick as the sum-fold rewrite). For seed forms
        // we combine the SQL-side max/min with the seed using the same op.
        private bool TryRewriteMinMaxAggregate<TResult>(
            MethodCallExpression mc,
            Expression source,
            LambdaExpression fold,
            ParameterExpression accParam,
            ParameterExpression elemParam,
            object? seed,
            bool throwOnEmpty,
            out TResult result)
        {
            result = default!;
            // Try to detect "isMax" (true = MAX, false = MIN, null = no match).
            bool? isMax = null;
            // Math.Max(acc, x) / Math.Min(acc, x) - order-insensitive args.
            if (fold.Body is MethodCallExpression mathCall
                && mathCall.Method.DeclaringType == typeof(Math)
                && mathCall.Arguments.Count == 2
                && ((mathCall.Arguments[0] == accParam && mathCall.Arguments[1] == elemParam)
                    || (mathCall.Arguments[0] == elemParam && mathCall.Arguments[1] == accParam)))
            {
                if (mathCall.Method.Name == nameof(Math.Max)) isMax = true;
                else if (mathCall.Method.Name == nameof(Math.Min)) isMax = false;
            }
            // Conditional: x [>/<] acc ? x : acc - or acc [>/<] x ? acc : x.
            else if (fold.Body is ConditionalExpression cond
                     && cond.Test is BinaryExpression cmp
                     && (cmp.NodeType == ExpressionType.GreaterThan
                         || cmp.NodeType == ExpressionType.GreaterThanOrEqual
                         || cmp.NodeType == ExpressionType.LessThan
                         || cmp.NodeType == ExpressionType.LessThanOrEqual))
            {
                bool isGreater = cmp.NodeType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual;
                // Test == "x > acc"  : IfTrue=x, IfFalse=acc ? MAX
                // Test == "acc > x"  : IfTrue=acc, IfFalse=x ? MAX
                // (and mirrored for <)
                if (cmp.Left == elemParam && cmp.Right == accParam
                    && cond.IfTrue == elemParam && cond.IfFalse == accParam) isMax = isGreater;
                else if (cmp.Left == accParam && cmp.Right == elemParam
                         && cond.IfTrue == accParam && cond.IfFalse == elemParam) isMax = isGreater;
            }
            if (isMax == null) return false;

            // Peel `source = Select(r => proj)` so the synthesized Max/Min selector
            // projects an entity column rather than identity-over-scalar (same
            // trick as TryRewriteAggregateToSum's outer-Select unwrap).
            Expression aggSource = source;
            Expression projBody = elemParam;
            ParameterExpression projParam = elemParam;
            if (source is MethodCallExpression preSel
                && preSel.Method.DeclaringType == typeof(System.Linq.Queryable)
                && preSel.Method.Name == nameof(System.Linq.Queryable.Select)
                && preSel.Arguments.Count == 2
                && StripQuotesLocal(preSel.Arguments[1]) is LambdaExpression preSelLambda)
            {
                aggSource = preSel.Arguments[0];
                projParam = preSelLambda.Parameters[0];
                projBody = preSelLambda.Body;
            }
            var sourceGenArg = aggSource.Type.IsGenericType ? aggSource.Type.GetGenericArguments()[0] : projParam.Type;
            var subType = projBody.Type;
            var selectorLambda = Expression.Lambda(projBody, projParam);

            // Find Queryable.Max<TSource, TResult>(IQueryable<TSource>, Expression<Func<TSource,TResult>>).
            var aggMethod = FindAggregateOverload(isMax.Value ? nameof(System.Linq.Queryable.Max) : nameof(System.Linq.Queryable.Min), sourceGenArg, subType);
            if (aggMethod == null) return false;
            var aggCall = Expression.Call(aggMethod, aggSource, Expression.Quote(selectorLambda));

            object? sqlResult;
            try
            {
                sqlResult = Execute(aggCall);
            }
            catch (InvalidOperationException) when (throwOnEmpty)
            {
                // Max/Min on empty already throws "Sequence contains no elements".
                throw;
            }

            // Combine with seed if 3-arg overload. For empty-source: 1-arg form
            // already threw; 3-arg sees sqlResult==null and returns seed alone.
            if (seed != null && sqlResult != null)
            {
                sqlResult = isMax.Value
                    ? CombineMinMax(seed, sqlResult, subType, isMax: true)
                    : CombineMinMax(seed, sqlResult, subType, isMax: false);
            }
            else if (seed != null && sqlResult == null)
            {
                sqlResult = seed;
            }

            result = (TResult)System.Convert.ChangeType(sqlResult!, typeof(TResult), System.Globalization.CultureInfo.InvariantCulture)!;
            return true;
        }

        private static System.Reflection.MethodInfo? FindAggregateOverload(string methodName, Type tSource, Type returnType)
        {
            // Prefer the 2-generic overload Max<TSource,TResult>(...) since it
            // accepts any return type. Queryable also has 1-generic overloads
            // with fixed return types (int/long/double/decimal/etc.), but
            // those would match by return-type comparison only against the
            // already-instantiated Func signature.
            foreach (var m in typeof(System.Linq.Queryable).GetMethods())
            {
                if (m.Name != methodName) continue;
                if (!m.IsGenericMethodDefinition) continue;
                var ga = m.GetGenericArguments();
                if (ga.Length != 2) continue;
                var ps = m.GetParameters();
                if (ps.Length != 2) continue;
                return m.MakeGenericMethod(tSource, returnType);
            }
            return null;
        }

        private static object CombineMinMax(object seed, object sqlValue, Type accType, bool isMax)
        {
            accType = Nullable.GetUnderlyingType(accType) ?? accType;
            if (accType == typeof(int))
            {
                int a = System.Convert.ToInt32(seed), b = System.Convert.ToInt32(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            if (accType == typeof(long))
            {
                long a = System.Convert.ToInt64(seed), b = System.Convert.ToInt64(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            if (accType == typeof(double))
            {
                double a = System.Convert.ToDouble(seed), b = System.Convert.ToDouble(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            if (accType == typeof(decimal))
            {
                decimal a = System.Convert.ToDecimal(seed), b = System.Convert.ToDecimal(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            throw new NormUnsupportedFeatureException(
                $"Aggregate min/max fold accumulator type '{accType.Name}' is not supported by nORM's provider-mobile aggregate fold rewrite.");
        }

        private bool TryRewriteStringConcatAggregate<TResult>(
            MethodCallExpression mc,
            Expression source,
            LambdaExpression fold,
            ParameterExpression accParam,
            ParameterExpression elemParam,
            object? seed,
            bool throwOnEmpty,
            out TResult result)
        {
            result = default!;
            if (fold.Body is not BinaryExpression outerAdd || outerAdd.NodeType != ExpressionType.Add)
                return false;

            // Outer Add: (prefix) + elemExpr. elemExpr must not reference acc.
            var elemExpr = outerAdd.Right;
            if (ReferencesParameter(elemExpr, accParam)) throw new InvalidOperationException("DBG-SC3: elemExpr refs acc");

            // prefix is either `acc` (no separator) OR Add(acc, sepExpr).
            string sep;
            if (outerAdd.Left == accParam)
            {
                sep = "";
            }
            else if (outerAdd.Left is BinaryExpression innerAdd
                     && innerAdd.NodeType == ExpressionType.Add
                     && innerAdd.Left == accParam
                     && TryExtractSeparator(innerAdd.Right, out sep))
            {
                // ok
            }
            else
            {
                throw new InvalidOperationException($"DBG-SC4: outerAdd.Left shape unexpected: type={outerAdd.Left.GetType().Name} nodeType={outerAdd.Left.NodeType} body=[{outerAdd.Left}]");
            }

            // Synthesize: groupedSource.GroupBy(_ => 1).Select(g => string.Join(sep, g.Select(<proj>))).FirstOrDefault()
            // where <proj> projects each group element to its string value. For
            // `Query<T>().Select(r => r.Name).Aggregate(...)` peel the outer
            // Select so the inner-Select projects an entity column (which nORM
            // can translate) rather than an identity over a scalar parameter
            // (which can't reference any column).
            Expression groupedSource = source;
            Expression projBody = elemExpr;
            ParameterExpression projParam = elemParam;
            if (elemExpr == elemParam
                && source is MethodCallExpression preSel
                && preSel.Method.DeclaringType == typeof(System.Linq.Queryable)
                && preSel.Method.Name == nameof(System.Linq.Queryable.Select)
                && preSel.Arguments.Count == 2
                && StripQuotesLocal(preSel.Arguments[1]) is LambdaExpression preSelLambda)
            {
                groupedSource = preSel.Arguments[0];
                projParam = preSelLambda.Parameters[0];
                projBody = preSelLambda.Body;
            }
            // Ordering is preserved in groupedSource. HandleGroupBy sets _orderBy when
            // it processes the OrderBy chain. For the constant-key GroupBy (no GROUP BY
            // emitted), TranslateGroupAggregateMethod consumes _orderBy and routes it
            // through GetStringAggregateSql(expr, sep, orderBy) so each provider uses
            // its native ordered-aggregate syntax (WITHIN GROUP / inline ORDER BY / etc.).
            var sourceGenArg = groupedSource.Type.IsGenericType ? groupedSource.Type.GetGenericArguments()[0] : projParam.Type;
            var projLambda = Expression.Lambda(projBody, projParam);

            // GroupBy<TSource, TKey>(IQueryable<TSource>, Expression<Func<TSource, TKey>>)
            var groupKeyParam = Expression.Parameter(sourceGenArg, "_");
            var groupKeyLambda = Expression.Lambda(Expression.Constant(1), groupKeyParam);
            var groupByMethod = typeof(System.Linq.Queryable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Queryable.GroupBy) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2)
                .Where(m => m.GetParameters().Length == 2)
                .FirstOrDefault(m =>
                {
                    var p1 = m.GetParameters()[1].ParameterType;
                    if (!p1.IsGenericType) return false;
                    var fn = p1.GetGenericArguments()[0];
                    return fn.IsGenericType && fn.GetGenericArguments().Length == 2;
                });
            if (groupByMethod == null) throw new InvalidOperationException("DBG-SC6: groupByMethod is null");
            var groupByCall = Expression.Call(groupByMethod.MakeGenericMethod(sourceGenArg, typeof(int)), groupedSource, Expression.Quote(groupKeyLambda));

            // Inside the result selector: g.Select(<projLambda>) - Enumerable.Select
            // (NOT Queryable.Select; the second param is Func<,>, not Expression<Func<,>>).
            var iGroupingType = typeof(System.Linq.IGrouping<,>).MakeGenericType(typeof(int), sourceGenArg);
            var enumerableSelect = typeof(System.Linq.Enumerable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Enumerable.Select) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2 && m.GetParameters().Length == 2)
                .FirstOrDefault(m =>
                {
                    var p1 = m.GetParameters()[1].ParameterType;
                    // Want Func<TSource, TResult>, NOT Func<TSource, int, TResult>.
                    return p1.IsGenericType && p1.GetGenericTypeDefinition() == typeof(Func<,>);
                });
            if (enumerableSelect == null) throw new InvalidOperationException("DBG-SC7: enumerableSelect is null");
            var gParam = Expression.Parameter(iGroupingType, "g");
            // Enumerable.Select takes a Func, but Expression.Lambda<Func<...>>(...) decays
            // implicitly. Pass the LambdaExpression directly.
            var innerSelectCall = Expression.Call(
                enumerableSelect.MakeGenericMethod(sourceGenArg, typeof(string)),
                gParam,
                projLambda);

            // string.Join(string, IEnumerable<string>)
            var stringJoinMethod = typeof(string).GetMethod(nameof(string.Join), new[] { typeof(string), typeof(System.Collections.Generic.IEnumerable<string>) });
            if (stringJoinMethod == null) throw new InvalidOperationException("DBG-SC8: stringJoinMethod null");
            var joinCall = Expression.Call(stringJoinMethod, Expression.Constant(sep), innerSelectCall);

            // Wrap in Select(g => new { V = string.Join(...) }) - the existing
            // IGrouping-projection path emits BOTH groupKey AND value columns
            // for a scalar MethodCall body (Aggregates.cs:184). The single-
            // field NewExpression takes the NewExpression branch which only
            // emits the explicit args - exactly one column (the joined value)
            // so the scalar materialiser reads it correctly.
            var anonType = typeof(StringConcatAggResult);
            var anonCtor = anonType.GetConstructor(new[] { typeof(string) })!;
            var anonNew = Expression.New(anonCtor, new[] { joinCall }, new[] { anonType.GetMember("V")[0] });
            var resultSelector = Expression.Lambda(anonNew, gParam);
            var queryableSelect = typeof(System.Linq.Queryable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Queryable.Select) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2)
                .Where(m => m.GetParameters().Length == 2)
                .FirstOrDefault(m =>
                {
                    var p1 = m.GetParameters()[1].ParameterType;
                    if (!p1.IsGenericType) return false;
                    var fn = p1.GetGenericArguments()[0];
                    return fn.IsGenericType && fn.GetGenericArguments().Length == 2;
                });
            if (queryableSelect == null) throw new InvalidOperationException("DBG-SC9: queryableSelect null");
            var outerSelectCall = Expression.Call(
                queryableSelect.MakeGenericMethod(iGroupingType, anonType),
                groupByCall,
                Expression.Quote(resultSelector));

            var firstOrDefault = typeof(System.Linq.Queryable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Queryable.FirstOrDefault) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 1)
                .First(m => m.GetParameters().Length == 1)
                .MakeGenericMethod(anonType);
            var firstCall = Expression.Call(firstOrDefault, outerSelectCall);

            var row = Execute(firstCall) as StringConcatAggResult;
            var joined = row?.V;

            if (joined == null)
            {
                // Empty source. 1-arg form throws; 2-arg form returns seed.
                if (throwOnEmpty)
                    throw new InvalidOperationException("Sequence contains no elements");
                joined = seed as string ?? string.Empty;
            }

            result = (TResult)(object)joined;
            return true;
        }

        private static bool TryExtractSeparator(Expression sepExpr, out string sep)
        {
            sep = "";
            // Direct string constant: ", "
            if (TryEvaluateConstant(sepExpr, out var v) && v is string s1) { sep = s1; return true; }
            // Conditional: (acc == "" ? "" : sep) - extract the non-empty branch.
            if (sepExpr is ConditionalExpression cond)
            {
                if (TryEvaluateConstant(cond.IfTrue, out var t) && t is string tStr
                    && TryEvaluateConstant(cond.IfFalse, out var f) && f is string fStr)
                {
                    if (string.IsNullOrEmpty(tStr) && !string.IsNullOrEmpty(fStr)) { sep = fStr; return true; }
                    if (string.IsNullOrEmpty(fStr) && !string.IsNullOrEmpty(tStr)) { sep = tStr; return true; }
                }
            }
            return false;
        }

        private static System.Reflection.MethodInfo? FindSumNoSelectorOverload(Type elementType)
        {
            // Sum has a non-generic overload for each numeric type that takes
            // IQueryable<T> directly. Look them up by parameter shape.
            foreach (var m in typeof(System.Linq.Queryable).GetMethods())
            {
                if (m.Name != nameof(System.Linq.Queryable.Sum)) continue;
                if (m.IsGenericMethodDefinition) continue;
                var ps = m.GetParameters();
                if (ps.Length != 1) continue;
                var pt = ps[0].ParameterType;
                if (!pt.IsGenericType || pt.GetGenericTypeDefinition() != typeof(IQueryable<>)) continue;
                if (pt.GetGenericArguments()[0] == elementType) return m;
            }
            return null;
        }

        private static System.Reflection.MethodInfo? FindSumOverload(Type tSource, Type returnType)
        {
            foreach (var m in typeof(System.Linq.Queryable).GetMethods())
            {
                if (m.Name != nameof(System.Linq.Queryable.Sum)) continue;
                if (!m.IsGenericMethodDefinition || m.GetGenericArguments().Length != 1) continue;
                var ps = m.GetParameters();
                if (ps.Length != 2) continue;
                var selectorParam = ps[1].ParameterType;
                if (!selectorParam.IsGenericType) continue;
                var funcType = selectorParam.GetGenericArguments()[0];
                if (!funcType.IsGenericType) continue;
                var funcGen = funcType.GetGenericArguments();
                if (funcGen.Length != 2) continue;
                if (funcGen[1] != returnType) continue;
                return m.MakeGenericMethod(tSource);
            }
            return null;
        }

        private static object AddSeedToSum(object seed, object sumValue, Type accType)
        {
            // Normalize both to the accumulator type via direct casts on the
            // supported numeric types. Convert.ChangeType chokes on bool/null
            // and we don't need it: Aggregate's seed type drives accType.
            accType = Nullable.GetUnderlyingType(accType) ?? accType;
            if (accType == typeof(long))    return (long)System.Convert.ToInt64(seed)    + (long)System.Convert.ToInt64(sumValue);
            if (accType == typeof(int))     return (int)System.Convert.ToInt32(seed)     + (int)System.Convert.ToInt32(sumValue);
            if (accType == typeof(double))  return (double)System.Convert.ToDouble(seed) + (double)System.Convert.ToDouble(sumValue);
            if (accType == typeof(float))   return (float)System.Convert.ToSingle(seed)  + (float)System.Convert.ToSingle(sumValue);
            if (accType == typeof(decimal)) return (decimal)System.Convert.ToDecimal(seed) + (decimal)System.Convert.ToDecimal(sumValue);
            throw new NormUnsupportedFeatureException(
                $"Aggregate sum-fold accumulator type '{accType.Name}' is not supported by nORM's provider-mobile aggregate fold rewrite.");
        }
    }
}
