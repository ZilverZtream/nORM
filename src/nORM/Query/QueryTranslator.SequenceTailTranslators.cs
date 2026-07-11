using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// Scalar SQL (EXISTS/aggregate) computes over server rows and cannot see a
        /// pending client-tail reshape, so translators that emit scalar plans fail closed
        /// when one is pending instead of returning a value for the wrong sequence.
        /// Count and LongCount instead reduce the reshaped rows client-side via
        /// <see cref="TryAppendClientCountAggregate"/>.
        /// </summary>
        private static void ThrowIfClientTailReshapePending(QueryTranslator t, string methodName)
        {
            if (t._postMaterializeTransform != null)
            {
                throw new NormUnsupportedFeatureException(
                    $"{methodName} after a client-materialized sequence operator (Append, Prepend, Chunk, Zip, " +
                    "DefaultIfEmpty with a default value) would evaluate against server rows, not the reshaped " +
                    "sequence. Materialize the query first (e.g. ToListAsync) and evaluate in memory.");
            }
        }

        /// <summary>
        /// When a client-tail reshape is pending, Count/LongCount reduce the reshaped
        /// rows in memory instead of emitting a server COUNT that would ignore the
        /// reshape. An optional predicate is compiled against the reshaped element type
        /// and applied per row, matching LINQ-to-Objects semantics.
        /// </summary>
        private bool TryAppendClientCountAggregate(MethodCallExpression node)
        {
            // Applies in any post-materialize tail mode: a pending reshape transform, or
            // a group-join result shape whose rows are assembled after materialization.
            if (!IsPostMaterializeTailMode)
                return false;

            Func<object?, bool>? predicate = null;
            if (node.Arguments.Count > 1)
            {
                if (StripQuotes(node.Arguments[1]) is not LambdaExpression predicateLambda
                    || predicateLambda.Parameters.Count != 1)
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} over a client-materialized sequence supports only a one-argument predicate.");
                }
                var elementType = CurrentPostMaterializeElementType ?? predicateLambda.Parameters[0].Type;
                var rowArg = Expression.Parameter(typeof(object), "row");
                var typedRow = elementType.IsValueType
                    ? (Expression)Expression.Convert(rowArg, elementType)
                    : Expression.TypeAs(rowArg, elementType);
                var body = new nORM.Internal.ParameterReplacer(predicateLambda.Parameters[0], typedRow).Visit(predicateLambda.Body)!;
                predicate = Expression.Lambda<Func<object?, bool>>(body, rowArg).Compile();
            }

            // The plan must stay list-shaped: CountTranslator marks the query as an
            // aggregate before visiting the source, which would make Generate() emit
            // scalar COUNT(*) SQL and skip materialization entirely.
            _isAggregate = false;
            _methodName = string.Empty;

            var isLong = node.Method.Name == nameof(Queryable.LongCount);
            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var count = 0;
                if (predicate == null)
                {
                    count = rows.Count;
                }
                else
                {
                    foreach (var row in rows)
                    {
                        if (predicate(row))
                            count++;
                    }
                }
                // Box each arm explicitly: a mixed long/int conditional promotes both
                // arms to long, which would hand Count callers a boxed Int64.
                return new List<object> { isLong ? (object)(long)count : count };
            }, isLong ? typeof(long) : typeof(int));
            _clientScalarResult = true;
            return true;
        }

        /// <summary>
        /// Filters the reshaped rows in memory with the given predicate, preserving the
        /// element type so further tail operators and aggregates compose. Used by Where
        /// when a client-tail reshape is pending — a SQL WHERE would only filter server
        /// rows and let reshaped elements bypass the predicate.
        /// </summary>
        private void AppendClientTailFilter(LambdaExpression predicateLambda)
        {
            var elementType = CurrentPostMaterializeElementType ?? predicateLambda.Parameters[0].Type;
            var rowArg = Expression.Parameter(typeof(object), "row");
            var typedRow = elementType.IsValueType
                ? (Expression)Expression.Convert(rowArg, elementType)
                : Expression.TypeAs(rowArg, elementType);
            var body = new nORM.Internal.ParameterReplacer(predicateLambda.Parameters[0], typedRow).Visit(predicateLambda.Body)!;
            var predicate = Expression.Lambda<Func<object?, bool>>(body, rowArg).Compile();

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var output = CreateRuntimeList(elementType, rows.Count);
                foreach (var row in rows)
                {
                    if (predicate(row))
                        output.Add(row);
                }
                return output;
            }, elementType);
        }

        /// <summary>
        /// When a client-tail reshape is pending, scalar aggregates and quantifiers
        /// (Sum, Min, Max, Average, Any, All, Contains) evaluate in memory over the
        /// reshaped rows via the matching <see cref="Enumerable"/> operator, which
        /// gives exact LINQ-to-Objects semantics including empty-sequence behavior.
        /// Selector and predicate lambdas are compiled as-is: after a reshape they are
        /// already typed against the reshaped element type.
        /// </summary>
        private bool TryAppendClientScalarAggregate(MethodCallExpression node)
        {
            // Applies in any post-materialize tail mode: a pending reshape transform, or
            // a group-join result shape whose rows are assembled after materialization.
            if (!IsPostMaterializeTailMode)
                return false;

            var enumerableMethod = FindEnumerableEquivalent(node.Method);
            if (enumerableMethod == null)
                return false;

            var sourceType = node.Arguments[0].Type;
            var elementType = sourceType.IsGenericType
                ? sourceType.GetGenericArguments()[0]
                : CurrentPostMaterializeElementType ?? typeof(object);

            var rowsParam = Expression.Parameter(typeof(System.Collections.IList), "rows");
            var args = new Expression[node.Arguments.Count];
            args[0] = Expression.Call(typeof(Enumerable), nameof(Enumerable.Cast), new[] { elementType }, rowsParam);
            for (var i = 1; i < node.Arguments.Count; i++)
                args[i] = StripQuotes(node.Arguments[i]);

            Expression call;
            try
            {
                call = Expression.Call(enumerableMethod, args);
            }
            catch (ArgumentException)
            {
                // Overload shape the resolver matched structurally but the expression
                // factory rejects (e.g. an untypeable argument) — fail closed upstream.
                return false;
            }

            var evaluator = Expression.Lambda<Func<System.Collections.IList, object?>>(
                Expression.Convert(call, typeof(object)), rowsParam).Compile();

            // The plan must stay list-shaped: aggregate translators mark the query as a
            // scalar aggregate, which would make Generate() emit aggregate SQL and skip
            // materialization of the rows the client evaluation needs.
            _isAggregate = false;
            _methodName = string.Empty;

            AppendPostMaterializeTransform((ctx, rows) => new List<object?> { evaluator(rows) }, node.Type);
            _clientScalarResult = true;
            return true;
        }

        /// <summary>
        /// When a client-tail reshape is pending, sequence operators (Take, Skip,
        /// TakeLast, SkipLast, TakeWhile, SkipWhile, Distinct, Reverse, ElementAt-style
        /// paging inputs) run in memory over the reshaped rows via the matching
        /// <see cref="Enumerable"/> operator. Argument expressions compile into the
        /// transform, so captured locals are read at execution time.
        /// </summary>
        private bool TryAppendClientSequenceOperator(MethodCallExpression node)
        {
            // Applies in any post-materialize tail mode: a pending reshape transform, or
            // a group-join result shape whose rows are assembled after materialization.
            if (!IsPostMaterializeTailMode)
                return false;

            var enumerableMethod = FindEnumerableEquivalent(node.Method);
            if (enumerableMethod == null)
                return false;

            var resultElementType = GetSequenceElementType(node.Type);
            var sourceType = node.Arguments[0].Type;
            var elementType = sourceType.IsGenericType
                ? sourceType.GetGenericArguments()[0]
                : CurrentPostMaterializeElementType ?? typeof(object);

            var rowsParam = Expression.Parameter(typeof(System.Collections.IList), "rows");
            var args = new Expression[node.Arguments.Count];
            args[0] = Expression.Call(typeof(Enumerable), nameof(Enumerable.Cast), new[] { elementType }, rowsParam);
            for (var i = 1; i < node.Arguments.Count; i++)
                args[i] = StripQuotes(node.Arguments[i]);

            Expression call;
            try
            {
                call = Expression.Call(enumerableMethod, args);
            }
            catch (ArgumentException)
            {
                return false;
            }

            var evaluator = Expression.Lambda<Func<System.Collections.IList, System.Collections.IEnumerable>>(
                Expression.Convert(call, typeof(System.Collections.IEnumerable)), rowsParam).Compile();

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var output = CreateRuntimeList(resultElementType, rows.Count);
                foreach (var item in evaluator(rows))
                    output.Add(item);
                return output;
            }, resultElementType);
            return true;
        }

        /// <summary>
        /// Resolves the <see cref="Enumerable"/> overload mirroring a <see cref="Queryable"/>
        /// aggregate: same name and generic arguments, with IQueryable sources becoming
        /// IEnumerable and quoted lambdas becoming delegates.
        /// </summary>
        private static System.Reflection.MethodInfo? FindEnumerableEquivalent(System.Reflection.MethodInfo queryableMethod)
        {
            if (queryableMethod.DeclaringType != typeof(Queryable))
                return null;
            var genericArgs = queryableMethod.IsGenericMethod ? queryableMethod.GetGenericArguments() : Type.EmptyTypes;
            var qParams = queryableMethod.GetParameters();
            foreach (var candidate in typeof(Enumerable).GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            {
                if (candidate.Name != queryableMethod.Name)
                    continue;
                var candidateArity = candidate.IsGenericMethod ? candidate.GetGenericArguments().Length : 0;
                if (candidateArity != genericArgs.Length)
                    continue;
                System.Reflection.MethodInfo closed;
                try
                {
                    closed = candidate.IsGenericMethod ? candidate.MakeGenericMethod(genericArgs) : candidate;
                }
                catch (ArgumentException)
                {
                    continue;
                }
                var eParams = closed.GetParameters();
                if (eParams.Length != qParams.Length)
                    continue;
                // Normalize the return type too: sequence operators return IQueryable<T>
                // on the Queryable side and IEnumerable<T> on the Enumerable side.
                var match = NormalizeQueryableParameter(queryableMethod.ReturnType) == closed.ReturnType;
                for (var i = 0; match && i < qParams.Length; i++)
                    match = NormalizeQueryableParameter(qParams[i].ParameterType) == eParams[i].ParameterType;
                if (match)
                    return closed;
            }
            return null;
        }

        private static Type NormalizeQueryableParameter(Type type)
        {
            if (!type.IsGenericType)
                return type;
            var definition = type.GetGenericTypeDefinition();
            if (definition == typeof(Expression<>))
                return type.GetGenericArguments()[0];
            if (definition == typeof(IQueryable<>))
                return typeof(IEnumerable<>).MakeGenericType(type.GetGenericArguments());
            return type;
        }

        /// <summary>
        /// Early divert for scalar terminals (First/Single/Last/ElementAt/MinBy/MaxBy,
        /// Any/All/Contains, aggregates) whose source spine contains a client-tail
        /// reshape: translate the source as the row plan and evaluate the terminal in
        /// memory. Returns null when the spine has no reshape (normal SQL path applies).
        /// The check must run BEFORE the terminal mutates translation state (LIMIT,
        /// ORDER BY flips, EXISTS SQL) — those server-side shapes evaluate against
        /// pre-reshape rows. Once the source is visited, falling through to SQL
        /// generation is never valid, so resolver failure fails closed.
        /// </summary>
        private static Expression? TryTranslateReshapedScalarTerminal(QueryTranslator t, MethodCallExpression node)
        {
            if (!SourceHasClientTailReshape(node.Arguments[0]) && !SourceHasGroupJoinResultTail(node.Arguments[0]))
                return null;
            var source = t.Visit(node.Arguments[0]);
            if (t.TryAppendClientScalarAggregate(node))
                return source;
            ThrowIfClientTailReshapePending(t, node.Method.Name);
            throw new NormUnsupportedFeatureException(
                $"{node.Method.Name} after a client-materialized sequence operator has no in-memory equivalent overload.");
        }

        /// <summary>
        /// Detects a source spine ending in a group-join RESULT shape (GroupJoin whose
        /// groups survive into the output, not the GroupJoin+SelectMany flattening that
        /// translates fully to SQL joins). Group-join results are assembled after
        /// materialization, so server-side LIMIT/OFFSET/DISTINCT on the flat joined
        /// rows would truncate or dedup a parent's children mid-group — terminals and
        /// sequence operators over these sources must divert to client evaluation
        /// BEFORE emitting any server-side shape.
        /// </summary>
        private static bool SourceHasGroupJoinResultTail(Expression source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case nameof(Queryable.SelectMany):
                        return false; // flattened group join — translates to SQL joins
                    case nameof(Queryable.GroupJoin):
                        return true;
                }
                if (mce.Arguments.Count == 0)
                    break;
                current = mce.Arguments[0];
            }
            return false;
        }

        /// <summary>
        /// Syntactic scan of a source's operator spine for the client-tail reshape
        /// operators. Set-predicate translators (Any/All/Contains) build EXISTS SQL via
        /// sub-context translation, so they must divert to client evaluation before any
        /// SQL is emitted — the pending-transform field is only populated during the walk.
        /// </summary>
        private static bool SourceHasClientTailReshape(Expression source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case nameof(Queryable.Append):
                    case nameof(Queryable.Prepend):
                    case nameof(Queryable.Chunk):
                    case nameof(Queryable.Zip):
                        return true;
                    case nameof(Queryable.DefaultIfEmpty) when mce.Arguments.Count == 2:
                        return true;
                }
                if (mce.Arguments.Count == 0)
                    break;
                current = mce.Arguments[0];
            }
            return false;
        }

        /// <summary>
        /// Implements <c>Chunk(size)</c> as a post-materialization transform: the source
        /// query executes server-side unchanged and the materialized rows are grouped
        /// into arrays of <c>size</c> elements in result order, matching
        /// <see cref="Enumerable.Chunk{TSource}(IEnumerable{TSource}, int)"/> semantics.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ChunkTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (!TryGetIntValue(node.Arguments[1], out var size))
                {
                    if (TryGetConstantValue(node.Arguments[1], out var sizeObj) && sizeObj is int captured)
                        size = captured;
                    else
                        throw new NormUnsupportedFeatureException(
                            "Chunk requires a constant or captured int size; a column-derived size has no SQL translation.");
                }
                if (size < 1)
                    throw new ArgumentOutOfRangeException(nameof(size), size,
                        "Chunk size must be at least 1, matching Enumerable.Chunk semantics.");

                var source = t.Visit(node.Arguments[0]);
                var elementType = t.CurrentPostMaterializeElementType ?? t._projection?.Body.Type ?? t._mapping.Type;
                var arrayType = elementType.MakeArrayType();

                t.AppendPostMaterializeTransform((ctx, rows) =>
                {
                    var output = CreateRuntimeList(arrayType, (rows.Count + size - 1) / size);
                    for (var i = 0; i < rows.Count; i += size)
                    {
                        var length = Math.Min(size, rows.Count - i);
                        var chunk = Array.CreateInstance(elementType, length);
                        for (var j = 0; j < length; j++)
                            chunk.SetValue(rows[i + j], j);
                        output.Add(chunk);
                    }
                    return output;
                }, arrayType);
                return source;
            }
        }

        /// <summary>
        /// Implements <c>Append(element)</c> and <c>Prepend(element)</c> as
        /// post-materialization transforms. The element must be a constant or captured
        /// value — it is not part of the SQL — and is attached to the materialized rows
        /// in result order, matching LINQ-to-Objects semantics.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AppendPrependTranslator(bool append) : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (!TryGetConstantValue(node.Arguments[1], out var element))
                {
                    // Fall back to compiling the element expression when it is a pure
                    // computation over captured state (e.g. `new Dto { … }`).
                    if (HasParameterReference(node.Arguments[1]))
                        throw new NormUnsupportedFeatureException(
                            $"{node.Method.Name} requires a constant or captured element; a row-derived element has no SQL translation. " +
                            $"Materialize the query first (e.g. ToListAsync) and use LINQ-to-Objects {node.Method.Name}.");
                    element = Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke();
                }

                var source = t.Visit(node.Arguments[0]);
                var elementType = t.CurrentPostMaterializeElementType ?? t._projection?.Body.Type ?? t._mapping.Type;
                var captured = element;
                var isAppend = append;

                t.AppendPostMaterializeTransform((ctx, rows) =>
                {
                    var output = CreateRuntimeList(elementType, rows.Count + 1);
                    if (!isAppend)
                        output.Add(captured);
                    foreach (var row in rows)
                        output.Add(row);
                    if (isAppend)
                        output.Add(captured);
                    return output;
                }, elementType);
                return source;
            }
        }

        private static bool HasParameterReference(Expression expression)
        {
            var finder = new ParameterFinder();
            finder.Visit(expression);
            return finder.Found;
        }

        private static bool HasFreeParameterReference(Expression expression)
        {
            var finder = new FreeParameterFinder();
            finder.Visit(expression);
            return finder.Found;
        }

        /// <summary>
        /// Detects parameter references not bound by a lambda inside the expression —
        /// i.e. references to an OUTER query's row. Lambda-bound parameters (an inner
        /// OrderBy's key selector) are legitimate parts of a self-contained queryable chain.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class FreeParameterFinder : ExpressionVisitor
        {
            private readonly HashSet<ParameterExpression> _bound = new();
            public bool Found { get; private set; }

            protected override Expression VisitLambda<TDelegate>(Expression<TDelegate> node)
            {
                var added = new List<ParameterExpression>();
                foreach (var p in node.Parameters)
                {
                    if (_bound.Add(p))
                        added.Add(p);
                }
                var result = base.VisitLambda(node);
                foreach (var p in added)
                    _bound.Remove(p);
                return result;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (!_bound.Contains(node))
                    Found = true;
                return node;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ParameterFinder : ExpressionVisitor
        {
            public bool Found { get; private set; }
            protected override Expression VisitParameter(ParameterExpression node)
            {
                Found = true;
                return node;
            }
        }

        /// <summary>
        /// Implements <c>Zip(second)</c> and <c>Zip(second, resultSelector)</c> over a
        /// local (constant or captured) second sequence as a post-materialization
        /// transform. Pairing follows LINQ semantics: the result length is the shorter
        /// of the two sequences. Zipping two database queries is rejected because a
        /// positional pairing of two result sets has no provider-mobile SQL translation.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ZipTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                LambdaExpression? resultSelector = null;
                if (node.Arguments.Count == 3)
                {
                    resultSelector = StripQuotes(node.Arguments[2]) as LambdaExpression
                        ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                            "Zip requires a lambda result selector"));
                }

                if (typeof(IQueryable).IsAssignableFrom(node.Arguments[1].Type)
                    || (TryGetConstantValue(node.Arguments[1], out var probe) && probe is IQueryable))
                {
                    return TranslateQueryZip(t, node, resultSelector);
                }

                if (!TryGetConstantValue(node.Arguments[1], out var secondObj)
                    || secondObj is not System.Collections.IEnumerable secondSequence)
                {
                    throw new NormUnsupportedFeatureException(
                        "Zip requires a constant or captured local second sequence, or a second database query. " +
                        "Materialize the query first (e.g. ToListAsync) and use LINQ-to-Objects Zip for other shapes.");
                }

                var source = t.Visit(node.Arguments[0]);
                var firstType = t.CurrentPostMaterializeElementType ?? t._projection?.Body.Type ?? t._mapping.Type;
                var genericArgs = node.Method.GetGenericArguments();
                var secondElementType = genericArgs.Length >= 2 ? genericArgs[1] : typeof(object);
                var second = secondSequence.Cast<object?>().ToArray();
                var (resultType, projector) = BuildZipProjector(resultSelector, firstType, secondElementType);

                t.AppendPostMaterializeTransform((ctx, rows) =>
                {
                    var count = Math.Min(rows.Count, second.Length);
                    var output = CreateRuntimeList(resultType, count);
                    for (var i = 0; i < count; i++)
                        output.Add(projector(rows[i], second[i]));
                    return output;
                }, resultType);
                return source;
            }

            /// <summary>
            /// Zip over two database queries: the first source translates and executes
            /// normally; the second ordered query executes through its own provider when
            /// the transform runs, and the two materialized sequences pair positionally
            /// with LINQ-to-Objects semantics (truncating to the shorter side). Both
            /// sides must carry an explicit OrderBy/ThenBy chain — without one, database
            /// row order is undefined and positional pairing would be nondeterministic.
            /// Costs one round trip per side.
            /// </summary>
            private static Expression TranslateQueryZip(QueryTranslator t, MethodCallExpression node, LambdaExpression? resultSelector)
            {
                if (ExtractOrderByKeys(node.Arguments[0]).Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        "Zip over two database queries requires the first source to have an explicit OrderBy/ThenBy chain so positional pairing is deterministic.");
                }

                // Queryable.Zip inlines the second queryable's expression tree, so the
                // argument is usually the full operator chain (with its own bound lambda
                // parameters), not a constant. Rebuild the queryable instance by
                // evaluating the chain — only a FREE parameter (a row-derived query
                // inside a correlated lambda) has no translation.
                IQueryable secondQuery;
                if (TryGetConstantValue(node.Arguments[1], out var secondConst) && secondConst is IQueryable direct)
                {
                    secondQuery = direct;
                }
                else if (!HasFreeParameterReference(node.Arguments[1]))
                {
                    secondQuery = (IQueryable)Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke()!;
                }
                else
                {
                    throw new NormUnsupportedFeatureException(
                        "Zip requires the second database query to be built from captured state; a row-derived query has no translation.");
                }

                if (ExtractOrderByKeys(secondQuery.Expression).Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        "Zip over two database queries requires the second source to have an explicit OrderBy/ThenBy chain so positional pairing is deterministic.");
                }

                var source = t.Visit(node.Arguments[0]);
                var firstType = t.CurrentPostMaterializeElementType ?? t._projection?.Body.Type ?? t._mapping.Type;
                var genericArgs = node.Method.GetGenericArguments();
                var secondElementType = genericArgs.Length >= 2 ? genericArgs[1] : secondQuery.ElementType;
                var (resultType, projector) = BuildZipProjector(resultSelector, firstType, secondElementType);
                var capturedSecond = secondQuery;

                t.AppendPostMaterializeTransform((ctx, rows) =>
                {
                    var second = new List<object?>();
                    foreach (var item in (System.Collections.IEnumerable)capturedSecond)
                        second.Add(item);
                    var count = Math.Min(rows.Count, second.Count);
                    var output = CreateRuntimeList(resultType, count);
                    for (var i = 0; i < count; i++)
                        output.Add(projector(rows[i], second[i]));
                    return output;
                }, resultType);
                return source;
            }

            private static (Type ResultType, Func<object?, object?, object?> Projector) BuildZipProjector(
                LambdaExpression? resultSelector, Type firstType, Type secondElementType)
            {
                if (resultSelector != null)
                {
                    var resultType = resultSelector.Body.Type;
                    var firstArg = Expression.Parameter(typeof(object), "first");
                    var secondArg = Expression.Parameter(typeof(object), "second");
                    var body = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[0], Expression.Convert(firstArg, firstType)).Visit(resultSelector.Body)!;
                    body = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[1], Expression.Convert(secondArg, secondElementType)).Visit(body)!;
                    var projector = Expression.Lambda<Func<object?, object?, object?>>(
                        Expression.Convert(body, typeof(object)), firstArg, secondArg).Compile();
                    return (resultType, projector);
                }

                var tupleType = typeof(ValueTuple<,>).MakeGenericType(firstType, secondElementType);
                var tupleCtor = tupleType.GetConstructor(new[] { firstType, secondElementType })!;
                return (tupleType, (first, secondItem) => tupleCtor.Invoke(new[] { first, secondItem }));
            }
        }
    }
}
