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
            if (_postMaterializeTransform == null)
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

            private static bool HasParameterReference(Expression expression)
            {
                var finder = new ParameterFinder();
                finder.Visit(expression);
                return finder.Found;
            }

            private sealed class ParameterFinder : ExpressionVisitor
            {
                public bool Found { get; private set; }
                protected override Expression VisitParameter(ParameterExpression node)
                {
                    Found = true;
                    return node;
                }
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
                if (node.Arguments[1].Type is { } secondType
                    && typeof(IQueryable).IsAssignableFrom(secondType)
                    && !TryGetConstantValue(node.Arguments[1], out _))
                {
                    throw new NormUnsupportedFeatureException(
                        "Zip over two database queries has no provider-mobile SQL translation (positional pairing " +
                        "requires row numbering on both sides). Zip a query with a local collection, or materialize " +
                        "both queries first and use LINQ-to-Objects Zip.");
                }

                if (!TryGetConstantValue(node.Arguments[1], out var secondObj)
                    || secondObj is not System.Collections.IEnumerable secondSequence
                    || secondObj is IQueryable)
                {
                    throw new NormUnsupportedFeatureException(
                        "Zip requires a constant or captured local second sequence. Materialize the query first " +
                        "(e.g. ToListAsync) and use LINQ-to-Objects Zip for other shapes.");
                }

                LambdaExpression? resultSelector = null;
                if (node.Arguments.Count == 3)
                {
                    resultSelector = StripQuotes(node.Arguments[2]) as LambdaExpression
                        ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                            "Zip requires a lambda result selector"));
                }

                var source = t.Visit(node.Arguments[0]);
                var firstType = t.CurrentPostMaterializeElementType ?? t._projection?.Body.Type ?? t._mapping.Type;
                var genericArgs = node.Method.GetGenericArguments();
                var secondElementType = genericArgs.Length >= 2 ? genericArgs[1] : typeof(object);
                var second = secondSequence.Cast<object?>().ToArray();

                Type resultType;
                Func<object?, object?, object?> projector;
                if (resultSelector != null)
                {
                    resultType = resultSelector.Body.Type;
                    var firstArg = Expression.Parameter(typeof(object), "first");
                    var secondArg = Expression.Parameter(typeof(object), "second");
                    var body = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[0], Expression.Convert(firstArg, firstType)).Visit(resultSelector.Body)!;
                    body = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[1], Expression.Convert(secondArg, secondElementType)).Visit(body)!;
                    projector = Expression.Lambda<Func<object?, object?, object?>>(
                        Expression.Convert(body, typeof(object)), firstArg, secondArg).Compile();
                }
                else
                {
                    resultType = typeof(ValueTuple<,>).MakeGenericType(firstType, secondElementType);
                    var tupleCtor = resultType.GetConstructor(new[] { firstType, secondElementType })!;
                    projector = (first, secondItem) => tupleCtor.Invoke(new[] { first, secondItem });
                }

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
        }
    }
}
