using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class DistinctByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var keyLambda = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(
                        ErrorMessages.QueryTranslationFailed,
                        "DistinctBy requires a key selector lambda."));

                var subPlan = t.TranslateInSubContext(
                    node.Arguments[0],
                    t._mapping,
                    t._parameterManager.Index,
                    t._joinCounter,
                    t._recursionDepth + 1,
                    out var subMapping);
                t._mapping = subMapping;
                t.MergeSubPlanParameters(subPlan);

                var sourceAlias = t.EscapeAlias("__dbsrc" + t._joinCounter++);
                var outerAlias = t.EscapeAlias("__distinctby" + t._joinCounter++);
                var rowAlias = t._provider.Escape("__norm_distinctby_rn");
                var keySql = BuildDistinctByKeySql(t, keyLambda, subMapping, sourceAlias);
                var orderSql = BuildDistinctByOrderSql(t, node.Arguments[0], subMapping, sourceAlias);
                var outerOrder = BuildDistinctByOrderSql(t, node.Arguments[0], subMapping, outerAlias);
                var sourceSql = RemoveTrailingOrderBy(node.Arguments[0], subPlan.Sql);
                var outerSelect = PooledStringBuilder.Join(subMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}"));

                t._sql.Append("SELECT ").Append(outerSelect).Append(" FROM (SELECT ")
                    .Append(sourceAlias).Append(".*, ROW_NUMBER() OVER (PARTITION BY ")
                    .Append(keySql).Append(" ORDER BY ").Append(PooledStringBuilder.JoinOrderBy(orderSql))
                    .Append(") AS ").Append(rowAlias)
                    .Append(" FROM (").Append(sourceSql).Append(") AS ").Append(sourceAlias)
                    .Append(") AS ").Append(outerAlias)
                    .Append(" WHERE ").Append(outerAlias).Append('.').Append(rowAlias).Append(" = 1");

                foreach (var order in outerOrder)
                    t._orderBy.Add(order);

                return node;
            }

            private static string BuildDistinctByKeySql(
                QueryTranslator t,
                LambdaExpression keyLambda,
                TableMapping mapping,
                string alias)
            {
                var parts = new List<string>();
                if (keyLambda.Body is NewExpression newKey && newKey.Arguments.Count > 0)
                {
                    foreach (var arg in newKey.Arguments)
                        parts.Add(OrdinalPartitionKey(t, BuildSql(t, keyLambda.Parameters[0], arg, mapping, alias), arg.Type));
                }
                else
                {
                    parts.Add(OrdinalPartitionKey(t, BuildSql(t, keyLambda.Parameters[0], keyLambda.Body, mapping, alias), keyLambda.Body.Type));
                }
                return string.Join(", ", parts);
            }

            /// <summary>
            /// C# keyed operators compare string keys ordinally, but PARTITION BY on a
            /// CI-collation provider partitions by the column collation — fusing
            /// "abc"/"ABC" into one group. Wrap string keys in the value-preserving
            /// binary collation. DateTimeOffset keys compare by INSTANT in C#, while
            /// offset-suffixed TEXT storage partitions by representation — same
            /// instant under different offsets would wrongly survive as distinct;
            /// canonicalize via the provider's group-key hook. Identity elsewhere.
            /// </summary>
            internal static string OrdinalPartitionKey(QueryTranslator t, string sql, Type keyType)
            {
                var underlying = Nullable.GetUnderlyingType(keyType) ?? keyType;
                if (underlying == typeof(string) && t._provider.DefaultStringEqualityIsCaseInsensitive)
                    return t._provider.ForceCaseSensitiveStringComparison(sql);
                if (underlying == typeof(DateTimeOffset)
                    && t._provider.CanonicalizeDateTimeOffsetGroupKey(sql) is { } canonical)
                    return canonical;
                return sql;
            }

            private static List<(string col, bool asc)> BuildDistinctByOrderSql(
                QueryTranslator t,
                Expression source,
                TableMapping mapping,
                string alias)
            {
                var orderKeys = ExtractOrderByKeys(source);
                var result = new List<(string col, bool asc)>();
                if (orderKeys.Count == 0)
                {
                    foreach (var key in mapping.KeyColumns)
                        result.Add(($"{alias}.{key.EscCol}", true));
                    return result;
                }

                foreach (var (keyLambda, ascending) in orderKeys)
                {
                    result.Add((BuildSql(t, keyLambda.Parameters[0], keyLambda.Body, mapping, alias), ascending));
                }
                return result;
            }

            private static string BuildSql(
                QueryTranslator t,
                ParameterExpression parameter,
                Expression expression,
                TableMapping mapping,
                string alias)
            {
                t._correlatedParams[parameter] = (mapping, alias);
                var vctx = new VisitorContext(t._ctx, mapping, t._provider, parameter, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var sql = visitor.Translate(expression);
                foreach (var kvp in visitor.GetParameters())
                    t._params[kvp.Key] = kvp.Value;
                if (t._params.Count > t._parameterManager.Index)
                    t._parameterManager.Index = t._params.Count;
                FastExpressionVisitorPool.Return(visitor);
                var type = Nullable.GetUnderlyingType(expression.Type) ?? expression.Type;
                return t._provider.ExactKeySql(sql, type);
            }

            private static string RemoveTrailingOrderBy(Expression source, string sql)
            {
                if (SourceHasTakeOrSkip(source))
                    return sql;
                return RemoveTrailingOrderByUnlessPaged(sql);
            }
        }

        /// <summary>
        /// ExceptBy/IntersectBy translate as provider-side key filters plus
        /// server-side DistinctBy. UnionBy with a local second entity sequence lowers
        /// that sequence to a parameterized derived table, then applies the same
        /// row-number key dedupe with source rows ordered before appended rows.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ExceptByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => RewriteKeyedFilterSetOp(t, node, keepMatches: false);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class IntersectByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
                => RewriteKeyedFilterSetOp(t, node, keepMatches: true);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class UnionByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (TryTranslateLocalUnionBy(t, node, out var translated))
                    return translated;

                return InstallKeyedSetOp(t, node, KeyedSetOp.Union);
            }

            private static bool TryTranslateLocalUnionBy(QueryTranslator t, MethodCallExpression node, out Expression translated)
            {
                translated = node;
                if (node.Arguments.Count < 3
                    || StripQuotes(node.Arguments[2]) is not LambdaExpression keyLambda
                    || !TryGetConstantValue(node.Arguments[1], out var secondValue)
                    || secondValue is not System.Collections.IEnumerable secondEnumerable)
                {
                    return false;
                }

                var secondRows = new List<object>();
                foreach (var item in secondEnumerable)
                {
                    if (item is null)
                        return false;
                    secondRows.Add(item);
                }

                if (secondRows.Count == 0)
                {
                    var sourceType = keyLambda.Parameters[0].Type;
                    var keyType = keyLambda.Body.Type;
                    var distinctByMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                        .First(m => m.Name == nameof(Queryable.DistinctBy)
                                    && m.GetParameters().Length == 2
                                    && m.GetGenericArguments().Length == 2)
                        .MakeGenericMethod(sourceType, keyType);
                    translated = t.Visit(Expression.Call(distinctByMethod, node.Arguments[0], Expression.Quote(keyLambda)));
                    return true;
                }

                var subPlan = t.TranslateInSubContext(
                    node.Arguments[0],
                    t._mapping,
                    t._parameterManager.Index,
                    t._joinCounter,
                    t._recursionDepth + 1,
                    out var subMapping);
                t._mapping = subMapping;
                t.MergeSubPlanParameters(subPlan);

                var sourceAlias = t.EscapeAlias("__ubsrc" + t._joinCounter++);
                var combinedAlias = t.EscapeAlias("__ubc" + t._joinCounter++);
                var rankedAlias = t.EscapeAlias("__ubr" + t._joinCounter++);
                var srcFlag = t._provider.Escape("__norm_union_src");
                var ordFlag = t._provider.Escape("__norm_union_ord");
                var rnFlag = t._provider.Escape("__norm_union_rn");
                var sourceSql = RemoveTrailingOrderBy(node.Arguments[0], subPlan.Sql);
                var sourceOrder = BuildUnionOrderSql(t, node.Arguments[0], subMapping, sourceAlias);
                var sourceSelectCols = string.Join(", ", subMapping.Columns.Select(c => $"{sourceAlias}.{c.EscCol}"));
                var finalSelectCols = PooledStringBuilder.Join(subMapping.Columns.Select(c => $"{rankedAlias}.{c.EscCol}"));

                var combined = PooledStringBuilder.Rent();
                try
                {
                    combined.Append("SELECT ").Append(sourceSelectCols)
                        .Append(", 0 AS ").Append(srcFlag)
                        .Append(", ROW_NUMBER() OVER (ORDER BY ").Append(PooledStringBuilder.JoinOrderBy(sourceOrder))
                        .Append(") AS ").Append(ordFlag)
                        .Append(" FROM (").Append(sourceSql).Append(") AS ").Append(sourceAlias);

                    for (var rowIndex = 0; rowIndex < secondRows.Count; rowIndex++)
                    {
                        combined.Append(" UNION ALL SELECT ");
                        for (var colIndex = 0; colIndex < subMapping.Columns.Length; colIndex++)
                        {
                            if (colIndex > 0)
                                combined.Append(", ");
                            var column = subMapping.Columns[colIndex];
                            var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
                            t.AddLiteralParameter(pName, column.Prop.GetValue(secondRows[rowIndex]));
                            combined.Append(pName).Append(" AS ").Append(column.EscCol);
                        }
                        combined.Append(", 1 AS ").Append(srcFlag)
                            .Append(", ").Append(rowIndex.ToString(System.Globalization.CultureInfo.InvariantCulture))
                            .Append(" AS ").Append(ordFlag);
                    }

                    var keySql = BuildUnionKeySql(t, keyLambda, subMapping, combinedAlias);
                    t._sql.Append("SELECT ").Append(finalSelectCols).Append(" FROM (SELECT ")
                        .Append(combinedAlias).Append(".*, ROW_NUMBER() OVER (PARTITION BY ")
                        .Append(keySql).Append(" ORDER BY ").Append(combinedAlias).Append('.').Append(srcFlag)
                        .Append(" ASC, ").Append(combinedAlias).Append('.').Append(ordFlag).Append(" ASC) AS ").Append(rnFlag)
                        .Append(" FROM (").Append(combined.ToString()).Append(") AS ").Append(combinedAlias)
                        .Append(") AS ").Append(rankedAlias)
                        .Append(" WHERE ").Append(rankedAlias).Append('.').Append(rnFlag).Append(" = 1");

                    t._orderBy.Add(($"{rankedAlias}.{srcFlag}", true));
                    t._orderBy.Add(($"{rankedAlias}.{ordFlag}", true));
                    translated = node;
                    return true;
                }
                finally
                {
                    PooledStringBuilder.Return(combined);
                }
            }

            private static string BuildUnionKeySql(QueryTranslator t, LambdaExpression keyLambda, TableMapping mapping, string alias)
            {
                var parts = new List<string>();
                if (keyLambda.Body is NewExpression newKey && newKey.Arguments.Count > 0)
                {
                    foreach (var arg in newKey.Arguments)
                        parts.Add(DistinctByTranslator.OrdinalPartitionKey(t, BuildUnionSql(t, keyLambda.Parameters[0], arg, mapping, alias), arg.Type));
                }
                else
                {
                    parts.Add(DistinctByTranslator.OrdinalPartitionKey(t, BuildUnionSql(t, keyLambda.Parameters[0], keyLambda.Body, mapping, alias), keyLambda.Body.Type));
                }
                return string.Join(", ", parts);
            }

            private static List<(string col, bool asc)> BuildUnionOrderSql(QueryTranslator t, Expression source, TableMapping mapping, string alias)
            {
                var orderKeys = ExtractOrderByKeys(source);
                var result = new List<(string col, bool asc)>();
                if (orderKeys.Count == 0)
                {
                    foreach (var key in mapping.KeyColumns)
                        result.Add(($"{alias}.{key.EscCol}", true));
                    return result;
                }

                foreach (var (keyLambda, ascending) in orderKeys)
                    result.Add((BuildUnionSql(t, keyLambda.Parameters[0], keyLambda.Body, mapping, alias), ascending));
                return result;
            }

            private static string BuildUnionSql(QueryTranslator t, ParameterExpression parameter, Expression expression, TableMapping mapping, string alias)
            {
                t._correlatedParams[parameter] = (mapping, alias);
                var vctx = new VisitorContext(t._ctx, mapping, t._provider, parameter, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var sql = visitor.Translate(expression);
                foreach (var kvp in visitor.GetParameters())
                    t._params[kvp.Key] = kvp.Value;
                if (t._params.Count > t._parameterManager.Index)
                    t._parameterManager.Index = t._params.Count;
                FastExpressionVisitorPool.Return(visitor);
                var type = Nullable.GetUnderlyingType(expression.Type) ?? expression.Type;
                return t._provider.ExactKeySql(sql, type);
            }

            private static string RemoveTrailingOrderBy(Expression source, string sql)
            {
                if (SourceHasTakeOrSkip(source))
                    return sql;
                return RemoveTrailingOrderByUnlessPaged(sql);
            }
        }

        /// <summary>
        /// Implements DefaultIfEmpty standalone (not via GroupJoin left-join).
        /// Post-materialize transform: if the materialized list is empty, append
        /// null (no-arg form) or the provided default value (1-arg form).
        /// Mirrors LINQ-to-Objects semantics exactly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class DefaultIfEmptyTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var elementType = node.Method.GetGenericArguments()[0];

                // Determine the default element: null for no-arg, provided value for 1-arg.
                object? defaultVal = elementType.IsValueType && Nullable.GetUnderlyingType(elementType) == null
                    ? Activator.CreateInstance(elementType)
                    : null;
                if (node.Arguments.Count >= 2)
                {
                    if (TryGetConstantValue(node.Arguments[1], out var dv))
                        defaultVal = dv;
                    else
                        defaultVal = Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke();
                }

                var captured = defaultVal;
                System.Collections.IList ApplyDefault(DbContext ctx, System.Collections.IList list)
                {
                    var output = CreateRuntimeList(elementType, Math.Max(list.Count, 1));
                    foreach (var item in list)
                        output.Add(item);

                    if (list.Count == 0)
                        output.Add(captured);
                    return output;
                }

                // Visit the source FIRST, then append. Setting the transform before the
                // source walk gives inner translators (Where's client-tail filter branch,
                // scalar aggregates) a false positive on "reshape pending", and an inner
                // reshape like Append would compose in the wrong order — DefaultIfEmpty
                // is the outer operator, so it must see the already-reshaped rows.
                var source = t.Visit(node.Arguments[0]);
                t.AppendPostMaterializeTransform(ApplyDefault, elementType);
                return source;
            }
        }

        private enum KeyedSetOp { Except, Intersect, Union }

        private static Expression RewriteKeyedFilterSetOp(QueryTranslator t, MethodCallExpression node, bool keepMatches)
        {
            if (node.Arguments.Count < 3)
            {
                throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{node.Method.Name} requires a second key collection and a key selector."));
            }

            var keyLambda = StripQuotes(node.Arguments[2]) as LambdaExpression
                ?? throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{node.Method.Name} requires a key selector lambda."));

            var sourceType = keyLambda.Parameters[0].Type;
            var keyType = keyLambda.Body.Type;
            var containsMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .First(m => m.Name == nameof(Enumerable.Contains)
                            && m.GetParameters().Length == 2
                            && m.GetGenericArguments().Length == 1)
                .MakeGenericMethod(keyType);
            Expression contains = Expression.Call(containsMethod, node.Arguments[1], keyLambda.Body);
            var predicateBody = keepMatches ? contains : Expression.Not(contains);
            var predicate = Expression.Lambda(predicateBody, keyLambda.Parameters);

            var whereMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .First(m => m.Name == nameof(Queryable.Where)
                            && m.GetParameters().Length == 2
                            && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 2)
                .MakeGenericMethod(sourceType);
            var filtered = Expression.Call(whereMethod, node.Arguments[0], Expression.Quote(predicate));

            var distinctByMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .First(m => m.Name == nameof(Queryable.DistinctBy)
                            && m.GetParameters().Length == 2
                            && m.GetGenericArguments().Length == 2)
                .MakeGenericMethod(sourceType, keyType);
            var distinct = Expression.Call(distinctByMethod, filtered, Expression.Quote(keyLambda));
            return t.Visit(distinct);
        }

        private static Expression InstallKeyedSetOp(QueryTranslator t, MethodCallExpression node, KeyedSetOp op)
        {
            // node.Arguments: [0] = source IQueryable<TSource>,
            //                 [1] = second IEnumerable<TKey> (Except/Intersect) or
            //                       IEnumerable<TSource> (Union),
            //                 [2] = key selector lambda.
            if (node.Arguments.Count < 3)
            {
                throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{node.Method.Name} requires a second collection and a key selector."));
            }
            var keyLambda = StripQuotes(node.Arguments[2]) as LambdaExpression
                ?? throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{node.Method.Name} requires a key selector lambda."));

            var entityType = keyLambda.Parameters[0].Type;
            var keySelector = CompileKeySelector(keyLambda);

            // Compile a delegate that LAZILY reads the second collection from its
            // closure access expression. The QueryPlan cache keys by expression
            // fingerprint -- if we captured the IEnumerable's value at translate
            // time, repeated calls with different captured collections would reuse
            // the stale-value transform. Reading live on each transform invocation
            // mirrors how nORM's CompiledParameters re-extract closure values for
            // SQL parameters per-call.
            var secondExpr = node.Arguments[1];
            // Box to non-generic IEnumerable so the delegate type is fixed regardless
            // of whether the user passed IEnumerable<TKey> (Except/Intersect) or
            // IEnumerable<TSource> (Union).
            Expression secondCast = typeof(System.Collections.IEnumerable).IsAssignableFrom(secondExpr.Type)
                ? secondExpr
                : Expression.Convert(secondExpr, typeof(System.Collections.IEnumerable));
            var secondLookup = Expression.Lambda<Func<System.Collections.IEnumerable?>>(secondCast).Compile();

            System.Collections.IList Transform(DbContext ctx, System.Collections.IList sourceList)
            {
                var second = secondLookup() ?? throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"{op}'s second argument resolved to null at invocation time."));

                // Materialize the second collection's keys for this invocation.
                //   Except/Intersect -> second is IEnumerable<TKey> directly
                //   Union            -> second is IEnumerable<TSource>; apply keySelector
                var secondKeys = new HashSet<object?>();
                object?[]? unionAppendRows = null;
                if (op == KeyedSetOp.Union)
                {
                    var rows = new List<object?>();
                    foreach (var item in second)
                    {
                        var key = item is null ? null : keySelector(item);
                        secondKeys.Add(key);
                        rows.Add(item);
                    }
                    unionAppendRows = rows.ToArray();
                }
                else
                {
                    foreach (var item in second) secondKeys.Add(item);
                }

                var result = QueryExecutor.CreateList(entityType, sourceList.Count);
                var seenInResult = new HashSet<object?>();
                foreach (var item in sourceList)
                {
                    var key = item is null ? null : keySelector(item);
                    bool keep = op switch
                    {
                        KeyedSetOp.Except => !secondKeys.Contains(key),
                        KeyedSetOp.Intersect => secondKeys.Contains(key),
                        // Union starts by taking source rows; dedupe by key.
                        KeyedSetOp.Union => true,
                        _ => true
                    };
                    if (keep && seenInResult.Add(key))
                        result.Add(item);
                }
                if (op == KeyedSetOp.Union && unionAppendRows != null)
                {
                    foreach (var item in unionAppendRows)
                    {
                        var key = item is null ? null : keySelector(item);
                        if (seenInResult.Add(key))
                            result.Add(item);
                    }
                }
                return result;
            }

            t._postMaterializeTransform = Transform;
            return t.Visit(node.Arguments[0]);
        }

        private static Func<object, object?> CompileKeySelector(LambdaExpression keyLambda)
        {
            var entityType = keyLambda.Parameters[0].Type;
            var keyType = keyLambda.Body.Type;
            var entityObjParam = Expression.Parameter(typeof(object), "entity");
            var castEntity = Expression.Convert(entityObjParam, entityType);
            var body = new ParameterReplacer(keyLambda.Parameters[0], castEntity).Visit(keyLambda.Body)!;
            var boxedBody = keyType.IsValueType ? (Expression)Expression.Convert(body, typeof(object)) : body;
            return Expression.Lambda<Func<object, object?>>(boxedBody, entityObjParam).Compile();
        }

    }
}
