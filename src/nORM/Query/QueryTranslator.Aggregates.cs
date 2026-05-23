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
        private Expression HandleAggregateExpression(MethodCallExpression node)
        {
            // node.Arguments[0] = source query
            // node.Arguments[1] = selector lambda
            // node.Arguments[2] = function name

            var sourceQuery = node.Arguments[0];
            var selectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
            var functionConstant = node.Arguments[2] as ConstantExpression;

            if (selectorLambda == null || functionConstant?.Value is not string functionName)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Invalid aggregate expression structure"));
            Visit(sourceQuery);

            var param = selectorLambda.Parameters[0];
            var alias = EscapeAlias("T" + _joinCounter);
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            var columnSql = visitor.Translate(selectorLambda.Body);
            foreach (var kvp in visitor.GetParameters())
                AddParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);
            _isAggregate = true;
            _sql.Clear();

            if (!AggregateFunctionMap.TryGetValue(functionName, out var sqlFunction))
                sqlFunction = functionName.ToUpperInvariant();

            _sql.AppendSelect(ReadOnlySpan<char>.Empty);
            _sql.AppendAggregateFunction(sqlFunction, columnSql);

            return node;
        }
        private Expression HandleGroupBy(MethodCallExpression node)
        {
            // GroupBy(source, keySelector) or GroupBy(source, keySelector, resultSelector)
            var sourceQuery = node.Arguments[0];
            var keySelectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;

            if (keySelectorLambda == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "GroupBy key selector must be a lambda expression"));

            // G1: Composite GroupBy keys (p => new { p.A, p.B }) are not supported.
            // Visiting a NewExpression without a VisitNew override causes ExpressionToSqlVisitor to
            // concatenate column SQL fragments without separators, producing invalid GROUP BY SQL
            // (e.g., "GROUP BY T0.[Category]T0.[Price]"). Fail early with a clear message.
            if (keySelectorLambda.Body is NewExpression)
                throw new NormQueryException(
                    "Composite GroupBy keys (e.g., p => new { p.A, p.B }) are not supported. " +
                    "Use a single column as the GroupBy key (e.g., p => p.Category), or pre-project " +
                    "to a scalar value before calling GroupBy.");

            Visit(sourceQuery);

            var param = keySelectorLambda.Parameters[0];
            var alias = EscapeAlias("T" + _joinCounter);
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var visitor = FastExpressionVisitorPool.Get(in vctx2);
            var groupBySql = visitor.Translate(keySelectorLambda.Body);
            foreach (var kvp in visitor.GetParameters())
                AddParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);
            _groupBy.Add(groupBySql);

            // If there's a result selector, handle the projection
            if (node.Arguments.Count > 2)
            {
                var resultSelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
                if (resultSelector != null)
                {
                    _projection = resultSelector;

                    // Clear the default select and let the projection handling rebuild it
                    _sql.Clear();

                    // Analyze the result selector to build appropriate SELECT clause
                    BuildGroupBySelectClause(resultSelector, groupBySql, alias);
                }
            }

            return node;
        }
        private void BuildGroupBySelectClause(LambdaExpression resultSelector, string groupBySql, string alias)
        {
            var selectItems = _selectItemsPool.Get();
            try
            {
                // Handle direct Key access: g => g.Key
                if (resultSelector.Body is MemberExpression memberExpr && memberExpr.Member.Name == "Key")
                {
                    var keyBuilder = PooledStringBuilder.Rent();
                    keyBuilder.Append(groupBySql).Append(" AS ").Append(_provider.Escape("Key"));
                    selectItems.Add(keyBuilder.ToString());
                    PooledStringBuilder.Return(keyBuilder);
                    _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                    _sql.AppendJoin(", ", selectItems);
                    return;
                }

                // Handle direct aggregate: g => g.Count()
                if (resultSelector.Body is MethodCallExpression methodCall)
                {
                    var aggregateSql = TranslateGroupAggregateMethod(methodCall, alias);
                    if (aggregateSql != null)
                    {
                        var aggregateBuilder = PooledStringBuilder.Rent();
                        // Include the group key for proper grouping
                        aggregateBuilder.Append(groupBySql).Append(" AS GroupKey");
                        selectItems.Add(aggregateBuilder.ToString());
                        PooledStringBuilder.Return(aggregateBuilder);

                        aggregateBuilder = PooledStringBuilder.Rent();
                        aggregateBuilder.Append(aggregateSql).Append(" AS ").Append(_provider.Escape("Value"));
                        selectItems.Add(aggregateBuilder.ToString());
                        PooledStringBuilder.Return(aggregateBuilder);

                        _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                        _sql.AppendJoin(", ", selectItems);
                        return;
                    }
                }

                var builderForKey = PooledStringBuilder.Rent();
                builderForKey.Append(groupBySql).Append(" AS GroupKey");
                selectItems.Add(builderForKey.ToString());
                PooledStringBuilder.Return(builderForKey);
                var builder = (System.Text.StringBuilder?)null;
                // Analyze the result selector body to find aggregates
                if (resultSelector.Body is NewExpression newExpr)
                {
                    for (int i = 0; i < newExpr.Arguments.Count; i++)
                    {
                        var arg = newExpr.Arguments[i];
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        if (arg is MethodCallExpression aggregateCall)
                        {
                            var aggregateSql = TranslateGroupAggregateMethod(aggregateCall, alias);
                            if (aggregateSql != null)
                            {
                                builder = PooledStringBuilder.Rent();
                                builder.Append(aggregateSql).Append(" AS ").Append(memberName);
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                            }
                        }
                        else if (arg is ParameterExpression param && param == resultSelector.Parameters[0])
                        {
                            // This is the key parameter, already added
                            continue;
                        }
                        else
                        {
                            // Try to translate as regular expression
                            if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                                _correlatedParams[resultSelector.Parameters[0]] = (_mapping, alias);
                            var vctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                            var visitor = FastExpressionVisitorPool.Get(in vctx);
                            var sql = visitor.Translate(arg);
                            builder = PooledStringBuilder.Rent();
                            builder.Append(sql).Append(" AS ").Append(memberName);
                            selectItems.Add(builder.ToString());
                            PooledStringBuilder.Return(builder);
                            foreach (var kvp in visitor.GetParameters())
                                AddParameter(kvp.Key, kvp.Value);
                            FastExpressionVisitorPool.Return(visitor);
                        }
                    }
                }
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.AppendJoin(", ", selectItems);
            }
            finally
            {
                _selectItemsPool.Return(selectItems);
            }
        }
        private string? TranslateGroupAggregateMethod(MethodCallExpression methodCall, string alias)
        {
            var methodName = methodCall.Method.Name;

            // Handle IGrouping<TKey, TElement> methods
            switch (methodName)
            {
                case "Count":
                    return "COUNT(*)";
                case "LongCount":
                    return "COUNT(*)";
                case "Sum":
                    {
                        // Extension method form: Enumerable.Sum(source, selector) — selector at [1]
                        // Instance method form: g.Sum(selector) — selector at [0]
                        var sumSelectorArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                            ? methodCall.Arguments[1] : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
                        var sumSelector = sumSelectorArg != null ? StripQuotes(sumSelectorArg) as LambdaExpression : null;
                        if (sumSelector != null)
                        {
                            if (!_correlatedParams.ContainsKey(sumSelector.Parameters[0]))
                                _correlatedParams[sumSelector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, sumSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(sumSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddParameter(kvp.Key, kvp.Value);
                            FastExpressionVisitorPool.Return(visitor);
                            return $"SUM({columnSql})";
                        }
                    }
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                        "SUM requires a selector expression (e.g., g.Sum(x => x.Amount)). SUM(*) is not valid SQL."));
                case "Average":
                    {
                        // Extension method form: Enumerable.Average(source, selector) — selector at [1]
                        // Instance method form: g.Average(selector) — selector at [0]
                        var avgSelectorArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                            ? methodCall.Arguments[1] : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
                        var avgSelector = avgSelectorArg != null ? StripQuotes(avgSelectorArg) as LambdaExpression : null;
                        if (avgSelector != null)
                        {
                            if (!_correlatedParams.ContainsKey(avgSelector.Parameters[0]))
                                _correlatedParams[avgSelector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, avgSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(avgSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddParameter(kvp.Key, kvp.Value);
                            FastExpressionVisitorPool.Return(visitor);
                            return $"AVG({columnSql})";
                        }
                    }
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                        "AVG requires a selector expression (e.g., g.Average(x => x.Amount)). AVG(*) is not valid SQL."));
                case "Min":
                    {
                        // Extension method form: Enumerable.Min(source, selector) — selector at [1]
                        // Instance method form (hypothetical): g.Min(selector) — selector at [0]
                        var selectorArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                            ? methodCall.Arguments[1] : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
                        var minSelector = selectorArg != null ? StripQuotes(selectorArg) as LambdaExpression : null;
                        if (minSelector != null)
                        {
                            if (!_correlatedParams.ContainsKey(minSelector.Parameters[0]))
                                _correlatedParams[minSelector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, minSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(minSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddParameter(kvp.Key, kvp.Value);
                            FastExpressionVisitorPool.Return(visitor);
                            return $"MIN({columnSql})";
                        }
                    }
                    return null;
                case "Max":
                    {
                        // Extension method form: Enumerable.Max(source, selector) — selector at [1]
                        // Instance method form (hypothetical): g.Max(selector) — selector at [0]
                        var selectorArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                            ? methodCall.Arguments[1] : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
                        var maxSelector = selectorArg != null ? StripQuotes(selectorArg) as LambdaExpression : null;
                        if (maxSelector != null)
                        {
                            if (!_correlatedParams.ContainsKey(maxSelector.Parameters[0]))
                                _correlatedParams[maxSelector.Parameters[0]] = (_mapping, alias);
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, maxSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(maxSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddParameter(kvp.Key, kvp.Value);
                            FastExpressionVisitorPool.Return(visitor);
                            return $"MAX({columnSql})";
                        }
                    }
                    return null;
                default:
                    return null;
            }
        }
        private Expression HandleDirectAggregate(MethodCallExpression node)
        {
            // Handle direct aggregate calls like query.Sum(x => x.Amount)
            var sourceQuery = node.Arguments[0];

            Visit(sourceQuery);

            if (node.Arguments.Count > 1 && StripQuotes(node.Arguments[1]) is LambdaExpression selector)
            {
                var param = selector.Parameters[0];
                var alias = EscapeAlias("T" + _joinCounter);
                if (!_correlatedParams.ContainsKey(param))
                    _correlatedParams[param] = (_mapping, alias);
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var columnSql = visitor.Translate(selector.Body);
                foreach (var kvp in visitor.GetParameters())
                    AddParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(visitor);
                _isAggregate = true;
                _sql.Clear();

                if (!AggregateFunctionMap.TryGetValue(node.Method.Name, out var sqlFunction))
                    sqlFunction = node.Method.Name.ToUpperInvariant();

                // Build complete SELECT ... FROM ... so the sql-length guard in Generate()
                // correctly skips the default SELECT/FROM assembly block.
                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.AppendAggregateFunction(sqlFunction, columnSql);
                _sql.AppendFragment(" FROM ").Append(_mapping.EscTable).Append(' ').Append(alias);
            }

            return node;
        }
        private Expression HandleAllOperation(MethodCallExpression node)
        {
            // ALL is translated as:
            // SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM table alias WHERE NOT (pred)) THEN 1 ELSE 0 END
            // The predicate MUST be embedded inside the subquery — do NOT use _where, which
            // Build() would append AFTER the closing parenthesis, breaking the EXISTS syntax.
            var sourceQuery = node.Arguments[0];
            var predicate = StripQuotes(node.Arguments[1]) as LambdaExpression;

            if (predicate == null)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "All operation requires a predicate"));
            Visit(sourceQuery);

            var param = predicate.Parameters[0];
            var alias = EscapeAlias("T" + _joinCounter);
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth);
            var visitor = FastExpressionVisitorPool.Get(in vctx2);
            var predicateSql = visitor.Translate(predicate.Body);
            // All() predicate parameters are fixed constants (not closure captures),
            // so add them only to _params (NOT _compiledParams) to ensure they are
            // bound directly rather than relying on ParameterValueExtractor.
            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value ?? DBNull.Value;
            FastExpressionVisitorPool.Return(visitor);

            // Build the complete SQL inline so the predicate is inside the EXISTS subquery.
            _sql.Insert(0,
                $"SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {_mapping.EscTable} {alias} WHERE NOT ({predicateSql})) THEN 1 ELSE 0 END");

            // Signal to Build() that this is a scalar query so it uses the scalar materializer
            // path (reads position 0, converts via Convert.ChangeType to bool).
            _isAggregate = true;

            return node;
        }
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
        private static Func<object, IEnumerable<object>, object> CompileGroupJoinResultSelector(LambdaExpression resultSelector)
        {
            var outerParam = Expression.Parameter(typeof(object), "outer");
            var innerParam = Expression.Parameter(typeof(IEnumerable<object>), "inners");
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
            body = new ParameterReplacer(resultSelector.Parameters[0], castOuter).Visit(body)!;
            body = new ParameterReplacer(resultSelector.Parameters[1], castInner).Visit(body)!;
            body = Expression.Convert(body, typeof(object));
            var lambda = Expression.Lambda<Func<object, IEnumerable<object>, object>>(body, outerParam, innerParam);
            ExpressionUtils.ValidateExpression(lambda);
            var timeout = ExpressionUtils.GetCompilationTimeout(lambda);
            using var cts = new CancellationTokenSource(timeout);
            return ExpressionUtils.CompileWithFallback(lambda, cts.Token);
        }
        private sealed class ProjectionMemberReplacer : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression is NewExpression newExpr)
                {
                    if (newExpr.Members != null)
                    {
                        for (int i = 0; i < newExpr.Members.Count; i++)
                        {
                            if (newExpr.Members[i].Name == node.Member.Name)
                                return Visit(newExpr.Arguments[i]);
                        }
                    }

                    var parameters = newExpr.Constructor?.GetParameters();
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length && i < newExpr.Arguments.Count; i++)
                        {
                            if (string.Equals(parameters[i].Name, node.Member.Name, StringComparison.OrdinalIgnoreCase))
                                return Visit(newExpr.Arguments[i]);
                        }
                    }
                }
                return base.VisitMember(node);
            }
        }
    }
}
