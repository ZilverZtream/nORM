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
            var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            var columnSql = visitor.Translate(selectorLambda.Body);
            // See HandleGroupBy / OrderByTranslator / Join family: literal constants in
            // an aggregate selector (e.g. `Sum(x => x.Col ?? 0)`) must merge as
            // AddLiteralParameter so they don't get mis-flagged as compiled-runtime
            // placeholders and skipped at execution time.
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
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

            Visit(sourceQuery);

            var param = keySelectorLambda.Parameters[0];
            var alias = EscapeAlias("T" + _joinCounter);
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);

            string groupBySql;
            // Composite key (p => new { p.A, p.B }) → translate each arg individually so the
            // emitted GROUP BY clause is comma-separated, and remember the per-member SQL so
            // the result-projection path can resolve `g.Key.A` back to its column.
            if (keySelectorLambda.Body is NewExpression compositeKey)
            {
                var parts = new List<string>(compositeKey.Arguments.Count);
                for (int i = 0; i < compositeKey.Arguments.Count; i++)
                {
                    var vctxPart = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                    var partVisitor = FastExpressionVisitorPool.Get(in vctxPart);
                    var partSql = partVisitor.Translate(compositeKey.Arguments[i]);
                    foreach (var kvp in partVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(partVisitor);
                    parts.Add(partSql);
                    _groupBy.Add(partSql);
                    var memberName = compositeKey.Members?[i]?.Name ?? $"Item{i + 1}";
                    _compositeKeyMemberSql[memberName] = partSql;
                }
                groupBySql = string.Join(", ", parts);
            }
            else
            {
                var vctx2 = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx2);
                groupBySql = visitor.Translate(keySelectorLambda.Body);
                // Use AddLiteralParameter (not AddParameter) so the inline-constant `@p0` from
                // a COALESCE-fallback / literal-bearing key selector isn't also flagged in
                // `_compiledParams`. Flagging it makes BindPlanParameters treat the slot as a
                // compiled-query closure binding and skip the literal value at execution time —
                // SQLite then throws `Must add values for the following parameters`. Matches the
                // composite-key path above which already uses AddLiteralParameter.
                foreach (var kvp in visitor.GetParameters())
                    AddLiteralParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(visitor);
                _groupBy.Add(groupBySql);
            }

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
                // Direct Key projection. Covers:
                //  - g => g.Key             (inline 3-arg GroupBy with IGrouping projection)
                //  - (k, g) => k            (rewritten from 2-arg GroupBy + Select(g => g.Key))
                bool bodyIsKey = (resultSelector.Body is MemberExpression me && me.Member.Name == "Key")
                    || (resultSelector.Body is ParameterExpression pe
                        && resultSelector.Parameters.Count >= 1
                        && pe == resultSelector.Parameters[0]);
                if (bodyIsKey)
                {
                    var keyBuilder = PooledStringBuilder.Rent();
                    keyBuilder.Append(groupBySql).Append(" AS ").Append(_provider.Escape("Key"));
                    selectItems.Add(keyBuilder.ToString());
                    PooledStringBuilder.Return(keyBuilder);
                }
                else if (resultSelector.Body is MethodCallExpression methodCall
                         && TranslateGroupAggregateMethod(methodCall, alias) is { } aggregateSql)
                {
                    var groupBuilder = PooledStringBuilder.Rent();
                    groupBuilder.Append(groupBySql).Append(" AS GroupKey");
                    selectItems.Add(groupBuilder.ToString());
                    PooledStringBuilder.Return(groupBuilder);

                    var valueBuilder = PooledStringBuilder.Rent();
                    valueBuilder.Append(aggregateSql).Append(" AS ").Append(_provider.Escape("Value"));
                    selectItems.Add(valueBuilder.ToString());
                    PooledStringBuilder.Return(valueBuilder);
                }
                else
                {
                    var builder = (System.Text.StringBuilder?)null;
                    if (resultSelector.Body is NewExpression newExpr)
                    {
                        for (int i = 0; i < newExpr.Arguments.Count; i++)
                        {
                            var arg = newExpr.Arguments[i];
                            var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                            // g.Key inside a NewExpression projects the group key column.
                            if (arg is MemberExpression keyMember && keyMember.Member.Name == "Key")
                            {
                                builder = PooledStringBuilder.Rent();
                                builder.Append(groupBySql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            // Bare key parameter inside a 3-arg result selector: (k, g) => new { k, ... }.
                            if (arg is ParameterExpression keyParam
                                && resultSelector.Parameters.Count >= 1
                                && keyParam == resultSelector.Parameters[0])
                            {
                                builder = PooledStringBuilder.Rent();
                                builder.Append(groupBySql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            // Composite-key member access: `key.A` where key is the result selector's
                            // first parameter and A is one of the anonymous-type members on the
                            // original GroupBy key. Resolve to the per-member GROUP BY column SQL.
                            if (arg is MemberExpression compositeKeyAccess
                                && compositeKeyAccess.Expression is ParameterExpression maybeKey
                                && resultSelector.Parameters.Count >= 1
                                && maybeKey == resultSelector.Parameters[0]
                                && _compositeKeyMemberSql.TryGetValue(compositeKeyAccess.Member.Name, out var memberSql))
                            {
                                builder = PooledStringBuilder.Rent();
                                builder.Append(memberSql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                continue;
                            }
                            if (arg is MethodCallExpression aggregateCall)
                            {
                                var innerAggregate = TranslateGroupAggregateMethod(aggregateCall, alias);
                                if (innerAggregate != null)
                                {
                                    builder = PooledStringBuilder.Rent();
                                    builder.Append(innerAggregate).Append(" AS ").Append(_provider.Escape(memberName));
                                    selectItems.Add(builder.ToString());
                                    PooledStringBuilder.Return(builder);
                                }
                            }
                            else
                            {
                                if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                                    _correlatedParams[resultSelector.Parameters[0]] = (_mapping, alias);
                                var vctx = new VisitorContext(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var sql = visitor.Translate(arg);
                                builder = PooledStringBuilder.Rent();
                                builder.Append(sql).Append(" AS ").Append(_provider.Escape(memberName));
                                selectItems.Add(builder.ToString());
                                PooledStringBuilder.Return(builder);
                                // See HandleGroupBy simple-key path: AddLiteralParameter so the
                                // visitor's inline constants don't get mis-flagged as compiled.
                                foreach (var kvp in visitor.GetParameters())
                                    AddLiteralParameter(kvp.Key, kvp.Value);
                                FastExpressionVisitorPool.Return(visitor);
                            }
                        }
                    }
                }

                _sql.AppendSelect(ReadOnlySpan<char>.Empty);
                _sql.AppendJoin(", ", selectItems);
                // Build() skips its SELECT/FROM-prefix block when _sql.Length > 0, so we must
                // emit FROM ourselves. Subsequent clauses (WHERE/GROUP BY/ORDER BY) are appended
                // by the Build() pipeline as usual.
                _sql.AppendFragment(" FROM ").Append(_mapping.EscTable).Append(' ').Append(alias);
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
                case "LongCount":
                    {
                        // 1-arg `g.Count()` → COUNT(*). 2-arg `g.Count(predicate)` (extension
                        // method form Enumerable.Count(source, predicate) puts the lambda at [1])
                        // must apply the predicate: COUNT(CASE WHEN <pred> THEN 1 ELSE NULL END)
                        // — NULL excluded by SQL count semantics, matching .NET behaviour.
                        var predicateArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                            ? methodCall.Arguments[1] : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
                        var predicate = predicateArg != null ? StripQuotes(predicateArg) as LambdaExpression : null;
                        if (predicate == null) return "COUNT(*)";
                        if (!_correlatedParams.ContainsKey(predicate.Parameters[0]))
                            _correlatedParams[predicate.Parameters[0]] = (_mapping, alias);
                        var vctxPred = new VisitorContext(_ctx, _mapping, _provider, predicate.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                        var predVisitor = FastExpressionVisitorPool.Get(in vctxPred);
                        var predSql = predVisitor.Translate(predicate.Body);
                        foreach (var kvp in predVisitor.GetParameters())
                            AddLiteralParameter(kvp.Key, kvp.Value);
                        FastExpressionVisitorPool.Return(predVisitor);
                        return $"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)";
                    }
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
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, sumSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(sumSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddLiteralParameter(kvp.Key, kvp.Value);
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
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, avgSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(avgSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddLiteralParameter(kvp.Key, kvp.Value);
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
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, minSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(minSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddLiteralParameter(kvp.Key, kvp.Value);
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
                            var vctxSel = new VisitorContext(_ctx, _mapping, _provider, maxSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
                            var columnSql = visitor.Translate(maxSelector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                AddLiteralParameter(kvp.Key, kvp.Value);
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
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var columnSql = visitor.Translate(selector.Body);
                // AddLiteralParameter — see HandleAggregateExpression above.
                foreach (var kvp in visitor.GetParameters())
                    AddLiteralParameter(kvp.Key, kvp.Value);
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
                if (node.Expression is MemberInitExpression memberInit)
                {
                    foreach (var binding in memberInit.Bindings)
                    {
                        if (binding is MemberAssignment ma && ma.Member.Name == node.Member.Name)
                            return Visit(ma.Expression);
                    }
                }
                return base.VisitMember(node);
            }
        }
    }
}
