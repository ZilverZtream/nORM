using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        /// <summary>True for an identity aggregate selector <c>x =&gt; x</c> (optionally with a conversion),
        /// whose operand is the group element itself.</summary>
        private static bool IsIdentityLambda(LambdaExpression lambda)
        {
            if (lambda.Parameters.Count != 1)
                return false;
            var body = lambda.Body;
            while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                body = u.Operand;
            return body is ParameterExpression p && ReferenceEquals(p, lambda.Parameters[0]);
        }

        private Expression? TryTranslateEnumerableOrQueryableMethod(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Enumerable) || node.Method.DeclaringType == typeof(Queryable))
            {
                // Detect nested aggregates on a mapped navigation collection in a predicate context,
                // e.g. `parent.Children.Any(c => c.Foo > x)`. The receiver `parent.Children` is a
                // CLR List/IEnumerable, not an IQueryable, so it normally cannot translate. We
                // rewrite it into a correlated subquery on the dependent table joined by the
                // relation's FK, then let the existing Queryable.Any/All/Count handlers run.
                if (node.Arguments.Count >= 1 &&
                    node.Arguments[0] is MemberExpression navMember &&
                    navMember.Expression is ParameterExpression navParent &&
                    _parameterMappings.TryGetValue(navParent, out var navParentInfo) &&
                    navParentInfo.Mapping.Relations.TryGetValue(navMember.Member.Name, out var relation) &&
                    (node.Method.Name is nameof(Queryable.Any)
                                      or nameof(Queryable.All)
                                      or nameof(Queryable.Count)
                                      or nameof(Queryable.LongCount)))
                {
                    var rewritten = RewriteNavigationAggregate(node, navParent, relation);
                    Visit(rewritten);
                    return node;
                }

                // The same aggregate shapes over an OWNED or MANY-TO-MANY collection — separate mapping
                // structures the relation rewrite above can't reach. Emit the correlated subquery directly.
                if (TryEmitNonRelationCollectionPredicateAggregate(node))
                    return node;

                // `parent.Children.Select(c => c.Value).Sum/Min/Max/Average()` - the Select
                // projection sits between the navigation and the aggregate. Emit as a
                // correlated scalar subquery directly:
                //   `(SELECT AGG(selector_sql) FROM ChildTable T1 WHERE T1.FK = parent.PK)`
                if (node.Arguments.Count == 1
                    && node.Method.Name is nameof(Queryable.Sum)
                                       or nameof(Queryable.Min)
                                       or nameof(Queryable.Max)
                                       or nameof(Queryable.Average)
                    && node.Arguments[0] is MethodCallExpression selectCall
                    && selectCall.Method.Name == nameof(Queryable.Select)
                    && selectCall.Arguments.Count == 2
                    && selectCall.Arguments[0] is MemberExpression selNav
                    && selNav.Expression is ParameterExpression selNavParent
                    && _parameterMappings.TryGetValue(selNavParent, out var selNavInfo)
                    && selNavInfo.Mapping.Relations.TryGetValue(selNav.Member.Name, out var selRel)
                    && StripQuotes(selectCall.Arguments[1]) is LambdaExpression selectorLambda)
                {
                    EmitNavigationScalarAggregateSubquery(
                        node.Method.Name, selNavParent, selNavInfo, selRel, selectorLambda);
                    return node;
                }
                // Direct selector overload — `parent.Children.Sum/Min/Max/Average(c => c.Value)`
                // is semantically Select(selector).Aggregate(); emit the same correlated scalar
                // subquery as the Select-then-aggregate shape above.
                if (node.Arguments.Count == 2
                    && node.Method.Name is nameof(Queryable.Sum)
                                       or nameof(Queryable.Min)
                                       or nameof(Queryable.Max)
                                       or nameof(Queryable.Average)
                    && node.Arguments[0] is MemberExpression dirNav
                    && dirNav.Expression is ParameterExpression dirNavParent
                    && _parameterMappings.TryGetValue(dirNavParent, out var dirNavInfo)
                    && dirNavInfo.Mapping.Relations.TryGetValue(dirNav.Member.Name, out var dirRel)
                    && StripQuotes(node.Arguments[1]) is LambdaExpression dirSelector)
                {
                    EmitNavigationScalarAggregateSubquery(
                        node.Method.Name, dirNavParent, dirNavInfo, dirRel, dirSelector);
                    return node;
                }
                switch (node.Method.Name)
                {
                    case nameof(Queryable.GroupBy):
                        HandleGroupByMethod(node);
                        return node;
                    case "Count":
                    case "LongCount":
                        if (node.Arguments.Count >= 1
                            && node.Arguments[0] is ParameterExpression cp
                            && (_parameterMappings.ContainsKey(cp) || _groupingKeys.ContainsKey(cp)))
                        {
                            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression countSelector)
                            {
                                (TableMapping Mapping, string Alias) info = _parameterMappings.TryGetValue(cp, out var existing)
                                    ? existing
                                    : (_mapping, _tableAlias);
                                // paramIndexStart=_paramIndex prevents the inner countSelector's
                                // @p0 from colliding with this visitor's outer-scope @p0 -- the
                                // pooled inner visitor would otherwise overwrite the outer
                                // parameter value in _params. Reclaim _paramIndex after the
                                // sub-translate so subsequent outer params keep their own slots.
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _paramIndex);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var predSql = visitor.Translate(countSelector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                _paramIndex = visitor.ParamIndex;
                                _sql.Append($"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)");
                                FastExpressionVisitorPool.Return(visitor);
                            }
                            else
                            {
                                _sql.Append("COUNT(*)");
                            }
                            return node;
                        }
                        // Count/LongCount over a query source in a predicate context: emit a
                        // correlated scalar subquery (SELECT COUNT(*) FROM child WHERE ...).
                        if (node.Arguments.Count >= 1)
                        {
                            var countLambda = node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null;
                            BuildScalarCountSubquery(node.Arguments[0], countLambda);
                            return node;
                        }
                        break;
                    case "Sum":
                    case "Average":
                    case "Min":
                    case "Max":
                        if (node.Arguments.Count >= 1
                            && node.Arguments[0] is ParameterExpression gp
                            && (_parameterMappings.ContainsKey(gp) || _groupingKeys.ContainsKey(gp)))
                        {
                            var selector = node.Arguments.Count >= 2 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null;
                            // Element-selector group: a parameterless (g.Sum()) or identity (g.Sum(x => x))
                            // aggregate takes the element selector's body as its operand, e.g. SUM(Amount).
                            if (_groupingElementSelectors.TryGetValue(gp, out var elementSelector)
                                && (selector == null || IsIdentityLambda(selector)))
                                selector = elementSelector;
                            if (selector != null)
                            {
                                // GroupBy aggregate inside HAVING: the IGrouping parameter is in
                                // _groupingKeys, not _parameterMappings. The selector's element
                                // parameter belongs to the same row source as the outer query
                                // (the GroupBy hasn't introduced a new table), so reuse this
                                // visitor's mapping + alias.
                                (TableMapping Mapping, string Alias) info = _parameterMappings.TryGetValue(gp, out var existing)
                                    ? existing
                                    : (_mapping, _tableAlias);
                                // Same paramIndex offset pattern as the Count branch above: prevent
                                // the inner selector's @p0 from overwriting an outer-scope @p0 in
                                // _params under chains like Where(g => g.Sum(x => x.Amount > N) > M).
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _paramIndex);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var colSql = visitor.Translate(selector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                _paramIndex = visitor.ParamIndex;
                                var fn = node.Method.Name switch
                                {
                                    "Sum" => "SUM",
                                    "Average" => "AVG",
                                    "Min" => "MIN",
                                    "Max" => "MAX",
                                    _ => "",
                                };
                                _sql.Append($"{fn}({colSql})");
                                FastExpressionVisitorPool.Return(visitor);
                                return node;
                            }
                        }
                        // Aggregate over a query source in a predicate context: emit a
                        // correlated scalar subquery, mirroring the Count branch above.
                        // Grouping/range parameters are handled by the branch above; a
                        // bare parameter reaching here has no query source to translate.
                        if (node.Arguments.Count >= 1 && node.Arguments[0] is not ParameterExpression)
                        {
                            var aggSelector = node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null;
                            BuildScalarAggregateSubquery(node.Arguments[0], aggSelector, node.Method.Name);
                            return node;
                        }
                        break;
                }
            }
            // Treat Dictionary<K,V>.ContainsKey(k)/ContainsValue(v) as Keys.Contains(k)/
            // Values.Contains(v): same LINQ semantics, same IN-clause SQL emit, but we
            // must walk just the keys or values respectively -- the dictionary's default
            // IEnumerable yields KeyValuePair items which would compare KVP instances to
            // a K (or V) value and never match.
            var isDictContains = (node.Method.Name == "ContainsKey" || node.Method.Name == "ContainsValue")
                && node.Object != null
                && node.Arguments.Count == 1
                && node.Method.DeclaringType is { } dictDt
                && IsDictionaryLikeReceiver(dictDt);
            if (node.Method.Name == nameof(List<int>.Contains) || isDictContains)
            {
                Expression? collectionExpr = null;
                Expression? valueExpr = null;
                if (node.Method.DeclaringType == typeof(Enumerable))
                {
                    if (node.Arguments.Count == 2)
                    {
                        collectionExpr = node.Arguments[0];
                        valueExpr = node.Arguments[1];
                    }
                }
                else if (node.Object != null && node.Arguments.Count == 1)
                {
                    collectionExpr = node.Object;
                    valueExpr = node.Arguments[0];
                }
                if (collectionExpr != null && valueExpr != null && TryGetConstantValue(collectionExpr, out var colVal) && colVal is IEnumerable en && colVal is not string)
                {
                    // Reserve a placeholder compiled-param slot when the IN-list
                    // collection is a closure-captured MemberExpression. The
                    // ParameterValueExtractor walks every closure MemberExpression
                    // in document order and appends a value to its list; without
                    // a placeholder the array reference shifts subsequent @cp
                    // bindings by one slot (or worse, gets bound directly to a
                    // @cp slot and crashes with "no mapping from System.Int32[]
                    // to a known managed provider native type"). Same fix shape
                    // as 407e03d / eeff6e7 / cf39b61.
                    if (collectionExpr is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                    var items = new List<object?>();
                    System.Collections.IEnumerable itemSource = en;
                    if (isDictContains && colVal is System.Collections.IDictionary dict)
                    {
                        itemSource = node.Method.Name == "ContainsValue" ? (System.Collections.IEnumerable)dict.Values : (System.Collections.IEnumerable)dict.Keys;
                    }
                    foreach (var item in itemSource)
                        items.Add(item);
                    if (items.Count == 0)
                    {
                        _sql.Append(SqlFalseLiteral);
                        return node;
                    }

                    // Separate nulls from non-nulls: SQL `col IN (NULL, @p1)` never matches null
                    // rows - only `col IS NULL` does. Emit (col IN (...) OR col IS NULL) when needed.
                    bool hasNulls = items.Any(x => x is null);
                    var nonNullItems = hasNulls ? items.Where(x => x != null).ToList() : items;

                    // All-nulls case: emit col IS NULL with no parameters (IN () is invalid SQL).
                    if (nonNullItems.Count == 0)
                    {
                        Visit(valueExpr);
                        _sql.Append(" IS NULL");
                        return node;
                    }

                    // Exact accounting: _paramIndex tracks all params added so far.
                    // Nulls cost no parameters, so use nonNullItems.Count for the budget check.
                    var remainingParams = _provider.MaxParameters - _paramIndex;
                    if (nonNullItems.Count > remainingParams)
                        throw new NormQueryException(
                            $"IN clause with {nonNullItems.Count} items exceeds remaining parameter budget " +
                            $"({remainingParams} available, {_paramIndex} already used, limit {_provider.MaxParameters}). " +
                            "Consider using a temporary table or reducing the number of items.");

                    // Optimizer batching (1000 items per IN clause for DB plan efficiency).
                    // This is decoupled from parameter limits.
                    // NOTE: When the collection exceeds MaxInClauseItems, each batch re-visits
                    // valueExpr to emit the column reference (e.g., "T0.[Col] IN (...) OR T0.[Col] IN (...)").
                    // This means the SQL string grows linearly with the number of batches (one column
                    // reference per batch). For very large collections this is a deliberate tradeoff:
                    // multiple smaller IN clauses let the query optimizer produce better plans than a
                    // single massive IN list, at the cost of a slightly larger SQL string and plan
                    // cache variance (different collection sizes produce different SQL shapes).
                    // If the tested column carries a value converter, the list holds RAW model values but
                    // the column stores PROVIDER values — bind the converted representation, or the IN
                    // matches nothing (silent-wrong). Mirrors the scalar `==` path (EmitConvertedValueOperand).
                    // Restricted to a direct member so detection never emits SQL as a side effect (the
                    // two-level navigation form of TryGetConverterColumn can lower a scalar subquery).
                    nORM.Mapping.IValueConverter? inConverter = null;
                    if (StripConvert(valueExpr) is MemberExpression inMember
                        && TableMapping.TryGetMemberAccessRoot(inMember, out var inRoot)
                        && _parameterMappings.TryGetValue(inRoot, out var inInfo)
                        && inInfo.Mapping.TryGetColumnForMemberAccess(inMember, out var inCol)
                        && inCol.Converter != null)
                    {
                        inConverter = inCol.Converter;
                    }

                    // C# list.Contains(string) is ordinal; on providers whose default collation
                    // makes IN case-insensitive (MySQL, SQL Server), pair each IN with a
                    // ForceCaseSensitiveStringComparison IN over the SAME parameters — the plain
                    // IN narrows through any index, the binary IN filters exactly. A converter that
                    // STORES a string (e.g. an enum->string converter) can hold case-variant members
                    // (Active/active) that a CI-collation IN would silently fuse, so wrap those too —
                    // the tested expression's CLR type is the enum, so the string check alone misses it.
                    bool ordinalStringIn = _provider.DefaultStringEqualityIsCaseInsensitive
                        && ((Nullable.GetUnderlyingType(valueExpr.Type) ?? valueExpr.Type) == typeof(string)
                            || (Nullable.GetUnderlyingType(valueExpr.Type) ?? valueExpr.Type) == typeof(char)
                            || inConverter?.ProviderType == typeof(string));

                    // Emits `col IN (@p…)` (registering the parameters once) and returns the
                    // rendered `(@p…)` list so the ordinal wrap can reference the same names.
                    string EmitInList(IEnumerable<object?> items)
                    {
                        var listStart = _sql.Length;
                        _sql.Append(" IN (");
                        bool first = true;
                        foreach (var item in items)
                        {
                            if (!first) _sql.Append(", ");
                            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            var bound = inConverter != null && item != null ? inConverter.ConvertToProvider(item) : item;
                            _sql.AppendParameterizedValue(paramName, bound, _paramSink);
                            first = false;
                        }
                        _sql.Append(")");
                        return _sql.ToString(listStart, _sql.Length - listStart);
                    }

                    void EmitMembershipTest(IEnumerable<object?> items)
                    {
                        var colSql = GetSql(valueExpr);
                        if (ordinalStringIn) _sql.Append('(');
                        _sql.Append(colSql);
                        var renderedList = EmitInList(items);
                        if (ordinalStringIn)
                        {
                            _sql.Append(" AND ")
                                .Append(_provider.ForceCaseSensitiveStringComparison(colSql))
                                .Append(renderedList)
                                .Append(')');
                        }
                    }

                    const int MaxInClauseItems = 1000;
                    if (nonNullItems.Count > MaxInClauseItems)
                    {
                        if (hasNulls) _sql.Append("(");
                        _sql.Append("(");
                        for (int batch = 0; batch < nonNullItems.Count; batch += MaxInClauseItems)
                        {
                            if (batch > 0) _sql.Append(" OR ");
                            EmitMembershipTest(nonNullItems.Skip(batch).Take(MaxInClauseItems));
                        }
                        _sql.Append(")");
                        if (hasNulls)
                        {
                            _sql.Append(" OR ");
                            Visit(valueExpr);
                            _sql.Append(" IS NULL)");
                        }
                    }
                    else
                    {
                        if (hasNulls) _sql.Append("(");
                        EmitMembershipTest(nonNullItems);
                        if (hasNulls)
                        {
                            _sql.Append(" OR ");
                            Visit(valueExpr);
                            _sql.Append(" IS NULL)");
                        }
                    }
                    return node;
                }
            }
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.Any):
                        BuildExists(node.Arguments[0], node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null, negate: false);
                        return node;
                    case nameof(Queryable.All):
                        if (node.Arguments.Count < 2)
                            throw new NormQueryException("All() requires a predicate argument.");
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new NormQueryException("All() requires a predicate lambda expression.");
                        var param = pred.Parameters[0];
                        var notBody = Expression.Not(pred.Body);
                        var lambda = Expression.Lambda(notBody, param);
                        BuildExists(node.Arguments[0], lambda, negate: true);
                        return node;
                    case nameof(Queryable.Contains):
                        BuildIn(node.Arguments[0], node.Arguments[1]);
                        return node;
                    case nameof(Queryable.Count):
                    case nameof(Queryable.LongCount):
                    {
                        LambdaExpression? countPredicate = null;
                        if (node.Arguments.Count > 1)
                        {
                            countPredicate = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (countPredicate == null)
                                throw new NormQueryException($"{node.Method.Name}() requires a lambda predicate argument.");
                        }
                        BuildScalarCountSubquery(node.Arguments[0], countPredicate);
                        return node;
                    }
                    case nameof(Queryable.Sum):
                    case nameof(Queryable.Min):
                    case nameof(Queryable.Max):
                    case nameof(Queryable.Average):
                    {
                        LambdaExpression? aggregateSelector = null;
                        if (node.Arguments.Count > 1)
                        {
                            aggregateSelector = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (aggregateSelector == null)
                                throw new NormQueryException($"{node.Method.Name}() requires a lambda selector argument.");
                        }
                        BuildScalarAggregateSubquery(node.Arguments[0], aggregateSelector, node.Method.Name);
                        return node;
                    }
                    case nameof(Queryable.First):
                    case nameof(Queryable.FirstOrDefault):
                    case nameof(Queryable.Last):
                    case nameof(Queryable.LastOrDefault):
                    {
                        LambdaExpression? firstPredicate = null;
                        if (node.Arguments.Count > 1)
                        {
                            firstPredicate = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (firstPredicate == null)
                                throw new NormQueryException($"{node.Method.Name}() requires a lambda predicate argument.");
                        }
                        BuildScalarFirstSubquery(node.Arguments[0], firstPredicate, node.Method.Name);
                        return node;
                    }
                    case nameof(Queryable.ElementAt):
                    case nameof(Queryable.ElementAtOrDefault):
                    {
                        if (node.Arguments.Count < 2)
                            throw new NormQueryException($"{node.Method.Name}() requires an index argument.");
                        BuildScalarFirstSubquery(node.Arguments[0], null, node.Method.Name, node.Arguments[1]);
                        return node;
                    }
                    default:
                        throw new NormUnsupportedFeatureException($"Queryable method '{node.Method.Name}' is not supported.");
                }
            }

            return null;
        }
    }
}
