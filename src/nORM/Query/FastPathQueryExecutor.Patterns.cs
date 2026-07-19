using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal static partial class FastPathQueryExecutor
    {
        private static bool IsSimpleCountPattern(Expression expr, out bool hasPredicate)
        {
            hasPredicate = false;
            if (expr is not MethodCallExpression countCall ||
                (countCall.Method.Name != nameof(Queryable.Count) && countCall.Method.Name != nameof(Queryable.LongCount)))
            {
                return false;
            }
            if (countCall.Arguments.Count == 2)
            {
                if (Unwrap(countCall.Arguments[0]) is not ConstantExpression) return false;
                // StripQuotes: LINQ wraps lambda args in UnaryExpression{Quote()}.
                if (StripQuotes(countCall.Arguments[1]) is not LambdaExpression) return false;
                hasPredicate = true;
                return true;
            }
            if (countCall.Arguments.Count == 1)
            {
                if (Unwrap(countCall.Arguments[0]) is not ConstantExpression) return false;
                hasPredicate = false;
                return true;
            }
            return false;
        }
        private static bool IsSimpleWherePattern(Expression expr, out WhereInfo info, out int? takeCount)
        {
            info = default;
            takeCount = null;
            if (expr is MethodCallExpression takeCall && takeCall.Method.Name == nameof(Queryable.Take))
            {
                if (takeCall.Arguments[1] is ConstantExpression ce)
                    takeCount = (int)ce.Value!;
                else
                    return false;
                expr = takeCall.Arguments[0];
            }
            // Unwrap AsNoTracking/AsSplitQuery between Take and Where so that
            // queries like .Where(...).AsNoTracking().Take(10) hit the fast path.
            expr = Unwrap(expr);
            if (expr is not MethodCallExpression whereCall || whereCall.Method.Name != nameof(Queryable.Where))
                return false;
            if (Unwrap(whereCall.Arguments[0]) is not ConstantExpression)
                return false;
            var lambdaArg = whereCall.Arguments[1];
            while (lambdaArg is UnaryExpression { NodeType: ExpressionType.Quote } q)
                lambdaArg = q.Operand;
            if (lambdaArg is not LambdaExpression lambda)
                return false;
            var body = lambda.Body;
            // Support boolean member access: u => u.IsActive
            if (body is MemberExpression meBoolean && meBoolean.Type == typeof(bool)
                && TableMapping.TryGetMemberAccessPath(meBoolean, out var boolPath))
            {
                info = new WhereInfo(boolPath, true);
                return true;
            }
            if (body is BinaryExpression be && be.NodeType == ExpressionType.Equal && be.Left is MemberExpression me)
            {
                if (!TableMapping.TryGetMemberAccessPath(me, out var propertyPath))
                    return false;

                // Only accept ConstantExpression or simple MemberExpression.
                // Never compile and execute arbitrary expressions (would be an RCE vulnerability).
                // Complex expressions fall back to the safe ExpressionToSqlVisitor.
                if (!TryGetSimpleValue(be.Right, out var value))
                    return false;

                info = new WhereInfo(propertyPath, value);
                return true;
            }
            return false;
        }
        /// <summary>
        /// Delegates to shared <see cref="ExpressionValueExtractor"/> utility.
        /// Eliminates duplicate logic and ensures consistent behavior across the codebase.
        /// </summary>
        private static bool TryGetSimpleValue(Expression expr, out object? value)
            => ExpressionValueExtractor.TryGetConstantValue(expr, out value);

        private static bool IsSimpleTakePattern(Expression expr, out int? takeCount)
        {
            takeCount = null;
            if (expr is MethodCallExpression takeCall && takeCall.Method.Name == nameof(Queryable.Take))
            {
                if (takeCall.Arguments[1] is ConstantExpression ce && Unwrap(takeCall.Arguments[0]) is ConstantExpression)
                {
                    takeCount = (int)ce.Value!;
                    return true;
                }
            }
            return false;
        }

        private static bool IsFilteredOrderedPagePattern(Expression expr, out ComplexQueryInfo info)
        {
            info = default!;
            var predicates = new List<PredicateInfo>(4);
            string? orderProperty = null;
            bool orderDescending = false;
            int? skipCount = null;
            int? takeCount = null;
            var sawPagingOrOrdering = false;
            // The walk goes top-down (outer-most LINQ call first). If we see paging/Take/Skip
            // BEFORE the Where or OrderBy at the outer level, the chain is actually
            // `.Take(n).Where(p)` / `.Take(n).OrderBy(k)` - LINQ semantics demand the Where /
            // OrderBy operate on the windowed result, which requires a subquery wrap that
            // this fast path can't emit. Surrender to the slow translator so its bca0523 /
            // sister pin can throw with the workaround hint instead of silently filtering
            // the full table. Tracked via `sawPaging` - true once Take/Skip is seen.
            bool sawPaging = false;

            while (expr is MethodCallExpression call)
            {
                if (call.Method.Name is "AsNoTracking" or "AsNoTrackingWithIdentityResolution" or "AsSplitQuery" && call.Arguments.Count == 1)
                {
                    expr = call.Arguments[0];
                    continue;
                }

                if (call.Method.DeclaringType != typeof(Queryable))
                    return false;

                switch (call.Method.Name)
                {
                    case nameof(Queryable.Take):
                        if (takeCount.HasValue || call.Arguments[1] is not ConstantExpression takeConst || takeConst.Value is not int take || take < 0)
                            return false;
                        // A Where/OrderBy already collected (visited OUTER to this Take in the
                        // top-down walk) means the chain is `.Take(n).Where(p)` / `.Take(n).OrderBy(k)`:
                        // LINQ pages FIRST then filters/orders, which needs a derived-table wrap the
                        // flat SQL can't express. Surrender to the slow translator instead of emitting
                        // `WHERE p LIMIT n` (wrong rows).
                        if (predicates.Count > 0 || orderProperty != null)
                            return false;
                        takeCount = take;
                        sawPagingOrOrdering = true;
                        sawPaging = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.Skip):
                        if (skipCount.HasValue || call.Arguments[1] is not ConstantExpression skipConst || skipConst.Value is not int skip || skip < 0)
                            return false;
                        // Same rule as Take: a filter/order outer to this Skip is `.Skip(n).Where(p)`,
                        // which pages first then filters. Beyond wrong rows, the flat builder would emit
                        // a bare OFFSET with no LIMIT (invalid SQL on SQLite/MySQL) — the slow path
                        // handles the skip-only shape correctly, so defer to it.
                        if (predicates.Count > 0 || orderProperty != null)
                            return false;
                        skipCount = skip;
                        sawPagingOrOrdering = true;
                        sawPaging = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                        if (orderProperty != null)
                            return false;
                        // OrderBy applied AFTER Take/Skip (i.e. outer-most call) - see comment
                        // above. Reject so the slow translator's bca0523 pin fires.
                        if (sawPaging) return false;
                        if (StripQuotes(call.Arguments[1]) is not LambdaExpression orderLambda ||
                            orderLambda.Body is not MemberExpression orderMember)
                            return false;
                        if (!TableMapping.TryGetMemberAccessPath(orderMember, out orderProperty))
                            return false;
                        orderDescending = call.Method.Name == nameof(Queryable.OrderByDescending);
                        sawPagingOrOrdering = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.Where):
                        if (predicates.Count > 0)
                            return false;
                        // Where applied AFTER Take/Skip - silent-wrongness vector. Reject so
                        // the slow translator's sister pin throws with the workaround hint.
                        if (sawPaging) return false;
                        if (StripQuotes(call.Arguments[1]) is not LambdaExpression whereLambda ||
                            !TryCollectPredicates(whereLambda.Body, predicates))
                            return false;
                        expr = call.Arguments[0];
                        break;

                    default:
                        return false;
                }
            }

            expr = Unwrap(expr);
            if (expr is not ConstantExpression || predicates.Count == 0 || !sawPagingOrOrdering)
                return false;

            info = new ComplexQueryInfo(predicates.ToArray(), orderProperty, orderDescending, skipCount, takeCount);
            return true;
        }

        private static bool TryCollectPredicates(Expression expression, List<PredicateInfo> predicates)
        {
            if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
            {
                return TryCollectPredicates(and.Left, predicates) &&
                       TryCollectPredicates(and.Right, predicates);
            }

            if (expression is MemberExpression boolMember
                && boolMember.Type == typeof(bool)
                && TableMapping.TryGetMemberAccessPath(boolMember, out var boolPath))
            {
                predicates.Add(new PredicateInfo(boolPath, ExpressionType.Equal, true));
                return true;
            }

            if (expression is UnaryExpression { NodeType: ExpressionType.Not, Operand: MemberExpression negatedMember }
                && negatedMember.Type == typeof(bool)
                && TableMapping.TryGetMemberAccessPath(negatedMember, out var negatedPath))
            {
                predicates.Add(new PredicateInfo(negatedPath, ExpressionType.Equal, false));
                return true;
            }

            if (expression is not BinaryExpression binary)
                return false;

            if (binary.NodeType is not (ExpressionType.Equal or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                or ExpressionType.LessThan or ExpressionType.LessThanOrEqual))
                return false;

            if (binary.Left is not MemberExpression member)
                return false;

            if (!TableMapping.TryGetMemberAccessPath(member, out var propertyPath))
                return false;

            if (!TryGetSimpleValue(binary.Right, out var value))
                return false;
            if ((value == null || value == DBNull.Value) && binary.NodeType != ExpressionType.Equal)
                return false;

            // DTO col [op] DateTime literal needs UTC-epoch lowering (the literal
            // binds without offset and would mismatch rows storing the same UTC
            // instant in a different offset). The slow translator (ETSV
            // TryEmitDateTimeOffsetEqualsLiteral) handles this - bail out of the
            // fast path so it takes that branch.
            var memberType = Nullable.GetUnderlyingType(member.Type) ?? member.Type;
            if (memberType == typeof(DateTimeOffset) && value is DateTime)
                return false;

            predicates.Add(new PredicateInfo(propertyPath, binary.NodeType, value));
            return true;
        }

        private static bool HasFilteredOrderedPageColumns(TableMapping map, ComplexQueryInfo info)
        {
            foreach (var predicate in info.Predicates)
            {
                if (!map.ColumnsByName.ContainsKey(predicate.Property))
                    return false;
            }

            return info.OrderProperty is null || map.ColumnsByName.ContainsKey(info.OrderProperty);
        }
    }
}
