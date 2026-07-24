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
        private static bool IsSimpleWherePattern(Expression expr, bool storesDecimalAsText, out WhereInfo info, out int? takeCount)
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

                // A decimal equality on a TEXT-storing provider (SQLite) is a lexical string compare here
                // ("24.500" = "24.5" is false though the values are equal); only the full translator's
                // canonical decimal wrapping is correct. Native-DECIMAL providers compare exactly and keep
                // the fast path.
                if (storesDecimalAsText && (Nullable.GetUnderlyingType(me.Type) ?? me.Type) == typeof(decimal))
                    return false;

                // Only accept ConstantExpression or simple MemberExpression.
                // Never compile and execute arbitrary expressions (would be an RCE vulnerability).
                // Complex expressions fall back to the safe ExpressionToSqlVisitor.
                if (!TryGetSimpleValue(be.Right, out var value))
                    return false;

                // DateTimeOffset and TimeSpan equality must lower to instant / numeric comparison in the
                // full translator: on SQLite both are stored as TEXT whose lexical `col = @p` compare misses
                // an equal value in a different representation (offset for DateTimeOffset; fractional-digit
                // format for TimeSpan). Defer both to the full translator (native providers keep parity —
                // the deferred SQL is identical there).
                var eqClrType = Nullable.GetUnderlyingType(me.Type) ?? me.Type;
                if (eqClrType == typeof(DateTimeOffset) || eqClrType == typeof(TimeSpan) || eqClrType == typeof(TimeOnly))
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

        private static bool IsFilteredOrderedPagePattern(Expression expr, bool storesDecimalAsText, out ComplexQueryInfo info)
        {
            info = default!;
            var predicates = new List<PredicateInfo>(4);
            string? orderProperty = null;
            bool orderDescending = false;
            int? skipCount = null;
            int? takeCount = null;
            var sawPagingOrOrdering = false;
            // The walk goes top-down (outer-most LINQ call first), i.e. it visits operators in
            // REVERSE fluent order. A query flattens to ONE
            // `SELECT .. WHERE p ORDER BY k LIMIT t OFFSET s` statement iff its fluent order applies
            // paging LAST: filter and order (which commute) first, then Skip, then Take. In the
            // outer-first walk that is the NON-DECREASING rank order Take(0) ≤ Skip(1) ≤
            // {OrderBy, Where}(2). A rank that DECREASES means a filter/order — or an earlier page —
            // was applied to an already-paged result (e.g. `.Where().Take().Skip()`, `.Take().Where()`,
            // `.Take().OrderBy()`, `.Where().Skip().OrderBy()`): LINQ evaluates that over the window,
            // which needs a derived-table wrap the flat builder cannot emit. Surrender those to the
            // slow translator (it pages correctly / its pin fails loud with the workaround hint)
            // rather than emitting flat SQL that would silently return the wrong rows. `Where` and
            // `OrderBy` share rank 2 because filter and sort commute, so `.OrderBy().Where()` stays
            // fast too; duplicate operators are rejected by the per-case guards below.
            var lastRank = -1;

            while (expr is MethodCallExpression call)
            {
                if (call.Method.Name is "AsNoTracking" or "AsNoTrackingWithIdentityResolution" or "AsSplitQuery" && call.Arguments.Count == 1)
                {
                    expr = call.Arguments[0];
                    continue;
                }

                if (call.Method.DeclaringType != typeof(Queryable))
                    return false;

                var rank = call.Method.Name switch
                {
                    nameof(Queryable.Take) => 0,
                    nameof(Queryable.Skip) => 1,
                    nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending) => 2,
                    nameof(Queryable.Where) => 2,
                    _ => -1
                };
                if (rank < 0 || rank < lastRank)
                    return false;
                lastRank = rank;

                switch (call.Method.Name)
                {
                    case nameof(Queryable.Take):
                        if (takeCount.HasValue || call.Arguments[1] is not ConstantExpression takeConst || takeConst.Value is not int take || take < 0)
                            return false;
                        takeCount = take;
                        sawPagingOrOrdering = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.Skip):
                        if (skipCount.HasValue || call.Arguments[1] is not ConstantExpression skipConst || skipConst.Value is not int skip || skip < 0)
                            return false;
                        skipCount = skip;
                        sawPagingOrOrdering = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                        if (orderProperty != null)
                            return false;
                        if (StripQuotes(call.Arguments[1]) is not LambdaExpression orderLambda ||
                            orderLambda.Body is not MemberExpression orderMember)
                            return false;
                        // Ordering by a decimal column must be NUMERIC. This fast path emits a raw
                        // `ORDER BY col`, which on a TEXT-storing provider (SQLite) sorts LEXICALLY
                        // ("24.50","429.00","79.99") — silently wrong. Defer decimal order keys to the
                        // full translator there, which emits the canonical numeric ordering. Native-DECIMAL
                        // providers order exactly, so they keep the fast path.
                        if (storesDecimalAsText
                            && (Nullable.GetUnderlyingType(orderMember.Type) ?? orderMember.Type) == typeof(decimal))
                            return false;
                        // A TimeSpan order key sorts 'c' TEXT lexically on SQLite, mis-ordering multi-day
                        // durations. Defer to the full translator's NormalizeTimeSpanForCompare ordering.
                        if ((Nullable.GetUnderlyingType(orderMember.Type) ?? orderMember.Type) == typeof(TimeSpan))
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
                        if (StripQuotes(call.Arguments[1]) is not LambdaExpression whereLambda ||
                            !TryCollectPredicates(whereLambda.Body, predicates, storesDecimalAsText))
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

            // Skip WITHOUT Take would emit a bare `OFFSET n` (no LIMIT), which is invalid SQL on
            // SQLite/MySQL. The slow translator emits each provider's correct no-limit form, so
            // defer skip-only paging to it.
            if (skipCount.HasValue && !takeCount.HasValue)
                return false;

            info = new ComplexQueryInfo(predicates.ToArray(), orderProperty, orderDescending, skipCount, takeCount);
            return true;
        }

        private static bool TryCollectPredicates(Expression expression, List<PredicateInfo> predicates, bool storesDecimalAsText)
        {
            if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
            {
                return TryCollectPredicates(and.Left, predicates, storesDecimalAsText) &&
                       TryCollectPredicates(and.Right, predicates, storesDecimalAsText);
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

            // A DateTimeOffset EQUALITY must lower to UTC-instant comparison (ETSV), not a raw text `=`:
            // on SQLite's offset-suffixed storage that lexical compare misses the same instant stored in a
            // different offset — whether the literal is an offset-less DateTime or a DateTimeOffset in
            // another offset. Also defer a DTO compared to an offset-less DateTime literal under ANY
            // operator (a range needs the literal's offset resolved). DTO ranges against a DateTimeOffset
            // literal stay on the fast path — BuildFilteredOrderedPageSql wraps the column with
            // NormalizeDateTimeForCompare for order comparisons.
            var memberType = Nullable.GetUnderlyingType(member.Type) ?? member.Type;
            if (memberType == typeof(DateTimeOffset)
                && (binary.NodeType == ExpressionType.Equal || value is DateTime))
                return false;

            // SQLite stores TimeSpan as 'c' TEXT and lex-compares, which mis-orders multi-day durations
            // ("10.00:00:00" < "9.23:59:59") and misses equality against a differently-formatted-but-equal
            // duration. The full translator wraps both sides with NormalizeTimeSpanForCompare (fractional
            // seconds); this fast path emits a raw `col op @p`. Defer EVERY TimeSpan predicate (range AND
            // equality). Native TIME/INTERVAL providers keep parity (their normalize is identity).
            if (memberType == typeof(TimeSpan))
                return false;

            // TimeOnly EQUALITY needs the canonical-text comparison the full translator emits (a TEXT-stored
            // "12:00:00.0000000" equals "12:00:00"); a raw `col = @p` misses it. Ranges/ordering are already
            // lexically correct for zero-padded TimeOnly text, so only equality defers.
            if (memberType == typeof(TimeOnly) && binary.NodeType == ExpressionType.Equal)
                return false;

            // A decimal comparison on a TEXT-storing provider (SQLite) needs the canonical wrapping the
            // full translator emits: CAST(col AS REAL) for ranges, and canonical decimal TEXT (trailing
            // zeros stripped) for equality. This flat fast path emits a raw `col op @p`, which for
            // TEXT-stored decimals is a LEXICAL string comparison — ranges silently drop rows
            // ("79.99" < "100" is false) and equality misses scale-mismatched rows ("24.500" = "24.5" is
            // false though 24.500m == 24.5m). Defer EVERY decimal predicate (range AND equality) to the
            // full translator there. Native-DECIMAL providers compare exactly, so their raw predicate is
            // already identical to the full translator's — they keep the fast path.
            if (memberType == typeof(decimal) && storesDecimalAsText)
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
