using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using System.Collections.Frozen;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
        {
            // SelectTranslator rewrites `GroupBy(k).Select(g => proj)` into a 3-arg
            // `GroupBy(k, (k, gs) => proj)`, so the projection's key parameter `k`
            // stands in directly for the group key (not via `g.Key`). When a downstream
            // Where / OrderBy references `k` as a standalone parameter
            // (`.Where(x => x.Category.StartsWith("A"))` → expands to `k.StartsWith(…)`)
            // we must emit the group-by SQL here rather than letting the parameter
            // fall through to the entity-column path (which emits nothing and produces
            // SQL like ` LIKE 'A%'` → SQLite syntax error) or the closure-binding path
            // (which would bind it as `@p0 LIKE 'A%'` and never set the value).
            if (_groupingKeys.TryGetValue(node, out var groupKeySql))
            {
                _sql.Append(groupKeySql);
                return node;
            }
            if (_parameterMappings.ContainsKey(node))
                return base.VisitParameter(node);
            if (_paramMap.TryGetValue(node, out var existing))
            {
                _sql.Append(existing);
                return node;
            }
            // __qv marks a whole-query-parameter slot so the compiled-query pipeline
            // pairs it by name instead of document-order position (projection slots
            // register at Build time, out of document order).
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}__qv";
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
        }
        private static bool IsFloatingType(Type t)
            => t == typeof(double) || t == typeof(float) || t == typeof(decimal);

        private static bool IsIntegralTargetType(Type t)
            => t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)
               || t == typeof(sbyte) || t == typeof(uint) || t == typeof(ulong) || t == typeof(ushort);

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                if (TryEmitMappedBooleanPredicate(node.Operand, expectedValue: false))
                    return node;

                // A C# predicate is a total boolean function, but its translation to SQL is
                // three-valued: a comparison against a NULL column yields UNKNOWN, and wrapping
                // that in a bare NOT(...) keeps it UNKNOWN, so the NULL row is filtered out. C#
                // instead treats `!(nullCol == x)` (and `!(nullCol < x)`) as true, so that row
                // must be INCLUDED. Push the negation down to the comparison leaves (De Morgan)
                // so each leaf is emitted in its already-correct null-safe form rather than being
                // negated whole. This is only reached for boolean negation in a predicate context.
                EmitNegation(node.Operand);
                return node;
            }
            if (node.NodeType is ExpressionType.Negate or ExpressionType.NegateChecked)
            {
                var operandType = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
                if (operandType == typeof(TimeSpan))
                {
                    var operandSql = GetSql(node.Operand);
                    _sql.Append("(-1.0 * ").Append(_provider.GetTimeSpanColumnSecondsSql(operandSql)).Append(')');
                    return node;
                }

                _sql.Append("-(");
                Visit(node.Operand);
                _sql.Append(')');
                return node;
            }
            // Numeric / enum conversions in projections: (int)entity.Status, (long)e.Count, etc.
            // SQL columns are already typed, so just emit the operand. Reference-type Convert
            // (interface casts, base→derived) has no SQL meaning and falls through to default.
            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                var operandType = node.Operand.Type;
                var targetType = node.Type;
                var operandUnderlying = Nullable.GetUnderlyingType(operandType) ?? operandType;
                var targetUnderlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
                bool operandIsPrimitive = operandUnderlying.IsPrimitive || operandUnderlying.IsEnum
                    || operandUnderlying == typeof(decimal) || operandUnderlying == typeof(string);
                bool targetIsPrimitive = targetUnderlying.IsPrimitive || targetUnderlying.IsEnum
                    || targetUnderlying == typeof(decimal) || targetUnderlying == typeof(string);
                if (operandIsPrimitive && targetIsPrimitive)
                {
                    // A NARROWING floating->integral cast has real semantics: C#
                    // truncates toward zero, and the raw column value would reach
                    // the materializer as a double (InvalidCastException on strict
                    // drivers). Anything else (widening, enum lifts) is a no-op in
                    // SQL and passes through.
                    if (IsFloatingType(operandUnderlying) && IsIntegralTargetType(targetUnderlying))
                    {
                        var castOperandSql = GetSql(node.Operand);
                        _sql.Append(_provider.FloatingToIntegralTruncatingSql(
                            castOperandSql, targetUnderlying == typeof(long) || targetUnderlying == typeof(ulong)));
                        return node;
                    }
                    // A WIDENING integral->floating cast ((double)A, (float)B) carries real-arithmetic
                    // semantics: without an explicit CAST, `(double)A / B` runs as INTEGER division on
                    // the provider (SQLite: 3/2 = 1, not 1.5) — a silent wrong result. Emit CAST AS REAL
                    // so the operand and any surrounding arithmetic/comparison are real. Decimal targets
                    // are excluded: they keep their own precise TEXT-based coercion (which already works).
                    if ((targetUnderlying == typeof(double) || targetUnderlying == typeof(float))
                        && DatabaseProvider.IsIntegralArithmeticType(operandUnderlying))
                    {
                        var floatCastSql = GetSql(node.Operand);
                        _sql.Append(_provider.GetRealCastSql(floatCastSql));
                        return node;
                    }
                    // A char->integer cast ((int)s[0]) is the CODE POINT in C#. nORM represents a
                    // char as a single-character string (s[0] -> SUBSTR(col, i+1, 1)), so a plain
                    // pass-through would leave the substr text and the materializer would CAST it
                    // to 0. Emit the provider's char-code function instead.
                    if (operandUnderlying == typeof(char) && IsIntegralTargetType(targetUnderlying))
                    {
                        _sql.Append(_provider.GetCharCodeSql(GetSql(node.Operand)));
                        return node;
                    }
                    Visit(node.Operand);
                    return node;
                }
            }
            return base.VisitUnary(node);
        }

        /// <summary>
        /// Emits SQL for the logical negation of <paramref name="operand"/> using C# (two-valued)
        /// boolean semantics rather than SQL three-valued logic. The negation is pushed down to the
        /// comparison leaves so that NULL rows are handled the way LINQ-to-Objects would evaluate
        /// them: <c>!(nullCol == x)</c> and <c>!(nullCol &lt; x)</c> are TRUE (row included), not
        /// UNKNOWN (row filtered out).
        /// </summary>
        private void EmitNegation(Expression operand)
        {
            operand = StripBoolConvert(operand);
            switch (operand)
            {
                // !!a == a. A bare boolean member still needs the boolean-comparison
                // emitter (`col = 1` on providers without boolean expressions).
                case UnaryExpression { NodeType: ExpressionType.Not } inner
                    when StripBoolConvert(inner.Operand) is MemberExpression innerMember
                        && innerMember.Type == typeof(bool):
                    EmitBoolComparison(innerMember, boolVal: true, ExpressionType.Equal);
                    return;
                case UnaryExpression { NodeType: ExpressionType.Not } inner:
                    Visit(StripBoolConvert(inner.Operand));
                    return;

                // De Morgan: !(a && b) == !a || !b, !(a || b) == !a && !b.
                case BinaryExpression { NodeType: ExpressionType.AndAlso } and:
                    _sql.Append('(');
                    EmitNegation(and.Left);
                    _sql.Append(" OR ");
                    EmitNegation(and.Right);
                    _sql.Append(')');
                    return;
                case BinaryExpression { NodeType: ExpressionType.OrElse } or:
                    _sql.Append('(');
                    EmitNegation(or.Left);
                    _sql.Append(" AND ");
                    EmitNegation(or.Right);
                    _sql.Append(')');
                    return;

                // !(a == b) == a != b and !(a != b) == a == b. Routing back through VisitBinary
                // reuses the existing null-safe expansion (e.g. NotEqual emits
                // `(a IS NULL OR a <> b)`), so the NULL row is included exactly as C# requires.
                case BinaryExpression { NodeType: ExpressionType.Equal } eq:
                    Visit(Expression.NotEqual(eq.Left, eq.Right));
                    return;
                case BinaryExpression { NodeType: ExpressionType.NotEqual } ne:
                    Visit(Expression.Equal(ne.Left, ne.Right));
                    return;

                // Relational operators cannot simply flip (`!(a < b)` is NOT `a >= b` when either
                // side is NULL — C# lifts a NULL relational comparison to false, so its negation is
                // true). Emit NOT(a op b) for the non-null rows and OR in an IS NULL guard for each
                // operand that could be NULL, which rescues those rows to true.
                case BinaryExpression
                {
                    NodeType: ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                        or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                } rel:
                    EmitNegatedRelational(rel);
                    return;

                // Negated Contains over a constant list with NO null (`!ids.Contains(nullableCol)`):
                // SQL `col NOT IN (…)` is UNKNOWN for a NULL column and drops that row, but C#
                // `!list.Contains(null)` is true when the list has no null, so the row must be
                // INCLUDED. Rescue it with OR col IS NULL. (Empty / all-null / has-null lists are
                // already correct under a plain NOT — the positive form handles the null column for
                // those — so only the no-null shape is special-cased here.)
                case MethodCallExpression mc
                    when ClassifyConstantContains(mc, out var containsCol) == ContainsListShape.NoNull:
                    _sql.Append("(NOT(");
                    Visit(mc);
                    _sql.Append(')');
                    if (containsCol != null && CouldBeNull(containsCol))
                    {
                        _sql.Append(" OR (");
                        Visit(containsCol);
                        _sql.Append(" IS NULL)");
                    }
                    _sql.Append(')');
                    return;

                // A negated bare boolean column routes through the boolean-comparison
                // emitter as `col == false`: SQL Server has no boolean expressions, so
                // `NOT([Flag])` over a BIT column is a syntax error, while providers
                // preferring bare booleans still get their `NOT col` form from the hook.
                case MemberExpression member when member.Type == typeof(bool):
                    EmitBoolComparison(member, boolVal: false, ExpressionType.Equal);
                    return;

                // Negated string instance predicate (`!col.Contains/StartsWith/EndsWith(...)`):
                // the inner match is UNKNOWN for a NULL instance and NOT(UNKNOWN) drops the row,
                // but under the two-valued C# semantics this translator implements for negation
                // (a null string matches nothing, so the negation of "no match" is TRUE) the NULL
                // row must be INCLUDED - the same rescue the negated comparison and list-Contains
                // cases above apply. Only rescue instances that can actually be NULL.
                case MethodCallExpression strMatch
                    when strMatch.Object is { } matchInstance
                        && matchInstance.Type == typeof(string)
                        && strMatch.Method.Name is nameof(string.Contains) or nameof(string.StartsWith) or nameof(string.EndsWith)
                        && CouldBeNull(matchInstance):
                    _sql.Append("(NOT(");
                    Visit(strMatch);
                    _sql.Append(") OR (");
                    Visit(matchInstance);
                    _sql.Append(" IS NULL))");
                    return;

                // Method calls (other shapes) and anything else fall back to a straight
                // NOT(...) — correct for non-nullable operands.
                default:
                    _sql.Append("(NOT(");
                    Visit(operand);
                    _sql.Append("))");
                    return;
            }
        }

        private enum ContainsListShape { NotConstantContains, Empty, AllNull, HasNullWithValues, NoNull }

        /// <summary>
        /// Classifies <paramref name="operand"/> when it is a <c>Contains</c>/<c>ContainsKey</c>/
        /// <c>ContainsValue</c> over a compile-time-constant collection, reporting the tested value
        /// expression and whether the collection is empty, all-null, has a null among values, or has
        /// no null at all. Used to decide whether a negated Contains needs an <c>IS NULL</c> rescue.
        /// Mirrors the operand detection in the Contains translator (MethodCallTranslators.Enumerable).
        /// </summary>
        private ContainsListShape ClassifyConstantContains(Expression operand, out Expression? valueExpr)
        {
            valueExpr = null;
            if (operand is not MethodCallExpression node)
                return ContainsListShape.NotConstantContains;

            bool isDictContains = (node.Method.Name == "ContainsKey" || node.Method.Name == "ContainsValue")
                && node.Object != null
                && node.Arguments.Count == 1
                && node.Method.DeclaringType is { } dictDt
                && IsDictionaryLikeReceiver(dictDt);
            if (node.Method.Name != nameof(List<int>.Contains) && !isDictContains)
                return ContainsListShape.NotConstantContains;

            Expression? collectionExpr = null;
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
            if (collectionExpr == null || valueExpr == null)
                return ContainsListShape.NotConstantContains;
            if (!TryGetConstantValue(collectionExpr, out var colVal)
                || colVal is not IEnumerable en || colVal is string)
            {
                valueExpr = null;
                return ContainsListShape.NotConstantContains;
            }

            IEnumerable itemSource = en;
            if (isDictContains && colVal is IDictionary dict)
                itemSource = node.Method.Name == "ContainsValue" ? dict.Values : dict.Keys;

            bool any = false, anyNull = false, anyNonNull = false;
            foreach (var item in itemSource)
            {
                any = true;
                if (item is null) anyNull = true; else anyNonNull = true;
            }
            if (!any) return ContainsListShape.Empty;
            if (anyNull && !anyNonNull) return ContainsListShape.AllNull;
            return anyNull ? ContainsListShape.HasNullWithValues : ContainsListShape.NoNull;
        }

        private void EmitNegatedRelational(BinaryExpression rel)
        {
            _sql.Append("(NOT(");
            Visit(rel);
            _sql.Append(')');
            if (CouldBeNull(rel.Left))
            {
                _sql.Append(" OR (");
                Visit(rel.Left);
                _sql.Append(" IS NULL)");
            }
            if (CouldBeNull(rel.Right))
            {
                _sql.Append(" OR (");
                Visit(rel.Right);
                _sql.Append(" IS NULL)");
            }
            _sql.Append(')');
        }

        /// <summary>Strips boolean-preserving Convert nodes (e.g. <c>bool</c> ↔ <c>bool?</c>) so the
        /// underlying comparison is what gets negated.</summary>
        private static Expression StripBoolConvert(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
                   && (u.Type == typeof(bool) || u.Type == typeof(bool?)))
                e = u.Operand;
            return e;
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            // x ? a : b -> (CASE WHEN x THEN a ELSE b END). Nested conditionals naturally
            // recurse, producing CASE WHEN ... WHEN ... ELSE ... END.
            _sql.Append("(CASE WHEN ");
            Visit(node.Test);
            _sql.Append(" THEN ");
            Visit(node.IfTrue);
            _sql.Append(" ELSE ");
            Visit(node.IfFalse);
            _sql.Append(" END)");
            return node;
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (TryTranslateDateOnlyAdd(node)
                || TryTranslateTimeOnlyAdd(node)
                || TryTranslateRegexMethod(node)
                || TryTranslateStringJoin(node))
            {
                return node;
            }
            // Fast path: common string methods (Contains, StartsWith, EndsWith) are handled
            // directly via pre-built delegates, bypassing the general method translation pipeline.
            if (_fastMethodHandlers.TryGetValue(node.Method, out var handler))
            {
                handler(this, node);
                return node;
            }
            if (TryTranslateNullableGetValueOrDefault(node)
                || TryTranslateToStringCall(node)
                || TryTranslateCharMethod(node)
                || TryTranslateNumericOrGuidParse(node)
                || TryTranslateEnumMethod(node))
            {
                return node;
            }
            if (!IsTranslatableMethod(node.Method))
                throw new NormUnsupportedFeatureException(
                    $"Method '{node.Method.Name}' cannot be translated to SQL by the nORM v1 query translator. " +
                    "See docs/linq-support.md for the supported method matrix.");
            if (!_suppressNullCheck && RequiresNullCheck(node))
            {
                return TranslateWithNullCheck(node);
            }
            // A correlated scalar aggregate rooted at ctx.Query<T>() with no outer-row
            // reference (its predicate compares only closures/constants) is technically
            // constant-evaluable — but folding it EXECUTES a database query during
            // translation, bakes the result as a literal parameter, and caches that:
            // the next execution with a different closure replays the first value.
            // Keep it as a server-side subquery so its closures re-bind per execution.
            if (!IsNonDeterministicServerMethod(node.Method)
                && !QueryTranslator.IsQueryRootedSubqueryTerminal(node)
                && TryGetConstantValueSafe(node, out var constVal))
            {
                return CreateSafeParameter(constVal);
            }
            if (TryTranslateJsonValue(node))
            {
                return node;
            }
            var stringMethodResult = TryTranslateStringMethodCall(node);
            if (stringMethodResult != null)
            {
                return stringMethodResult;
            }
            if (TryTranslateConvertMethod(node))
            {
                return node;
            }
            var enumerableOrQueryableResult = TryTranslateEnumerableOrQueryableMethod(node);
            if (enumerableOrQueryableResult != null)
            {
                return enumerableOrQueryableResult;
            }
            if (TryTranslateProviderMethodCall(node))
            {
                return node;
            }
            throw new NormUnsupportedFeatureException($"Method '{node.Method.Name}' is not supported.");
        }
    }
}
