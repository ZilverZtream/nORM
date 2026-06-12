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
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
        }
        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                if (TryEmitMappedBooleanPredicate(node.Operand, expectedValue: false))
                    return node;

                _sql.Append("(NOT(");
                Visit(node.Operand);
                _sql.Append("))");
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
                    Visit(node.Operand);
                    return node;
                }
            }
            return base.VisitUnary(node);
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
            if (!IsNonDeterministicServerMethod(node.Method) && TryGetConstantValueSafe(node, out var constVal))
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
