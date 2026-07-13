using System;
using System.Linq.Expressions;
using System.Text;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        // Convert.ChangeType(value, typeof(T)) carries the target type as a
        // constant Type; route it through provider CAST hooks when the target is
        // known at translation time.
        // Convert.ToIntXX(floating/decimal) in a projection: .NET rounds half to
        // even before narrowing, while raw casts diverge per dialect. Handled here
        // so every provider gets the same emit (the fallthrough would route to
        // per-provider TranslateFunction tables that only SQLite populates).
        private bool TryVisitConvertToIntegral(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Method.DeclaringType != typeof(Convert)
                || node.Arguments.Count != 1
                || node.Method.Name is not (nameof(Convert.ToInt32) or nameof(Convert.ToInt16)
                    or nameof(Convert.ToByte) or nameof(Convert.ToSByte) or nameof(Convert.ToInt64)))
                return false;
            var src = Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type;
            if (src != typeof(double) && src != typeof(float) && src != typeof(decimal))
                return false;

            var innerStart = sb.Length;
            Visit(node.Arguments[0]);
            var innerSql = sb.ToString(innerStart, sb.Length - innerStart);
            sb.Length = innerStart;
            sb.Append(_provider.ConvertFloatingToIntegralSql(innerSql, asLong: node.Method.Name == nameof(Convert.ToInt64)));
            return true;
        }

        private bool TryVisitConvertChangeType(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Method.DeclaringType != typeof(Convert)
                || node.Method.Name != nameof(Convert.ChangeType)
                || node.Arguments.Count != 2
                || node.Arguments[1] is not ConstantExpression typeConst
                || typeConst.Value is not Type targetType)
                return false;

            var innerStart = sb.Length;
            Visit(node.Arguments[0]);
            var innerSql = sb.ToString(innerStart, sb.Length - innerStart);
            sb.Length = innerStart;

            if (targetType == typeof(string))
            {
                sb.Append(_provider.GetToStringSql(innerSql));
                return true;
            }

            if (targetType == typeof(int) || targetType == typeof(short)
                || targetType == typeof(byte) || targetType == typeof(sbyte))
            {
                sb.Append(_provider.GetIntCastSql(innerSql, asLong: false));
                return true;
            }

            if (targetType == typeof(long))
            {
                sb.Append(_provider.GetIntCastSql(innerSql, asLong: true));
                return true;
            }

            if (targetType == typeof(double) || targetType == typeof(float))
            {
                sb.Append(_provider.GetRealCastSql(innerSql, asDecimal: false));
                return true;
            }

            if (targetType == typeof(decimal))
            {
                sb.Append(_provider.GetRealCastSql(innerSql, asDecimal: true));
                return true;
            }

            if (targetType == typeof(bool))
            {
                sb.Append(_provider.GetBoolCastSql(innerSql));
                return true;
            }

            return false;
        }

        // TimeSpan.Negate() and Duration() on a column route through the
        // seconds-as-REAL hook so materialization reconstructs via TimeSpan
        // rather than reading provider-specific text prefixes.
        private bool TryVisitTimeSpanUnary(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Object == null
                || (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) != typeof(TimeSpan)
                || node.Arguments.Count != 0
                || (node.Method.Name != nameof(TimeSpan.Negate) && node.Method.Name != nameof(TimeSpan.Duration)))
                return false;

            var tsObjStart = sb.Length;
            Visit(node.Object);
            var tsObjSql = sb.ToString(tsObjStart, sb.Length - tsObjStart);
            sb.Length = tsObjStart;
            var secondsSql = _provider.GetTimeSpanColumnSecondsSql(tsObjSql);
            if (node.Method.Name == nameof(TimeSpan.Negate))
                sb.Append("(-1.0 * ").Append(secondsSql).Append(')');
            else
                sb.Append("ABS(").Append(secondsSql).Append(')');
            return true;
        }

        // DateTime/DateTimeOffset Add/Subtract(TimeSpan) shares the provider
        // hooks used by binary date +/- span translation, for both constant and
        // column TimeSpan arguments.
        private bool TryVisitDateTimeTimeSpanArithmetic(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Object == null
                || ((Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) != typeof(DateTime)
                    && (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) != typeof(DateTimeOffset))
                || (node.Method.Name != nameof(DateTime.Add) && node.Method.Name != nameof(DateTime.Subtract))
                || node.Arguments.Count != 1
                || (Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type) != typeof(TimeSpan))
                return false;

            var dtSql = TranslateProjectionArg(node.Object);
            var subtract = node.Method.Name == nameof(DateTime.Subtract);
            if (TryGetTimeSpanConstant(node.Arguments[0], out var tsConst))
            {
                var seconds = tsConst.TotalSeconds;
                if (subtract) seconds = -seconds;
                var secondsLiteral = seconds.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                var addSql = _provider.AddSecondsToDateTimeSql(dtSql, secondsLiteral);
                if (addSql != null)
                {
                    sb.Append(addSql);
                    return true;
                }
            }
            else
            {
                var spanSql = TranslateProjectionArg(node.Arguments[0]);
                var addColSql = _provider.AddTimeSpanColumnToDateTimeSql(dtSql, spanSql, subtract);
                if (addColSql != null)
                {
                    sb.Append(addColSql);
                    return true;
                }
            }

            return false;
        }

        // Enum.Parse<T>(stringColumn) and Enum.Parse(typeof(T), stringColumn)
        // lower to a CASE mapping enum names to underlying integer values.
        private bool TryVisitEnumParse(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Method.Name != nameof(Enum.Parse) || node.Method.DeclaringType != typeof(Enum))
                return false;

            if (node.Method.IsGenericMethod
                && node.Arguments.Count == 1
                && node.Method.GetGenericArguments() is { Length: 1 } genericArgs
                && genericArgs[0].IsEnum)
            {
                var nameSql = TranslateProjectionArg(node.Arguments[0]);
                sb.Append(ExpressionToSqlVisitor.BuildStringToEnumCase(nameSql, genericArgs[0]));
                return true;
            }

            if (!node.Method.IsGenericMethod
                && node.Arguments.Count >= 2
                && node.Arguments[0] is ConstantExpression typeConst
                && typeConst.Value is Type enumType
                && enumType.IsEnum)
            {
                var nameSql = TranslateProjectionArg(node.Arguments[1]);
                sb.Append(ExpressionToSqlVisitor.BuildStringToEnumCase(nameSql, enumType));
                return true;
            }

            return false;
        }

        // DateTimeOffset.ToOffset(constTimeSpan) recomputes wall-clock text while
        // preserving the UTC instant through provider-specific offset SQL.
        private bool TryVisitDateTimeOffsetToOffset(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Object == null
                || (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) != typeof(DateTimeOffset)
                || node.Method.Name != nameof(DateTimeOffset.ToOffset)
                || node.Arguments.Count != 1
                || !TryGetTimeSpanConstant(node.Arguments[0], out var toOffsetTs))
                return false;

            var dtoSql = TranslateProjectionArg(node.Object);
            sb.Append(_provider.GetDateTimeOffsetWithOffsetSql(dtoSql, toOffsetTs));
            return true;
        }

        // DateOnly AddDays/AddMonths/AddYears uses provider date arithmetic in
        // projection, matching the predicate visitor's date-only handling.
        private bool TryVisitDateOnlyArithmetic(MethodCallExpression node, StringBuilder sb)
        {
            if (node.Object == null
                || (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) != typeof(DateOnly)
                || node.Arguments.Count != 1
                || (node.Method.Name != nameof(DateOnly.AddDays)
                    && node.Method.Name != nameof(DateOnly.AddMonths)
                    && node.Method.Name != nameof(DateOnly.AddYears)))
                return false;

            var dateSql = TranslateProjectionArg(node.Object);
            var nSql = TranslateProjectionArg(node.Arguments[0]);
            var arithSql = node.Method.Name switch
            {
                nameof(DateOnly.AddDays) => _provider.AddDaysToDateOnlySql(dateSql, nSql),
                nameof(DateOnly.AddMonths) => _provider.AddMonthsToDateOnlySql(dateSql, nSql),
                nameof(DateOnly.AddYears) => _provider.AddYearsToDateOnlySql(dateSql, nSql),
                _ => null
            };
            if (arithSql != null)
            {
                sb.Append(arithSql);
                return true;
            }

            throw new InvalidOperationException(
                $"{_provider.GetType().Name} does not implement {node.Method.Name}ToDateOnlySql; " +
                $"DateOnly.{node.Method.Name} in projection requires this provider hook.");
        }
    }
}
