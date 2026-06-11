using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;

#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        private bool TryTranslateDateOnlyAdd(MethodCallExpression node)
        {
            if (node.Object != null
                && (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) == typeof(DateOnly)
                && node.Arguments.Count == 1
                && (node.Method.Name == nameof(DateOnly.AddDays)
                    || node.Method.Name == nameof(DateOnly.AddMonths)
                    || node.Method.Name == nameof(DateOnly.AddYears)))
            {
                var dateSql = GetSql(node.Object);
                var nSql = GetSql(node.Arguments[0]);
                var arithSql = node.Method.Name switch
                {
                    nameof(DateOnly.AddDays) => _provider.AddDaysToDateOnlySql(dateSql, nSql),
                    nameof(DateOnly.AddMonths) => _provider.AddMonthsToDateOnlySql(dateSql, nSql),
                    nameof(DateOnly.AddYears) => _provider.AddYearsToDateOnlySql(dateSql, nSql),
                    _ => null
                };
                if (arithSql != null)
                {
                    _sql.Append(arithSql);
                    return true;
                }
                throw new NormUnsupportedFeatureException(
                    $"{_provider.GetType().Name} does not implement {node.Method.Name}ToDateOnlySql; " +
                    $"DateOnly.{node.Method.Name} in WHERE requires this provider hook.");
            }

            return false;
        }

        private bool TryTranslateTimeOnlyAdd(MethodCallExpression node)
        {
            if (node.Object != null
                && (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) == typeof(TimeOnly)
                && node.Arguments.Count == 1
                && ((node.Method.Name == nameof(TimeOnly.Add)
                     && (Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type) == typeof(TimeSpan))
                    || ((node.Method.Name == nameof(TimeOnly.AddHours) || node.Method.Name == nameof(TimeOnly.AddMinutes))
                        && (Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type) == typeof(double))))
            {
                var timeSql = GetSql(node.Object);
                if (node.Method.Name != nameof(TimeOnly.Add)
                    && TryGetConstantValue(node.Arguments[0], out var scalarVal)
                    && scalarVal is double scalar)
                {
                    if (node.Arguments[0] is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                    double secondsPerUnit = node.Method.Name == nameof(TimeOnly.AddHours) ? 3600.0 : 60.0;
                    var secondsLiteral = ((long)(scalar * secondsPerUnit)).ToString(System.Globalization.CultureInfo.InvariantCulture);
                    var arithSql0 = _provider.AddSecondsToTimeOnlySql(timeSql, secondsLiteral);
                    if (arithSql0 != null) { _sql.Append(arithSql0); return true; }
                    throw new NormUnsupportedFeatureException(
                        $"{_provider.GetType().Name} does not implement AddSecondsToTimeOnlySql; " +
                        $"TimeOnly.{node.Method.Name} in WHERE requires this provider hook.");
                }
                if (TryGetConstantValue(node.Arguments[0], out var spanVal) && spanVal is TimeSpan span)
                {
                    if (node.Arguments[0] is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                    var secondsLiteral = ((long)span.TotalSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture);
                    var arithSql = _provider.AddSecondsToTimeOnlySql(timeSql, secondsLiteral);
                    if (arithSql != null) { _sql.Append(arithSql); return true; }
                    throw new NormUnsupportedFeatureException(
                        $"{_provider.GetType().Name} does not implement AddSecondsToTimeOnlySql; " +
                        "TimeOnly.Add (constant TimeSpan) in WHERE requires this provider hook.");
                }
                else
                {
                    var spanSql = GetSql(node.Arguments[0]);
                    var arithSql = _provider.AddTimeSpanColumnToTimeOnlySql(timeSql, spanSql, subtract: false);
                    if (arithSql != null) { _sql.Append(arithSql); return true; }
                    throw new NormUnsupportedFeatureException(
                        $"{_provider.GetType().Name} does not implement AddTimeSpanColumnToTimeOnlySql; " +
                        "TimeOnly.Add (TimeSpan column) in WHERE requires this provider hook.");
                }
            }

            return false;
        }

        private bool TryTranslateRegexMethod(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(System.Text.RegularExpressions.Regex)
                && node.Method.Name == nameof(System.Text.RegularExpressions.Regex.IsMatch)
                && (node.Arguments.Count == 2 || node.Arguments.Count == 3)
                && node.Object == null)
            {
                bool ignoreCase = node.Arguments.Count == 3
                    && TryGetConstantValue(node.Arguments[2], out var optsRaw)
                    && optsRaw is System.Text.RegularExpressions.RegexOptions opts
                    && (opts & System.Text.RegularExpressions.RegexOptions.IgnoreCase) != 0;
                var inputSql = GetSql(node.Arguments[0]);
                var patternSql = GetRegexPatternSql(node.Arguments[1]);
                _sql.Append(ignoreCase
                    ? _provider.GetRegexMatchIgnoreCaseSql(inputSql, patternSql)
                    : _provider.GetRegexMatchSql(inputSql, patternSql));
                return true;
            }

            if (node.Method.DeclaringType == typeof(System.Text.RegularExpressions.Regex)
                && node.Method.Name == nameof(System.Text.RegularExpressions.Regex.Replace)
                && (node.Arguments.Count == 3 || node.Arguments.Count == 4)
                && node.Object == null)
            {
                bool ignoreCase = node.Arguments.Count == 4
                    && TryGetConstantValue(node.Arguments[3], out var optsR)
                    && optsR is System.Text.RegularExpressions.RegexOptions optsR2
                    && (optsR2 & System.Text.RegularExpressions.RegexOptions.IgnoreCase) != 0;
                var inputSql = GetSql(node.Arguments[0]);
                var patternSql = GetRegexPatternSql(node.Arguments[1]);
                var replSql = GetRegexReplacementSql(node.Arguments[2]);
                _sql.Append(ignoreCase
                    ? _provider.GetRegexReplaceIgnoreCaseSql(inputSql, patternSql, replSql)
                    : _provider.GetRegexReplaceSql(inputSql, patternSql, replSql));
                return true;
            }

            return false;
        }

        private bool TryTranslateStringJoin(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(string)
                && node.Method.Name == nameof(string.Join)
                && node.Arguments.Count == 2
                && node.Arguments[1] is NewArrayExpression joinArr
                && TryGetConstantValue(node.Arguments[0], out var joinSepVal)
                && joinSepVal is string joinSep)
            {
                var sepLit = $"'{joinSep.Replace("'", "''")}'";
                if (joinArr.Expressions.Count == 0)
                {
                    _sql.Append("''");
                    return true;
                }
                var parts = new List<string>(joinArr.Expressions.Count * 2 - 1);
                for (int i = 0; i < joinArr.Expressions.Count; i++)
                {
                    if (i > 0) parts.Add(sepLit);
                    parts.Add(GetSql(joinArr.Expressions[i]));
                }
                var joined = parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next));
                _sql.Append(joined);
                return true;
            }

            return false;
        }

        private bool TryTranslateNullableGetValueOrDefault(MethodCallExpression node)
        {
            if (node.Object != null
                && node.Method.Name == nameof(Nullable<int>.GetValueOrDefault)
                && node.Object.Type.IsGenericType
                && node.Object.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var inner = GetSql(node.Object);
                if (node.Arguments.Count == 0)
                {
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type)!;
                    var fallback = underlying == typeof(string) ? "''"
                        : underlying.IsValueType ? "0"
                        : "NULL";
                    _sql.Append("COALESCE(").Append(inner).Append(", ").Append(fallback).Append(')');
                }
                else
                {
                    var fbSql = GetSql(node.Arguments[0]);
                    _sql.Append("COALESCE(").Append(inner).Append(", ").Append(fbSql).Append(')');
                }
                return true;
            }

            return false;
        }

        private bool TryTranslateToStringCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 0
                && node.Object != null
                && node.Object.Type != typeof(string)
                && !(Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type).IsEnum)
            {
                var inner = GetSql(node.Object);
                _sql.Append(_provider.GetToStringSql(inner));
                return true;
            }

            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 0
                && node.Object != null
                && (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type).IsEnum)
            {
                EmitEnumToStringCase(GetSql(node.Object), Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type);
                return true;
            }

            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 0
                && node.Object != null
                && (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) == typeof(bool))
            {
                var inner = GetSql(node.Object);
                _sql.Append("(CASE WHEN ").Append(inner).Append(" = 1 THEN 'True' ELSE 'False' END)");
                return true;
            }

            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 1
                && node.Object != null
                && node.Object.Type != typeof(string)
                && node.Arguments[0].Type == typeof(string))
            {
                if (TryGetConstantValue(node.Arguments[0], out var rawFmt)
                    && rawFmt is string fmt
                    && fmt.Length >= 2
                    && (fmt[0] == 'F' || fmt[0] == 'f')
                    && int.TryParse(fmt.AsSpan(1), out var digits)
                    && digits >= 0 && digits <= 17)
                {
                    if (node.Arguments[0] is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                    var inner = GetSql(node.Object);
                    _sql.Append(_provider.FormatFixedDecimalSql(inner, digits));
                    return true;
                }

                if (TryGetConstantValue(node.Arguments[0], out var rawDateFmt)
                    && rawDateFmt is string dateFmt)
                {
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type;
                    if (underlying == typeof(DateTime)
                        || underlying == typeof(DateTimeOffset)
                        || underlying == typeof(DateOnly)
                        || underlying == typeof(TimeOnly))
                    {
                        if (node.Arguments[0] is MemberExpression)
                        {
                            var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                            _params[placeholder] = DBNull.Value;
                            _compiledParams.Add(placeholder);
                        }
                        var inner = GetSql(node.Object);
                        var formattedSql = _provider.FormatDateUsingDotNetPattern(inner, dateFmt);
                        if (formattedSql != null)
                        {
                            _sql.Append(formattedSql);
                            return true;
                        }
                        throw new NormUnsupportedFeatureException(
                            $"DateTime ToString(\"{dateFmt}\") in Where supports tokens yyyy yy MM dd HH mm ss " +
                            "with literal characters in between. Locale-aware tokens (MMM/MMMM/dddd/ddd) and " +
                            "fractional/offset tokens are unavailable; materialize first and filter after .ToList().");
                    }
                }
                throw new NormUnsupportedFeatureException(
                    "ToString(formatString) in Where supports numeric \"F<N>\"/\"f<N>\" and DateTime tokens " +
                    "yyyy/yy/MM/dd/HH/mm/ss. Materialize first and filter after .ToList() for other shapes.");
            }

            return false;
        }

        private bool TryTranslateCharMethod(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(char)
                && node.Object == null
                && node.Arguments.Count == 1
                && (node.Method.Name == nameof(char.IsDigit)
                    || node.Method.Name == nameof(char.IsLetter)
                    || node.Method.Name == nameof(char.IsLetterOrDigit)
                    || node.Method.Name == nameof(char.IsWhiteSpace)
                    || node.Method.Name == nameof(char.IsUpper)
                    || node.Method.Name == nameof(char.IsLower)
                    || node.Method.Name == nameof(char.IsPunctuation)
                    || node.Method.Name == nameof(char.IsSymbol)
                    || node.Method.Name == nameof(char.IsControl)
                    || node.Method.Name == nameof(char.GetNumericValue)))
            {
                var charSql = GetSql(node.Arguments[0]);
                switch (node.Method.Name)
                {
                    case nameof(char.IsDigit):
                        _sql.Append('(').Append(charSql).Append(" BETWEEN '0' AND '9')");
                        return true;
                    case nameof(char.IsLetter):
                        _sql.Append("((").Append(charSql).Append(" BETWEEN 'A' AND 'Z') OR (")
                            .Append(charSql).Append(" BETWEEN 'a' AND 'z'))");
                        return true;
                    case nameof(char.IsLetterOrDigit):
                        _sql.Append('(')
                            .Append('(').Append(charSql).Append(" BETWEEN 'A' AND 'Z') OR (")
                            .Append(charSql).Append(" BETWEEN 'a' AND 'z') OR (")
                            .Append(charSql).Append(" BETWEEN '0' AND '9'))");
                        return true;
                    case nameof(char.IsUpper):
                        _sql.Append('(').Append(charSql).Append(" BETWEEN 'A' AND 'Z')");
                        return true;
                    case nameof(char.IsLower):
                        _sql.Append('(').Append(charSql).Append(" BETWEEN 'a' AND 'z')");
                        return true;
                    case nameof(char.IsPunctuation):
                    {
                        var code = _provider.GetCharCodeSql(charSql);
                        _sql.Append("((").Append(code).Append(" BETWEEN 33 AND 35) OR ")
                            .Append('(').Append(code).Append(" BETWEEN 37 AND 42) OR ")
                            .Append('(').Append(code).Append(" BETWEEN 44 AND 47) OR ")
                            .Append('(').Append(code).Append(" BETWEEN 58 AND 59) OR ")
                            .Append('(').Append(code).Append(" BETWEEN 63 AND 64) OR ")
                            .Append('(').Append(code).Append(" BETWEEN 91 AND 93) OR ")
                            .Append(code).Append(" = 95 OR ")
                            .Append(code).Append(" = 123 OR ")
                            .Append(code).Append(" = 125)");
                        return true;
                    }
                    case nameof(char.IsSymbol):
                    {
                        var code = _provider.GetCharCodeSql(charSql);
                        _sql.Append('(').Append(code).Append(" = 36 OR ")
                            .Append(code).Append(" = 43 OR ")
                            .Append('(').Append(code).Append(" BETWEEN 60 AND 62) OR ")
                            .Append(code).Append(" = 94 OR ")
                            .Append(code).Append(" = 96 OR ")
                            .Append(code).Append(" = 124 OR ")
                            .Append(code).Append(" = 126)");
                        return true;
                    }
                    case nameof(char.IsControl):
                    {
                        var code = _provider.GetCharCodeSql(charSql);
                        _sql.Append("((").Append(code).Append(" BETWEEN 0 AND 31) OR ")
                            .Append(code).Append(" = 127)");
                        return true;
                    }
                    case nameof(char.GetNumericValue):
                    {
                        var code = _provider.GetCharCodeSql(charSql);
                        _sql.Append("(CASE WHEN ").Append(code).Append(" BETWEEN 48 AND 57 ")
                            .Append("THEN CAST(").Append(code).Append(" - 48 AS REAL) ELSE -1.0 END)");
                        return true;
                    }
                    case nameof(char.IsWhiteSpace):
                        _sql.Append('(').Append(charSql).Append(" = ' ' OR ")
                            .Append(charSql).Append(" = ").Append(_provider.GetCharFromCodeSql("9")).Append(" OR ")
                            .Append(charSql).Append(" = ").Append(_provider.GetCharFromCodeSql("10")).Append(" OR ")
                            .Append(charSql).Append(" = ").Append(_provider.GetCharFromCodeSql("13")).Append(')');
                        return true;
                }
            }

            if (node.Method.DeclaringType == typeof(char)
                && node.Object == null
                && node.Arguments.Count == 1
                && (node.Method.Name == nameof(char.ToUpper) || node.Method.Name == nameof(char.ToLower)))
            {
                var charSql = GetSql(node.Arguments[0]);
                var fnName = node.Method.Name == nameof(char.ToUpper)
                    ? nameof(string.ToUpper)
                    : nameof(string.ToLower);
                var fn = _provider.TranslateFunction(fnName, typeof(string), charSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return true;
                }
            }

            return false;
        }

        private bool TryTranslateNumericOrGuidParse(MethodCallExpression node)
        {
            if (node.Object == null
                && node.Method.Name == "Parse"
                && node.Arguments.Count is 1 or 2
                && (node.Method.DeclaringType == typeof(int) || node.Method.DeclaringType == typeof(long)
                    || node.Method.DeclaringType == typeof(decimal) || node.Method.DeclaringType == typeof(double))
                && node.Arguments[0].Type == typeof(string)
                && (node.Arguments.Count == 1 || typeof(IFormatProvider).IsAssignableFrom(node.Arguments[1].Type)))
            {
                var inner = GetSql(node.Arguments[0]);
                var dt = node.Method.DeclaringType!;
                string castSql;
                if (dt == typeof(int) || dt == typeof(long))
                    castSql = _provider.GetIntCastSql(inner, asLong: dt == typeof(long));
                else
                    castSql = _provider.GetRealCastSql(inner, asDecimal: dt == typeof(decimal));
                _sql.Append(castSql);
                return true;
            }

            if (node.Object == null
                && node.Method.Name == "Parse"
                && node.Arguments.Count == 1
                && node.Method.DeclaringType == typeof(Guid)
                && node.Arguments[0].Type == typeof(string))
            {
                Visit(node.Arguments[0]);
                return true;
            }

            return false;
        }

        private bool TryTranslateEnumMethod(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(Enum.HasFlag)
                && node.Arguments.Count == 1
                && node.Object != null
                && node.Object.Type.IsEnum)
            {
                var receiverSql = GetSql(node.Object);
                var flagSql = GetSql(node.Arguments[0]);
                _sql.Append('(').Append(receiverSql).Append(" & ").Append(flagSql).Append(") = ").Append(flagSql);
                return true;
            }

            if (node.Method.Name == nameof(Enum.GetName)
                && node.Method.DeclaringType == typeof(Enum)
                && node.Arguments.Count == 2
                && node.Arguments[0] is ConstantExpression getNameTypeConst
                && getNameTypeConst.Value is Type getNameEnumType
                && getNameEnumType.IsEnum)
            {
                var raw = node.Arguments[1];
                if (raw is UnaryExpression u && u.NodeType == ExpressionType.Convert) raw = u.Operand;
                var valueSql = GetSql(raw);
                _sql.Append(BuildEnumToStringCase(_provider, valueSql, getNameEnumType));
                return true;
            }

            if (node.Method.Name == nameof(Enum.GetName)
                && node.Method.DeclaringType == typeof(Enum)
                && node.Method.IsGenericMethod
                && node.Arguments.Count == 1
                && node.Method.GetGenericArguments() is { Length: 1 } genericArgs
                && genericArgs[0].IsEnum)
            {
                var valueSql = GetSql(node.Arguments[0]);
                _sql.Append(BuildEnumToStringCase(_provider, valueSql, genericArgs[0]));
                return true;
            }

            if (node.Method.Name == nameof(Enum.Parse)
                && node.Method.DeclaringType == typeof(Enum)
                && node.Method.IsGenericMethod
                && node.Arguments.Count == 1
                && node.Method.GetGenericArguments() is { Length: 1 } parseGenericArgs
                && parseGenericArgs[0].IsEnum)
            {
                var nameSql = GetSql(node.Arguments[0]);
                _sql.Append(BuildStringToEnumCase(nameSql, parseGenericArgs[0]));
                return true;
            }

            if (node.Method.Name == nameof(Enum.Parse)
                && node.Method.DeclaringType == typeof(Enum)
                && !node.Method.IsGenericMethod
                && node.Arguments.Count >= 2
                && node.Arguments[0] is ConstantExpression parseTypeConst
                && parseTypeConst.Value is Type parseEnumType
                && parseEnumType.IsEnum)
            {
                var nameSql = GetSql(node.Arguments[1]);
                _sql.Append(BuildStringToEnumCase(nameSql, parseEnumType));
                return true;
            }

            if (node.Method.Name == nameof(Enum.TryParse)
                && node.Method.DeclaringType == typeof(Enum)
                && node.Method.IsGenericMethod
                && node.Method.GetGenericArguments() is { Length: 1 } tryParseGenericArgs
                && tryParseGenericArgs[0].IsEnum
                && (node.Arguments.Count == 2 || node.Arguments.Count == 3))
            {
                var nameSql = GetSql(node.Arguments[0]);
                var ignoreCase = false;
                if (node.Arguments.Count == 3)
                {
                    if (!TryGetConstantValue(node.Arguments[1], out var ignoreCaseRaw) || ignoreCaseRaw is not bool ignoreCaseValue)
                        throw new NormUnsupportedFeatureException("Enum.TryParse<T>(value, ignoreCase, out result) requires a constant or captured ignoreCase value.");
                    ignoreCase = ignoreCaseValue;
                }

                var names = Enum.GetNames(tryParseGenericArgs[0]);
                if (names.Length == 0)
                {
                    _sql.Append("0");
                }
                else
                {
                    var compareSql = ignoreCase ? $"LOWER({nameSql})" : _provider.ForceCaseSensitiveStringComparison(nameSql);
                    _sql.Append("(").Append(compareSql).Append(" IN (");
                    for (int i = 0; i < names.Length; i++)
                    {
                        if (i > 0) _sql.Append(", ");
                        var name = ignoreCase ? names[i].ToLowerInvariant() : names[i];
                        _sql.Append('\'').Append(name.Replace("'", "''")).Append('\'');
                    }
                    _sql.Append("))");
                }
                return true;
            }

            if (node.Method.Name == nameof(Enum.IsDefined)
                && node.Method.DeclaringType == typeof(Enum)
                && node.Arguments.Count == 2
                && node.Arguments[0] is ConstantExpression typeConst
                && typeConst.Value is Type enumType
                && enumType.IsEnum)
            {
                var valueSql = GetSql(node.Arguments[1]);
                var defined = Enum.GetValues(enumType);
                var underlyingType = Enum.GetUnderlyingType(enumType);
                _sql.Append('(').Append(valueSql).Append(" IN (");
                bool first = true;
                foreach (var v in defined)
                {
                    if (!first) _sql.Append(", ");
                    var underlying = Convert.ChangeType(v, underlyingType, System.Globalization.CultureInfo.InvariantCulture)!;
                    _sql.Append(Convert.ToString(underlying, System.Globalization.CultureInfo.InvariantCulture)!);
                    first = false;
                }
                _sql.Append("))");
                return true;
            }

            return false;
        }

        private bool TryTranslateJsonValue(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Json) || node.Method.Name != nameof(Json.Value))
                return false;

            var columnSql = GetSql(node.Arguments[0]);
            if (TryGetConstantValue(node.Arguments[1], out var path) && path is string jsonPath)
            {
                if (string.IsNullOrWhiteSpace(jsonPath))
                {
                    throw new NormQueryException("JSON path cannot be null or whitespace.");
                }

                if (jsonPath.IndexOfAny(new[] { '\'', '"', ';', '\\' }) >= 0)
                {
                    throw new NormQueryException(
                        $"JSON path '{jsonPath}' contains invalid characters. " +
                        "JSON paths must not contain single-quote, double-quote, semicolon, or backslash.");
                }

                if (jsonPath.Length > MaxJsonPathLength)
                {
                    throw new NormQueryException(
                        $"JSON path exceeds maximum length of {MaxJsonPathLength} characters (actual: {jsonPath.Length}).");
                }

                var jsonSql = _provider.TranslateJsonPathAccess(columnSql, jsonPath);
                _sql.Append(jsonSql);
                return true;
            }

            throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "JSONPath argument in Json.Value must be a constant string."));
        }

        private bool TryTranslateConvertMethod(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Convert) && node.Arguments.Count == 1)
            {
                var inner = GetSql(node.Arguments[0]);
                var convertSql = node.Method.Name switch
                {
                    nameof(Convert.ToInt32) or nameof(Convert.ToInt16) or nameof(Convert.ToByte) or nameof(Convert.ToSByte) => _provider.GetIntCastSql(inner),
                    nameof(Convert.ToInt64) => _provider.GetIntCastSql(inner, asLong: true),
                    nameof(Convert.ToString) => _provider.GetToStringSql(inner),
                    nameof(Convert.ToDouble) or nameof(Convert.ToSingle) => _provider.GetRealCastSql(inner),
                    nameof(Convert.ToDecimal) => _provider.GetRealCastSql(inner, asDecimal: true),
                    nameof(Convert.ToBoolean) => node.Arguments[0].Type == typeof(string)
                        ? GetStringToBoolSql(inner)
                        : _provider.GetBoolCastSql(inner),
                    _ => null
                };
                if (convertSql != null)
                {
                    _sql.Append(convertSql);
                    return true;
                }
            }

            if (node.Method.DeclaringType == typeof(Convert)
                && node.Method.Name == nameof(Convert.ChangeType)
                && node.Arguments.Count == 2
                && TryGetConstantValue(node.Arguments[1], out var ctTypeRaw)
                && ctTypeRaw is Type ctTargetType)
            {
                var inner = GetSql(node.Arguments[0]);
                var ctSql = ctTargetType switch
                {
                    var t when t == typeof(int) || t == typeof(short) || t == typeof(byte) || t == typeof(sbyte) => _provider.GetIntCastSql(inner),
                    var t when t == typeof(long) => _provider.GetIntCastSql(inner, asLong: true),
                    var t when t == typeof(string) => _provider.GetToStringSql(inner),
                    var t when t == typeof(double) || t == typeof(float) => _provider.GetRealCastSql(inner),
                    var t when t == typeof(decimal) => _provider.GetRealCastSql(inner, asDecimal: true),
                    var t when t == typeof(bool) => node.Arguments[0].Type == typeof(string)
                        ? GetStringToBoolSql(inner)
                        : _provider.GetBoolCastSql(inner),
                    _ => null
                };
                if (ctSql != null)
                {
                    _sql.Append(ctSql);
                    return true;
                }
            }

            return false;
        }

        private bool TryTranslateProviderMethodCall(MethodCallExpression node)
        {
            var args = new List<string>();
            if (node.Object != null)
                args.Add(GetSql(node.Object));
            foreach (var a in node.Arguments)
                args.Add(GetSql(a));

            var argsArr = args.ToArray();
            var overloadSql = _provider.TranslateMethodCall(node, argsArr);
            if (overloadSql != null)
            {
                _sql.Append(overloadSql);
                return true;
            }

            var fnSql = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, argsArr);
            if (fnSql != null)
            {
                _sql.Append(fnSql);
                return true;
            }

            var custom = node.Method.GetCustomAttribute<SqlFunctionAttribute>();
            if (custom != null)
            {
                if (_ctx.IsStrictProviderMobility && node.Method.DeclaringType != typeof(NormFunctions))
                {
                    throw new NormUnsupportedFeatureException(
                        $"Custom SQL function '{node.Method.DeclaringType?.FullName}.{node.Method.Name}' is outside nORM's strict provider mobility contract. " +
                        "Use built-in nORM functions with provider translations, or keep the custom SQL function in ProviderMobilityMode.Compatibility with per-provider evidence.");
                }
                var formatted = string.Format(custom.Format, argsArr);
                _sql.Append(formatted);
                return true;
            }

            return false;
        }

        private Expression? TryTranslateStringMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(string))
            {
                return null;
            }

            // string.Trim / TrimStart / TrimEnd(char[]) -- the generic provider
            // route would expand the params char[] as multiple SQL args
            // crashing SQLite TRIM. Mirror be14020's SCV branch: walk
            // NewArrayInit or fold MemberExpression to extract chars at
            // translation time, then emit TRIM/LTRIM/RTRIM(s, 'chars').
            if ((node.Method.Name == nameof(string.Trim)
                    || node.Method.Name == nameof(string.TrimStart)
                    || node.Method.Name == nameof(string.TrimEnd))
                && node.Object != null
                && node.Arguments.Count == 1
                && node.Arguments[0].Type == typeof(char[]))
            {
                char[]? chars = null;
                if (node.Arguments[0] is NewArrayExpression nae && nae.NodeType == ExpressionType.NewArrayInit)
                {
                    var arr = new char[nae.Expressions.Count];
                    bool allConst = true;
                    for (int i = 0; i < nae.Expressions.Count; i++)
                    {
                        if (nae.Expressions[i] is ConstantExpression ec && ec.Value is char ch)
                            arr[i] = ch;
                        else { allConst = false; break; }
                    }
                    if (allConst) chars = arr;
                }
                else if (TryGetConstantValue(node.Arguments[0], out var arrVal) && arrVal is char[] capturedChars)
                {
                    // Closure-captured array -- reserve a placeholder slot so
                    // the ParameterValueExtractor's value-list stays aligned.
                    // Same shape as 407e03d / eeff6e7 / cf39b61 / 04a0003 /
                    // 7d6d7ac / c6c4710.
                    var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                    _params[placeholder] = DBNull.Value;
                    _compiledParams.Add(placeholder);
                    chars = capturedChars;
                }
                if (chars is { Length: > 0 })
                {
                    var sqlFn = node.Method.Name switch
                    {
                        nameof(string.TrimStart) => "LTRIM",
                        nameof(string.TrimEnd) => "RTRIM",
                        _ => "TRIM",
                    };
                    var recvSql = GetSql(node.Object);
                    var trimSet = new string(chars).Replace("'", "''");
                    _sql.Append(sqlFn).Append('(').Append(recvSql).Append(", '").Append(trimSet).Append("')");
                    return node;
                }
            }
            // String indexer s[i] compiles to String.get_Chars(i) — lower to
            // SUBSTR(col, i+1, 1) via the provider's existing 3-arg Substring shape.
            // The right-hand char literal binds as a single-char parameter, so the
            // SQL comparison ends up `SUBSTR(col, i+1, 1) = 'A'`.
            if (node.Method.Name == "get_Chars"
                && node.Object != null
                && node.Arguments.Count == 1)
            {
                var receiver = GetSql(node.Object);
                var index = GetSql(node.Arguments[0]);
                var indexerSql = _provider.TranslateFunction(nameof(string.Substring), typeof(string), receiver, index, "1");
                if (indexerSql != null)
                {
                    _sql.Append(indexerSql);
                    return node;
                }
            }
            // static string.IsNullOrEmpty(x): provider hook handles the empty-string check
            //   (SQL Server ignores trailing spaces in =, so must use DATALENGTH).
            // static string.IsNullOrWhiteSpace(x): trim then compare to ''.
            if (node.Object == null && node.Arguments.Count == 1 &&
                (node.Method.Name == nameof(string.IsNullOrEmpty) || node.Method.Name == nameof(string.IsNullOrWhiteSpace)))
            {
                var inner = GetSql(node.Arguments[0]);
                if (node.Method.Name == nameof(string.IsNullOrEmpty))
                {
                    _sql.Append(_provider.IsNullOrEmptySql(inner));
                }
                else
                {
                    var trimmed = _provider.TranslateFunction(nameof(string.Trim), typeof(string), inner) ?? inner;
                    _sql.Append('(').Append(inner).Append(" IS NULL OR ").Append(trimmed).Append(" = '')");
                }
                return node;
            }
            // static string.Compare(a, b), string.Compare(a, b, StringComparison),
            // string.Compare(a, b, bool ignoreCase), string.CompareOrdinal(a, b), and instance a.CompareTo(b)
            // -> CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END. The comparison
            // mode must be a literal or closure value so translation is fixed at
            // query-build time rather than changing collation semantics per row.
            if (node.Method.Name == nameof(string.Compare) || node.Method.Name == nameof(string.CompareTo) || node.Method.Name == nameof(string.CompareOrdinal))
            {
                string lhs, rhs;
                bool ignoreCase = false;
                bool forceCaseSensitive = false;
                if (node.Object == null && node.Method.Name == nameof(string.CompareOrdinal) && node.Arguments.Count == 2)
                {
                    lhs = GetSql(node.Arguments[0]);
                    rhs = GetSql(node.Arguments[1]);
                    forceCaseSensitive = true;
                }
                else if (node.Object == null && node.Arguments.Count is 2 or 3)
                {
                    lhs = GetSql(node.Arguments[0]);
                    rhs = GetSql(node.Arguments[1]);
                    if (node.Arguments.Count == 3)
                    {
                        if (!TryResolveStringCompareMode(node.Arguments[2], out ignoreCase, out forceCaseSensitive))
                        {
                            throw new NormUnsupportedFeatureException(
                                "string.Compare comparison mode must be a constant or captured bool/StringComparison value.");
                        }
                        ReserveCompiledParamSlotIfClosure(this, node.Arguments[2]);
                    }
                }
                else if (node.Object != null && node.Arguments.Count == 1)
                {
                    lhs = GetSql(node.Object);
                    rhs = GetSql(node.Arguments[0]);
                }
                else
                {
                    throw new NormUnsupportedFeatureException(
                        $"Overload of {node.Method.Name} with {node.Arguments.Count} arguments is not supported.");
                }
                if (ignoreCase)
                {
                    lhs = $"LOWER({lhs})";
                    rhs = $"LOWER({rhs})";
                }
                else if (forceCaseSensitive)
                {
                    lhs = _provider.ForceCaseSensitiveStringComparison(lhs);
                    rhs = _provider.ForceCaseSensitiveStringComparison(rhs);
                }
                _sql.Append("(CASE WHEN ").Append(lhs).Append(" < ").Append(rhs)
                    .Append(" THEN -1 WHEN ").Append(lhs).Append(" > ").Append(rhs)
                    .Append(" THEN 1 ELSE 0 END)");
                return node;
            }
            // static string.Format("template", args...) where the template uses only
            // simple positional placeholders (`{0}`, `{1}`, ...). Lowers to a provider
            // concat that interleaves literal pieces and argument SQL. Format specifiers
            // (`{0:N2}` / `{0,5}`) get rejected so client-evaluation can take over for
            // those — no provider has a portable equivalent of .NET format strings.
            if (node.Object == null
                && node.Method.Name == nameof(string.Format)
                && node.Arguments.Count >= 2
                && TryGetConstantValue(node.Arguments[0], out var fmtRaw) && fmtRaw is string template)
            {
                var segments = TryParseSimpleFormatSegments(template);
                if (segments != null)
                {
                    // Collect remaining args (one per argument expression). string.Format
                    // accepts `params object[]`, so the second argument may be a
                    // NewArrayExpression wrapping the parts — unwrap if so.
                    var argExprs = new List<Expression>();
                    for (int i = 1; i < node.Arguments.Count; i++)
                    {
                        var a = node.Arguments[i];
                        if (a is NewArrayExpression arr) argExprs.AddRange(arr.Expressions);
                        else argExprs.Add(a);
                    }
                    var parts = new List<string>();
                    bool clientEvalFallback = false;
                    foreach (var seg in segments)
                    {
                        if (seg.IsLiteral)
                        {
                            if (seg.Literal!.Length > 0)
                                parts.Add($"'{seg.Literal.Replace("'", "''")}'");
                        }
                        else
                        {
                            if (seg.ArgIndex >= argExprs.Count)
                                return base.VisitMethodCall(node); // bad template, defer
                            var rawArg = argExprs[seg.ArgIndex];
                            // string.Format takes object args; unwrap a Convert(x, typeof(object))
                            // so the underlying CLR type stays visible for format-spec dispatch.
                            if (rawArg is UnaryExpression u
                                && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked)
                                && u.Type == typeof(object))
                            {
                                rawArg = u.Operand;
                            }
                            var argSql = GetSql(rawArg);
                            var argType = rawArg.Type;
                            string emitted;
                            if (seg.FormatSpec != null)
                            {
                                var formatted = ApplyFormatSpecToArg(argSql, argType, seg.FormatSpec);
                                if (formatted == null)
                                {
                                    clientEvalFallback = true;
                                    break;
                                }
                                emitted = formatted;
                            }
                            else
                            {
                                emitted = argType != typeof(string) ? _provider.GetToStringSql(argSql) : argSql;
                            }
                            if (seg.Alignment != 0)
                            {
                                // Positive alignment = right-align (pad-left to width);
                                // negative = left-align (pad-right to abs width).
                                var width = Math.Abs(seg.Alignment).ToString(System.Globalization.CultureInfo.InvariantCulture);
                                var padded = seg.Alignment > 0
                                    ? _provider.TranslateFunction(nameof(string.PadLeft), typeof(string), emitted, width)
                                    : _provider.TranslateFunction(nameof(string.PadRight), typeof(string), emitted, width);
                                if (padded == null)
                                {
                                    clientEvalFallback = true;
                                    break;
                                }
                                emitted = padded;
                            }
                            parts.Add(emitted);
                        }
                    }
                    if (clientEvalFallback)
                        return base.VisitMethodCall(node);
                    if (parts.Count == 0)
                    {
                        _sql.Append("''");
                        return node;
                    }
                    var concatSql = parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next));
                    _sql.Append(concatSql);
                    return node;
                }
            }
            // static string.Concat(a, b, ...) -> provider-specific concat
            if (node.Object == null && node.Method.Name == nameof(string.Concat) && node.Arguments.Count >= 1)
            {
                var parts = new List<string>();
                foreach (var a in node.Arguments)
                {
                    // string.Concat(params object[]) - skip the array wrapper if present
                    if (a is NewArrayExpression arr)
                    {
                        foreach (var item in arr.Expressions) parts.Add(GetSql(item));
                    }
                    else
                    {
                        parts.Add(GetSql(a));
                    }
                }
                var concatSql = parts.Count == 1
                    ? parts[0]
                    : parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next));
                _sql.Append(concatSql);
                return node;
            }
            if (node.Object != null
                && node.Method.Name == nameof(string.IndexOf)
                && node.Arguments.Count == 2
                && node.Arguments[1].Type == typeof(StringComparison))
            {
                if (!TryResolveStringCompareMode(node.Arguments[1], out var ignoreCase, out var forceCaseSensitive))
                {
                    throw new NormUnsupportedFeatureException(
                        "string.IndexOf comparison mode must be a constant or captured StringComparison value.");
                }
                ReserveCompiledParamSlotIfClosure(this, node.Arguments[1]);
                var haystack = GetSql(node.Object);
                var needle = GetSql(node.Arguments[0]);
                if (ignoreCase)
                {
                    haystack = $"LOWER({haystack})";
                    needle = $"LOWER({needle})";
                }
                else if (forceCaseSensitive)
                {
                    haystack = _provider.ForceCaseSensitiveStringComparison(haystack);
                    needle = _provider.ForceCaseSensitiveStringComparison(needle);
                }
                var indexOfSql = _provider.TranslateFunction(nameof(string.IndexOf), typeof(string), haystack, needle);
                if (indexOfSql != null)
                {
                    _sql.Append(indexOfSql);
                    return node;
                }
                throw new NormUnsupportedFeatureException("string.IndexOf(value, StringComparison) is not supported by this provider.");
            }
            if (node.Object != null
                && node.Method.Name == nameof(string.Replace)
                && node.Arguments.Count == 3
                && node.Arguments[2].Type == typeof(StringComparison))
            {
                if (!TryResolveStringCompareMode(node.Arguments[2], out var ignoreCase, out _))
                {
                    throw new NormUnsupportedFeatureException(
                        "string.Replace comparison mode must be a constant or captured StringComparison value.");
                }
                ReserveCompiledParamSlotIfClosure(this, node.Arguments[2]);
                if (ignoreCase)
                {
                    throw new NormUnsupportedFeatureException(
                        "string.Replace(old, new, StringComparison) with an ignore-case mode is not provider-mobile: " +
                        "native REPLACE primitives do not preserve .NET's case-insensitive replacement semantics on every provider.");
                }
                var source = GetSql(node.Object);
                var oldValue = GetSql(node.Arguments[0]);
                var newValue = GetSql(node.Arguments[1]);
                if (_provider is SqlServerProvider)
                {
                    source = _provider.ForceCaseSensitiveStringComparison(source);
                    oldValue = _provider.ForceCaseSensitiveStringComparison(oldValue);
                }
                var replaceSql = _provider.TranslateFunction(nameof(string.Replace), typeof(string), source, oldValue, newValue);
                if (replaceSql != null)
                {
                    _sql.Append(replaceSql);
                    return node;
                }
                throw new NormUnsupportedFeatureException("string.Replace(old, new, StringComparison) is not supported by this provider.");
            }
            var strArgs = new List<string>();
            if (node.Object != null)
                strArgs.Add(GetSql(node.Object));
            foreach (var a in node.Arguments)
                strArgs.Add(GetSql(a));
            var fn = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, strArgs.ToArray());
            if (fn != null)
            {
                _sql.Append(fn);
                return node;
            }
            throw new NormUnsupportedFeatureException($"String method '{node.Method.Name}' is not supported.");
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

                // `parent.Children.Select(c => c.Value).Sum/Min/Max/Average()` — the Select
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
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap, _recursionDepth, _paramIndex);
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
                        if (node.Arguments.Count >= 2
                            && node.Arguments[0] is ParameterExpression gp
                            && (_parameterMappings.ContainsKey(gp) || _groupingKeys.ContainsKey(gp)))
                        {
                            var selector = StripQuotes(node.Arguments[1]) as LambdaExpression;
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
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap, _recursionDepth, _paramIndex);
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
                    // rows — only `col IS NULL` does. Emit (col IN (...) OR col IS NULL) when needed.
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
                    const int MaxInClauseItems = 1000;
                    if (nonNullItems.Count > MaxInClauseItems)
                    {
                        if (hasNulls) _sql.Append("(");
                        _sql.Append("(");
                        for (int batch = 0; batch < nonNullItems.Count; batch += MaxInClauseItems)
                        {
                            if (batch > 0) _sql.Append(" OR ");
                            var batchItems = nonNullItems.Skip(batch).Take(MaxInClauseItems);
                            Visit(valueExpr);
                            _sql.Append(" IN (");
                            bool first = true;
                            foreach (var item in batchItems)
                            {
                                if (!first) _sql.Append(", ");
                                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                                _sql.AppendParameterizedValue(paramName, item, _paramSink);
                                first = false;
                            }
                            _sql.Append(")");
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
                        Visit(valueExpr);
                        _sql.Append(" IN (");
                        for (int i = 0; i < nonNullItems.Count; i++)
                        {
                            if (i > 0) _sql.Append(", ");
                            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _sql.AppendParameterizedValue(paramName, nonNullItems[i], _paramSink);
                        }
                        _sql.Append(")");
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
                    default:
                        throw new NormUnsupportedFeatureException($"Queryable method '{node.Method.Name}' is not supported.");
                }
            }

            return null;
        }
    }
}
