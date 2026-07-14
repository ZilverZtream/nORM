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
                var joined = parts.Aggregate((acc, next) => _provider.GetNullSafeConcatSql(acc, next));
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
                    // The OBJECT is visited first in the extractor's document-order
                    // walk, so its parameters precede the format's placeholder.
                    var inner = GetSql(node.Object);
                    if (node.Arguments[0] is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
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
                        // Object first — see the numeric format branch above.
                        var inner = GetSql(node.Object);
                        if (node.Arguments[0] is MemberExpression)
                        {
                            var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                            _params[placeholder] = DBNull.Value;
                            _compiledParams.Add(placeholder);
                        }
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
                // GetValuesAsUnderlyingType returns underlying-typed values directly, which
                // avoids the runtime enum-array instantiation NativeAOT cannot provide.
                var defined = Enum.GetValuesAsUnderlyingType(enumType);
                _sql.Append('(').Append(valueSql).Append(" IN (");
                bool first = true;
                foreach (var underlying in defined)
                {
                    if (!first) _sql.Append(", ");
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
                // .NET Convert.ToIntXX over floating/decimal sources rounds half to
                // even; a bare CAST truncates (SQLite/SQL Server) or rounds half away
                // (MySQL). Integral/string sources keep the plain cast.
                var convSrc = Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type;
                var convSrcIsFloating = convSrc == typeof(double) || convSrc == typeof(float) || convSrc == typeof(decimal);
                var convertSql = node.Method.Name switch
                {
                    nameof(Convert.ToInt32) or nameof(Convert.ToInt16) or nameof(Convert.ToByte) or nameof(Convert.ToSByte)
                        => convSrcIsFloating ? _provider.ConvertFloatingToIntegralSql(inner, asLong: false) : _provider.GetIntCastSql(inner),
                    nameof(Convert.ToInt64)
                        => convSrcIsFloating ? _provider.ConvertFloatingToIntegralSql(inner, asLong: true) : _provider.GetIntCastSql(inner, asLong: true),
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
    }
}
