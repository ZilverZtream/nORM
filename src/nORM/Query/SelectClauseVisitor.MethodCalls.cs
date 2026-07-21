using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var sb = EnsureBuilder();

            if (TryEmitCorrelatedQueryableAggregate(node, sb)) return node;
            if (TryVisitConvertToIntegral(node, sb)) return node;
            if (TryVisitConvertChangeType(node, sb)
                || TryVisitTimeSpanUnary(node, sb)
                || TryVisitDateTimeTimeSpanArithmetic(node, sb)
                || TryVisitEnumParse(node, sb)
                || TryVisitDateTimeOffsetToOffset(node, sb)
                || TryVisitDateOnlyArithmetic(node, sb))
            {
                return node;
            }

            // string.Trim / TrimStart / TrimEnd with an explicit trim set -- both the
            // params char[] overload and the single-char overload (TrimEnd('x'), added
            // in .NET Core). The generic provider route would emit the method name
            // (TRIMEND) or expand the char[] as multiple SQL args -> SQLite TRIM accepts
            // only (s) or (s, trim_chars_str). Evaluate the trim set at translation time
            // and emit TRIM/LTRIM/RTRIM(s, 'concatenated_chars').
            if ((node.Method.Name == nameof(string.Trim)
                    || node.Method.Name == nameof(string.TrimStart)
                    || node.Method.Name == nameof(string.TrimEnd))
                && node.Method.DeclaringType == typeof(string)
                && node.Object != null
                && node.Arguments.Count == 1
                && (node.Arguments[0].Type == typeof(char[]) || node.Arguments[0].Type == typeof(char)))
            {
                // Extract the trim chars manually. TryGetConstantValue refuses
                // NewArrayInit / MethodCall (security-restricted to prevent
                // RCE), so walk NewArrayInit elements ourselves; for a foldable
                // MemberExpression / single char we re-use TryGetConstantValue.
                char[]? chars = null;
                if (node.Arguments[0].Type == typeof(char))
                {
                    // Single-char overload: TrimEnd('x').
                    if (node.Arguments[0] is ConstantExpression singleConst && singleConst.Value is char singleCh)
                        chars = new[] { singleCh };
                    else if (QueryTranslator.TryGetConstantValue(node.Arguments[0], out var singleVal) && singleVal is char capturedCh)
                        chars = new[] { capturedCh };
                }
                else if (node.Arguments[0] is NewArrayExpression nae && nae.NodeType == ExpressionType.NewArrayInit)
                {
                    var arr = new char[nae.Expressions.Count];
                    bool allConst = true;
                    for (int i = 0; i < nae.Expressions.Count; i++)
                    {
                        if (nae.Expressions[i] is ConstantExpression ec && ec.Value is char ch)
                        {
                            arr[i] = ch;
                        }
                        else
                        {
                            allConst = false;
                            break;
                        }
                    }
                    if (allConst) chars = arr;
                }
                else if (QueryTranslator.TryGetConstantValue(node.Arguments[0], out var arrVal) && arrVal is char[] capturedChars)
                {
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
                    var receiverStart = sb.Length;
                    Visit(node.Object);
                    var receiverSql = sb.ToString(receiverStart, sb.Length - receiverStart);
                    sb.Length = receiverStart;
                    var trimSet = new string(chars).Replace("'", "''");
                    sb.Append(sqlFn).Append('(').Append(receiverSql).Append(", '").Append(trimSet).Append("')");
                    return node;
                }
            }

            // Nullable<T>.GetValueOrDefault() / GetValueOrDefault(fallback) --
            // mirror ExpressionToSqlVisitor (line ~1138). Lower to COALESCE.
            if (node.Object != null
                && node.Method.Name == nameof(Nullable<int>.GetValueOrDefault)
                && node.Object.Type.IsGenericType
                && node.Object.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var inner = TranslateProjectionArg(node.Object);
                if (node.Arguments.Count == 0)
                {
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type)!;
                    var fallback = underlying == typeof(string) ? "''"
                        : underlying.IsValueType ? "0"
                        : "NULL";
                    sb.Append("COALESCE(").Append(inner).Append(", ").Append(fallback).Append(')');
                }
                else
                {
                    var fbSql = TranslateProjectionArg(node.Arguments[0]);
                    sb.Append("COALESCE(").Append(inner).Append(", ").Append(fbSql).Append(')');
                }
                return node;
            }

            // string.Concat(params string[] values) and overloads -- mirror of
            // ExpressionToSqlVisitor (line ~1759). The provider-route can't
            // unwrap the NewArrayInit so the generic fallthrough at the end of
            // this method would emit a literal `CONCAT(...)` which SQLite
            // rejects ("no such function: CONCAT"). Unwrap any NewArrayInit
            // params-array argument here and chain provider concat operators
            // (`||` on SQLite, `CONCAT(a,b)` on SQL Server/MySQL).
            if (node.Object == null
                && node.Method.DeclaringType == typeof(string)
                && node.Method.Name == nameof(string.Concat)
                && node.Arguments.Count >= 1)
            {
                var concatParts = new List<string>();
                foreach (var a in node.Arguments)
                {
                    if (a is NewArrayExpression naeC)
                    {
                        foreach (var elem in naeC.Expressions)
                            concatParts.Add(TranslateProjectionArg(elem));
                    }
                    else
                    {
                        concatParts.Add(TranslateProjectionArg(a));
                    }
                }
                if (concatParts.Count == 0) { sb.Append("''"); return node; }
                if (concatParts.Count == 1) { sb.Append(concatParts[0]); return node; }
                sb.Append(concatParts.Aggregate((acc, next) => _provider.GetNullSafeConcatSql(acc, next)));
                return node;
            }

            // string.Join(separator, params string[] values) -- the C# variadic
            // form compiles to a MethodCall with a NewArrayInit second arg
            // holding the value expressions. The args array passed to the
            // provider's TranslateFunction collapses the NewArrayInit into a
            // single opaque entry, so this handler lives in SCV where we can
            // pull each element directly and call TranslateProjectionArg per
            // element, then interleave a constant separator literal.
            if (node.Method.DeclaringType == typeof(string)
                && node.Method.Name == nameof(string.Join)
                && node.Arguments.Count == 2
                && node.Arguments[1] is NewArrayExpression joinArr
                && QueryTranslator.TryGetConstantValue(node.Arguments[0], out var joinSepVal)
                && joinSepVal is string joinSep)
            {
                var sepLit = $"'{joinSep.Replace("'", "''")}'";
                if (joinArr.Expressions.Count == 0)
                {
                    sb.Append("''");
                    return node;
                }
                // Fold through the provider's concat primitive (SqlServer
                // uses '+', MySQL uses CONCAT(...)); plain `||` interleave
                // would emit invalid SQL on those providers.
                var parts = new List<string>(joinArr.Expressions.Count * 2 - 1);
                for (int i = 0; i < joinArr.Expressions.Count; i++)
                {
                    if (i > 0) parts.Add(sepLit);
                    parts.Add(TranslateProjectionArg(joinArr.Expressions[i]));
                }
                sb.Append(parts.Aggregate((acc, next) => _provider.GetNullSafeConcatSql(acc, next)));
                return node;
            }

            // string.StartsWith / EndsWith / Contains -- the projection-time
            // shape needs LIKE-wildcard escaping the generic provider route
            // can't do (the provider only sees pre-rendered SQL fragments,
            // not the raw pattern value). Mirror ExpressionToSqlVisitor's
            // CreateSafeLikePattern: extract the constant pattern, escape
            // %, _, and the escape char itself, prepend/append % per op,
            // then emit `(col LIKE 'escaped%' ESCAPE 'X')`. Variable patterns
            // (column refs or non-foldable expressions) fall through to the
            // existing provider route which uses plain || concat without
            // escape -- acceptable since column-sourced patterns rarely
            // intentionally embed wildcards.
            if (node.Object != null
                && node.Method.DeclaringType == typeof(string)
                && (node.Arguments.Count == 1
                    || (node.Arguments.Count == 2 && node.Arguments[1].Type == typeof(StringComparison)))
                && (node.Method.Name == nameof(string.StartsWith)
                    || node.Method.Name == nameof(string.EndsWith)
                    || node.Method.Name == nameof(string.Contains))
                && QueryTranslator.TryGetConstantValue(node.Arguments[0], out var rawPattern)
                && (rawPattern is string || rawPattern is char))
            {
                // A char needle behaves as its one-character string on every branch below.
                var patternStr = rawPattern as string ?? ((char)rawPattern!).ToString();
                // 2-arg overload: extract the StringComparison and lower both
                // sides for ignore-case variants -- SQLite LIKE is BINARY by
                // default but LOWER/LIKE-LOWER is the portable equivalent. The
                // ETSV side does the same.
                bool ignoreCase = false;
                if (node.Arguments.Count == 2
                    && QueryTranslator.TryGetConstantValue(node.Arguments[1], out var cmpVal)
                    && cmpVal is StringComparison cmpMode
                    && cmpMode is StringComparison.OrdinalIgnoreCase
                        or StringComparison.CurrentCultureIgnoreCase
                        or StringComparison.InvariantCultureIgnoreCase)
                {
                    ignoreCase = true;
                }
                var receiverSql = TranslateProjectionArg(node.Object);
                sb.Append(EmitStringMatch(receiverSql, patternStr, node.Method.Name, ignoreCase));
                return node;
            }

            // string.Equals with a StringComparison argument -- mirror
            // ExpressionToSqlVisitor's HandleStringEqualsStatic/Instance
            // fast handlers so projection and Where return identical truths.
            // The static overload takes (a, b, comparison); instance is
            // (b, comparison). Wrap both sides in LOWER for ignore-case
            // variants; otherwise emit '='. Without this branch the
            // analyzer accepts Equals but SCV emits raw 'EQUALS(...)'
            // and SQLite throws 'no such function'.
            if (node.Method.Name == nameof(string.Equals)
                && node.Method.DeclaringType == typeof(string))
            {
                Expression? lhs = null, rhs = null, comparison = null;
                if (node.Object == null
                    && node.Arguments.Count == 3
                    && node.Arguments[2].Type == typeof(StringComparison))
                {
                    lhs = node.Arguments[0];
                    rhs = node.Arguments[1];
                    comparison = node.Arguments[2];
                }
                else if (node.Object != null
                    && node.Arguments.Count == 2
                    && node.Arguments[1].Type == typeof(StringComparison))
                {
                    lhs = node.Object;
                    rhs = node.Arguments[0];
                    comparison = node.Arguments[1];
                }
                if (lhs != null && rhs != null && comparison != null)
                {
                    var lhsSql = TranslateProjectionArg(lhs);
                    var rhsSql = TranslateProjectionArg(rhs);
                    bool ignoreCase = QueryTranslator.TryGetConstantValue(comparison, out var cv)
                        && cv is StringComparison sc
                        && (sc is StringComparison.OrdinalIgnoreCase
                                or StringComparison.InvariantCultureIgnoreCase
                                or StringComparison.CurrentCultureIgnoreCase);
                    if (ignoreCase)
                        sb.Append("(LOWER(").Append(lhsSql).Append(") = LOWER(").Append(rhsSql).Append("))");
                    else
                        sb.Append('(').Append(lhsSql).Append(" = ").Append(rhsSql).Append(')');
                    return node;
                }
            }

            // numeric.ToString(formatString) -- the most-common case is a fixed-
            // decimal "F<N>" / "f<N>" format like Score.ToString("F2") for currency
            // / report rendering. SQLite has printf('%.<N>f', col); other providers
            // expose similar primitives. Detect the constant format string at
            // translation time, route to printf when it matches F<N>, otherwise
            // throw with the supported-subset hint so users get a clear error
            // rather than a silent CAST that drops the format.
            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 1
                && node.Object != null
                && node.Object.Type != typeof(string)
                && node.Arguments[0].Type == typeof(string))
            {
                if (QueryTranslator.TryGetConstantValue(node.Arguments[0], out var rawFmt)
                    && rawFmt is string fmt)
                {
                    // Numeric F<N>/f<N> fixed-decimal -> printf('%.<N>f', ...).
                    if (fmt.Length >= 2
                        && (fmt[0] == 'F' || fmt[0] == 'f')
                        && int.TryParse(fmt.AsSpan(1), out var digits)
                        && digits >= 0 && digits <= 17)
                    {
                        var recvSql = TranslateProjectionArg(node.Object);
                        sb.Append(_provider.FormatFixedDecimalSql(recvSql, digits));
                        return node;
                    }

                    // DateTime/DateTimeOffset/DateOnly/TimeOnly format strings:
                    // map the common .NET tokens (yyyy/MM/dd/HH/mm/ss) to strftime
                    // tokens, preserving literal punctuation. Locale-aware tokens
                    // (MMM month name, dddd day name, etc.) are not supported --
                    // those require culture data SQLite doesn't carry.
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type;
                    if (underlying == typeof(DateTime)
                        || underlying == typeof(DateTimeOffset)
                        || underlying == typeof(DateOnly)
                        || underlying == typeof(TimeOnly))
                    {
                        var recvSql = TranslateProjectionArg(node.Object);
                        // Provider hook: SqliteProvider uses strftime; SqlServer
                        // FORMAT('en-US'); Postgres to_char; MySQL DATE_FORMAT.
                        var formattedSql = _provider.FormatDateUsingDotNetPattern(recvSql, fmt);
                        if (formattedSql != null)
                        {
                            sb.Append(formattedSql);
                            return node;
                        }
                        throw new InvalidOperationException(
                            $"DateTime ToString(\"{fmt}\") format is not supported in projection. " +
                            "Supported tokens: yyyy yy MM dd HH mm ss; literal characters are passed through. " +
                            "Locale-aware tokens (MMM/MMMM month names, dddd/ddd day names) are unavailable; " +
                            "project the DateTime and format after materialization.");
                    }
                }
                throw new InvalidOperationException(
                    $"ToString(\"{node.Arguments[0]}\") format is not supported in projection on type '{node.Object.Type.Name}'. " +
                    "Supported subset: numeric \"F<N>\"/\"f<N>\" and DateTime tokens yyyy/yy/MM/dd/HH/mm/ss. " +
                    "Project the value and apply the format after materialization for other shapes.");
            }

            // No-arg ToString() on a non-string receiver -- lower to the provider's
            // CAST AS TEXT for primitives, or CASE-WHEN-per-name for enums. Mirrors
            // ExpressionToSqlVisitor's matching handlers so projection and predicate
            // paths agree on shape.
            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 0
                && node.Object != null
                && node.Object.Type != typeof(string))
            {
                var receiverStart = sb.Length;
                Visit(node.Object);
                var receiverSql = sb.ToString(receiverStart, sb.Length - receiverStart);
                sb.Length = receiverStart;
                var underlying = Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type;
                if (underlying.IsEnum)
                {
                    sb.Append(ExpressionToSqlVisitor.BuildEnumToStringCase(_provider, receiverSql, underlying));
                }
                else if (underlying == typeof(bool))
                {
                    // .NET bool.ToString returns "True" / "False" (capitalized) --
                    // CAST AS TEXT on a 0/1 column returns "0"/"1" instead. Emit
                    // an explicit CASE so the projected text matches .NET.
                    sb.Append("(CASE WHEN ").Append(receiverSql).Append(" = 1 THEN 'True' ELSE 'False' END)");
                }
                else
                {
                    sb.Append(_provider.GetToStringSql(receiverSql));
                }
                return node;
            }

            // Navigation aggregates are correlated subqueries, not outer SELECT aggregates.
            if (TryVisitNavigationAggregate(node, sb))
                return node;

            // Use ToUpperInvariant to avoid locale-sensitive casing (e.g., Turkish-I problem).
            var methodNameUpper = node.Method.Name.ToUpperInvariant();

            // static string.Format("template", args...) -- emit as a provider concat of
            // literal pieces and projected argument columns. Format specifiers (`{0:N2}` /
            // `{0,5}`) fall through to the aggregate path (which throws), matching the
            // WHERE-side behaviour: simple positional placeholders translate, anything
            // richer needs client-eval.
            if (node.Method.DeclaringType == typeof(string)
                && node.Object == null
                && node.Method.Name == nameof(string.Format)
                && node.Arguments.Count >= 2
                && node.Arguments[0] is ConstantExpression { Value: string template })
            {
                var segments = TryParseFormatSegments(template);
                if (segments != null)
                {
                    var argExprs = new List<Expression>();
                    for (int i = 1; i < node.Arguments.Count; i++)
                    {
                        var a = node.Arguments[i];
                        if (a is NewArrayExpression arr) argExprs.AddRange(arr.Expressions);
                        else argExprs.Add(a);
                    }
                    var parts = new List<string>();
                    foreach (var seg in segments)
                    {
                        if (seg.IsLiteral)
                        {
                            if (seg.Literal!.Length > 0)
                                parts.Add($"'{seg.Literal.Replace("'", "''")}'");
                        }
                        else
                        {
                            if (seg.ArgIndex >= argExprs.Count) goto fallthrough;
                            var arg = UnwrapFormatObjectConvert(argExprs[seg.ArgIndex]);
                            var inner = TranslateProjectionArg(arg);
                            if (seg.FormatSpec != null)
                            {
                                inner = ApplyFormatSpecToArg(inner, arg.Type, seg.FormatSpec)
                                    ?? throw new InvalidOperationException(
                                        $"string.Format spec '{{{seg.ArgIndex}:{seg.FormatSpec}}}' is not supported in projection. " +
                                        "Supported subset: numeric F<N>/f<N> and DateTime tokens yyyy/yy/MM/dd/HH/mm/ss.");
                            }
                            else if (arg.Type != typeof(string))
                            {
                                inner = _provider.GetToStringSql(inner);
                            }
                            if (seg.Alignment != 0)
                            {
                                var width = Math.Abs(seg.Alignment).ToString(System.Globalization.CultureInfo.InvariantCulture);
                                var padded = seg.Alignment > 0
                                    ? _provider.TranslateFunction(nameof(string.PadLeft), typeof(string), inner, width)
                                    : _provider.TranslateFunction(nameof(string.PadRight), typeof(string), inner, width);
                                inner = padded
                                    ?? throw new InvalidOperationException(
                                        $"string.Format alignment '{{{seg.ArgIndex},{seg.Alignment}}}' is not supported by provider '{_provider.GetType().Name}'.");
                            }
                            parts.Add(inner);
                        }
                    }
                    if (parts.Count == 0) { sb.Append("''"); return node; }
                    sb.Append(parts.Aggregate((acc, next) => _provider.GetNullSafeConcatSql(acc, next)));
                    return node;
                }
            }
            // TimeOnly.Add / AddHours / AddMinutes in a projection -- mirrors the
            // predicate translator's TryTranslateTimeOnlyAdd. Without this branch the
            // projection falls to client evaluation and throws under the default
            // policy. .NET TimeOnly arithmetic wraps around midnight; the provider
            // hooks implement that wrap.
            if (node.Object != null
                && (Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type) == typeof(TimeOnly)
                && node.Arguments.Count == 1
                && ((node.Method.Name == nameof(TimeOnly.Add)
                     && (Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type) == typeof(TimeSpan))
                    || ((node.Method.Name is nameof(TimeOnly.AddHours) or nameof(TimeOnly.AddMinutes))
                        && (Nullable.GetUnderlyingType(node.Arguments[0].Type) ?? node.Arguments[0].Type) == typeof(double))))
            {
                var timeOnlySql = TranslateProjectionArg(node.Object);
                string? timeArith = null;
                if (node.Method.Name != nameof(TimeOnly.Add)
                    && QueryTranslator.TryGetConstantValue(node.Arguments[0], out var timeScalarVal)
                    && timeScalarVal is double timeScalar)
                {
                    ReserveUnusedCompiledSlotForClosure(node.Arguments[0]);
                    var perUnit = node.Method.Name == nameof(TimeOnly.AddHours) ? 3600.0 : 60.0;
                    timeArith = _provider.AddSecondsToTimeOnlySql(
                        timeOnlySql, ((long)(timeScalar * perUnit)).ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                else if (node.Method.Name == nameof(TimeOnly.Add)
                    && QueryTranslator.TryGetConstantValue(node.Arguments[0], out var timeSpanVal)
                    && timeSpanVal is TimeSpan timeSpan)
                {
                    ReserveUnusedCompiledSlotForClosure(node.Arguments[0]);
                    timeArith = _provider.AddSecondsToTimeOnlySql(
                        timeOnlySql, ((long)timeSpan.TotalSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                else if (node.Method.Name == nameof(TimeOnly.Add))
                {
                    var spanColumnSql = TranslateProjectionArg(node.Arguments[0]);
                    timeArith = _provider.AddTimeSpanColumnToTimeOnlySql(timeOnlySql, spanColumnSql, subtract: false);
                }
                if (timeArith != null) { sb.Append(timeArith); return node; }
                throw new NormUnsupportedFeatureException(
                    $"{_provider.GetType().Name} does not implement the TimeOnly arithmetic hook required for TimeOnly.{node.Method.Name} in a projection.");
            }

            fallthrough:

            // Scalar/named functions on known static surfaces (Math.Abs, Math.Min,
            // DateTime.AddDays, Convert.ToInt32, NormFunctions.ILike, etc.). The
            // Queryable-aggregate fall-through below hard-assumes the 2-arg
            // (source, selector-lambda) shape used by Sum/Min/Max/Count, so without
            // this routing `Math.Abs(p.Score)` emits `ABS(*)` and `Math.Min(p.A, p.B)`
            // throws "Expected a lambda expression as argument 1 of 'Min'".
            // Enumerable/Queryable are excluded so the aggregate path still handles
            // grouping-aggregate calls.
            var declType = node.Method.DeclaringType;
            if (declType != null && declType != typeof(Enumerable) && declType != typeof(Queryable))
            {
                var fnArgs = new string[node.Arguments.Count + (node.Object != null ? 1 : 0)];
                int fnIdx = 0;
                if (node.Object != null) fnArgs[fnIdx++] = TranslateProjectionArg(node.Object);
                foreach (var a in node.Arguments) fnArgs[fnIdx++] = TranslateProjectionArg(a);
                // Overload-aware hook first -- providers may dispatch on the
                // full MethodInfo when the stringified args alone are ambiguous.
                var overloadSql = _provider.TranslateMethodCall(node, fnArgs);
                if (overloadSql != null)
                {
                    sb.Append(overloadSql);
                    return node;
                }
                var fnSql = _provider.TranslateFunction(node.Method.Name, declType, fnArgs);
                if (fnSql != null)
                {
                    sb.Append(fnSql);
                    return node;
                }
            }

            sb.Append(methodNameUpper).Append('(');
            if (node.Arguments.Count > 1)
            {
                // StripQuotes handles LINQ's UnaryExpression{Quote()} wrapping around lambdas.
                if (StripQuotes(node.Arguments[1]) is not LambdaExpression lambda)
                    throw new InvalidOperationException(
                        $"Expected a lambda expression as argument 1 of '{node.Method.Name}', but got '{node.Arguments[1].NodeType}'.");
                if (lambda.Body is MemberExpression me)
                {
                    if (!_mapping.TryGetColumnForMemberAccess(me, out var col))
                        throw new InvalidOperationException(
                            $"Member '{me.Member.Name}' on type '{me.Member.DeclaringType?.Name}' is not mapped to a column in table '{_mapping.TableName}'. " +
                            $"Ensure the property is read/write and not a navigation collection.");
                    sb.Append(col.EscCol);
                }
                else
                {
                    // Computed selector: arithmetic, conditional, etc. Visit the body so the
                    // ConditionalExpression / BinaryExpression overrides turn it into SQL.
                    Visit(lambda.Body);
                }
            }
            else if (!string.Equals(methodNameUpper, CountFunctionName, StringComparison.Ordinal))
            {
                sb.Append(_groupBy.FirstOrDefault() ?? "*");
            }
            else
            {
                sb.Append('*');
            }
            sb.Append(')');
            return node;
        }


        /// <summary>
        /// A closure value baked into cached SQL still occupies a slot in the
        /// compiled-parameter list so the extractor's positional document-order
        /// mapping stays aligned (the expression fingerprint hashes the value, so
        /// distinct captures get distinct plans). Mirrors the predicate
        /// translator's placeholder bookkeeping.
        /// </summary>
        private void ReserveUnusedCompiledSlotForClosure(Expression argument)
        {
            if (argument is not MemberExpression || SharedCompiledParams == null)
                return;
            var placeholder = $"{_provider.ParamPrefix}cp{SharedCompiledParams.Count}_unused";
            if (SharedParams != null) SharedParams[placeholder] = DBNull.Value;
            SharedCompiledParams.Add(placeholder);
        }

    }
}
