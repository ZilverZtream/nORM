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
        private Expression? TryTranslateStringMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(string))
            {
                return null;
            }

            // string.Trim / TrimStart / TrimEnd with an explicit trim set -- both the
            // params char[] overload and the single-char overload (TrimEnd('x'), added
            // in .NET Core). The generic provider route would emit the method name
            // (TRIMEND) or expand the array as multiple SQL args, crashing the DB.
            // Mirror be14020's SCV branch: walk NewArrayInit / a single char constant /
            // fold a closure, then emit TRIM/LTRIM/RTRIM(s, 'chars').
            if ((node.Method.Name == nameof(string.Trim)
                    || node.Method.Name == nameof(string.TrimStart)
                    || node.Method.Name == nameof(string.TrimEnd))
                && node.Object != null
                && node.Arguments.Count == 1
                && (node.Arguments[0].Type == typeof(char[]) || node.Arguments[0].Type == typeof(char)))
            {
                char[]? chars = null;
                var charsFromClosure = false;
                if (node.Arguments[0].Type == typeof(char))
                {
                    // Single-char overload: TrimEnd('x').
                    if (node.Arguments[0] is ConstantExpression singleConst && singleConst.Value is char singleCh)
                        chars = new[] { singleCh };
                    else if (TryGetConstantValue(node.Arguments[0], out var singleVal) && singleVal is char capturedCh)
                    {
                        chars = new[] { capturedCh };
                        charsFromClosure = true;
                    }
                }
                else if (node.Arguments[0] is NewArrayExpression nae && nae.NodeType == ExpressionType.NewArrayInit)
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
                if (chars == null
                    && TryGetConstantValue(node.Arguments[0], out var arrVal) && arrVal is char[] capturedChars)
                {
                    chars = capturedChars;
                    charsFromClosure = true;
                }
                if (chars is { Length: > 0 })
                {
                    var sqlFn = node.Method.Name switch
                    {
                        nameof(string.TrimStart) => "LTRIM",
                        nameof(string.TrimEnd) => "RTRIM",
                        _ => "TRIM",
                    };
                    // Object first (extractor document order), THEN the closure
                    // array's placeholder slot — aligned with consumption so a
                    // degenerate empty captured array reserves nothing.
                    var recvSql = GetSql(node.Object);
                    if (charsFromClosure)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                    var trimSet = new string(chars).Replace("'", "''");
                    _sql.Append(sqlFn).Append('(').Append(recvSql).Append(", '").Append(trimSet).Append("')");
                    return node;
                }
            }
            // String indexer s[i] compiles to String.get_Chars(i) - lower to
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
                    // Relational-ordinal wrap, not the equality wrap: PostgreSQL's
                    // equality is already byte-exact but its locale collation orders
                    // 'M'/'m' interleaved, so ordinal ORDERING needs COLLATE "C" there.
                    lhs = _provider.OrdinalRelationalStringOperand(lhs);
                    rhs = _provider.OrdinalRelationalStringOperand(rhs);
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
            // those - no provider has a portable equivalent of .NET format strings.
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
                    // NewArrayExpression wrapping the parts - unwrap if so.
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
                    var concatSql = parts.Aggregate((acc, next) => _provider.GetNullSafeConcatSql(acc, next));
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
                    : parts.Aggregate((acc, next) => _provider.GetNullSafeConcatSql(acc, next));
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
                    // The base IndexOf translation matches ordinally, so LOWER-ing both
                    // operands yields OrdinalIgnoreCase exactly (binary compare of the
                    // lowered forms).
                    haystack = $"LOWER({haystack})";
                    needle = $"LOWER({needle})";
                }
                // Explicit StringComparison.Ordinal needs no extra wrapping: the base
                // IndexOf translation is ordinal by default, and stacking another
                // ForceCaseSensitiveStringComparison collation on an operand the
                // translation already collates is a syntax error on SQL Server
                // (chained COLLATE clauses).
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
                // No provider-specific wrapping here: the base Replace translation
                // already matches ordinally (SQL Server collates the REPLACE input;
                // MySQL REPLACE is natively case-sensitive), and a second collation
                // on the same operand is a syntax error on SQL Server.
                var replaceSql = _provider.TranslateFunction(nameof(string.Replace), typeof(string), source, oldValue, newValue);
                if (replaceSql != null)
                {
                    _sql.Append(replaceSql);
                    return node;
                }
                throw new NormUnsupportedFeatureException("string.Replace(old, new, StringComparison) is not supported by this provider.");
            }
            // string.Remove / string.Insert -- no single SQL primitive, but both compose from
            // Substring + concatenation. Remove(i) = s[..i]; Remove(i,c) = s[..i] + s[i+c..];
            // Insert(i, v) = s[..i] + v + s[i..]. Substring goes through the provider's 0-based
            // shape (it adds +1) and pieces join via the null-safe concat hook.
            if (node.Object != null && node.Method.DeclaringType == typeof(string)
                && (node.Method.Name == nameof(string.Remove) || node.Method.Name == nameof(string.Insert)))
            {
                var recv = GetSql(node.Object);
                if (node.Method.Name == nameof(string.Remove))
                {
                    var startSql = GetSql(node.Arguments[0]);
                    var head = _provider.TranslateFunction(nameof(string.Substring), typeof(string), recv, "0", startSql);
                    if (node.Arguments.Count == 1)
                    {
                        if (head != null) { _sql.Append(head); return node; }
                    }
                    else
                    {
                        var countSql = GetSql(node.Arguments[1]);
                        var tail = _provider.TranslateFunction(nameof(string.Substring), typeof(string), recv, $"(({startSql}) + ({countSql}))");
                        if (head != null && tail != null) { _sql.Append(_provider.GetNullSafeConcatSql(head, tail)); return node; }
                    }
                }
                else // Insert(startIndex, value)
                {
                    var startSql = GetSql(node.Arguments[0]);
                    var valueSql = GetSql(node.Arguments[1]);
                    var head = _provider.TranslateFunction(nameof(string.Substring), typeof(string), recv, "0", startSql);
                    var tail = _provider.TranslateFunction(nameof(string.Substring), typeof(string), recv, startSql);
                    if (head != null && tail != null)
                    {
                        _sql.Append(_provider.GetNullSafeConcatSql(_provider.GetNullSafeConcatSql(head, valueSql), tail));
                        return node;
                    }
                }
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
    }
}
