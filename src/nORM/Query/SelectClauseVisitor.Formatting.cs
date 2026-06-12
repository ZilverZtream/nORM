using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        private string TranslateProjectionArg(Expression arg)
        {
            var saved = _sb;
            var tmp = new StringBuilder();
            _sb = tmp;
            try
            {
                Visit(arg);
                return tmp.ToString();
            }
            finally
            {
                _sb = saved;
            }
        }

        private static Expression UnwrapFormatObjectConvert(Expression arg)
        {
            if (arg is UnaryExpression u
                && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked)
                && u.Type == typeof(object))
            {
                return u.Operand;
            }

            return arg;
        }

        private string? ApplyFormatSpecToArg(string argSql, Type argType, string spec)
        {
            var underlying = Nullable.GetUnderlyingType(argType) ?? argType;
            if (spec.Length >= 2
                && (spec[0] == 'F' || spec[0] == 'f')
                && int.TryParse(spec.AsSpan(1), out var digits)
                && digits >= 0
                && digits <= 28)
            {
                return _provider.FormatFixedDecimalSql(argSql, digits);
            }

            if (underlying == typeof(DateTime)
                || underlying == typeof(DateTimeOffset)
                || underlying == typeof(DateOnly)
                || underlying == typeof(TimeOnly))
            {
                return _provider.FormatDateUsingDotNetPattern(argSql, spec);
            }

            return null;
        }

        private readonly struct FormatSegment
        {
            public readonly bool IsLiteral;
            public readonly string? Literal;
            public readonly int ArgIndex;
            public readonly string? FormatSpec;
            public readonly int Alignment;
            public FormatSegment(string l) { IsLiteral = true; Literal = l; ArgIndex = -1; FormatSpec = null; Alignment = 0; }
            public FormatSegment(int i, string? f, int a) { IsLiteral = false; Literal = null; ArgIndex = i; FormatSpec = f; Alignment = a; }
        }

        /// <summary>
        /// Translates a .NET DateTime custom format string into a SQLite
        /// strftime format string. Returns false on any unsupported token
        /// (notably locale-aware MMM/MMMM/dddd/ddd). Single-quote characters
        /// in literal segments are doubled per SQL string-literal rules so the
        /// caller can wrap the result in '...'. Strftime % is escaped as %%.
        /// </summary>
        internal static bool TryConvertDotNetDateFormatToStrftime(string fmt, out string strftime)
        {
            var sb = new StringBuilder(fmt.Length + 4);
            int i = 0;
            while (i < fmt.Length)
            {
                if (i + 4 <= fmt.Length && fmt[i] == 'y' && fmt[i + 1] == 'y' && fmt[i + 2] == 'y' && fmt[i + 3] == 'y')
                { sb.Append("%Y"); i += 4; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'y' && fmt[i + 1] == 'y')
                { sb.Append("%y"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'M' && fmt[i + 1] == 'M')
                { sb.Append("%m"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'd' && fmt[i + 1] == 'd')
                { sb.Append("%d"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'H' && fmt[i + 1] == 'H')
                { sb.Append("%H"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'm' && fmt[i + 1] == 'm')
                { sb.Append("%M"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 's' && fmt[i + 1] == 's')
                { sb.Append("%S"); i += 2; continue; }
                // Reject locale-aware and unsupported single-character tokens
                // before they end up in the strftime literal as themselves.
                char c = fmt[i];
                if (c == 'M' || c == 'd' || c == 'H' || c == 'h' || c == 'm'
                    || c == 's' || c == 'y' || c == 'f' || c == 'F' || c == 'z' || c == 'K' || c == 't')
                {
                    strftime = string.Empty;
                    return false;
                }
                if (c == '\'') sb.Append("''");
                else if (c == '%') sb.Append("%%");
                else sb.Append(c);
                i++;
            }
            strftime = sb.ToString();
            return true;
        }

        private static List<FormatSegment>? TryParseFormatSegments(string template)
        {
            var segments = new List<FormatSegment>();
            var literal = new StringBuilder();
            int i = 0;
            while (i < template.Length)
            {
                char c = template[i];
                if (c == '{')
                {
                    if (i + 1 < template.Length && template[i + 1] == '{') { literal.Append('{'); i += 2; continue; }
                    if (literal.Length > 0) { segments.Add(new FormatSegment(literal.ToString())); literal.Clear(); }
                    int end = template.IndexOf('}', i + 1);
                    if (end < 0) return null;
                    var inner = template.Substring(i + 1, end - i - 1);
                    if (inner.Length == 0) return null;
                    string indexPart = inner;
                    string? alignmentPart = null;
                    string? spec = null;
                    int colon = inner.IndexOf(':');
                    if (colon >= 0)
                    {
                        spec = inner.Substring(colon + 1);
                        if (spec.Length == 0) return null;
                        indexPart = inner.Substring(0, colon);
                    }
                    int comma = indexPart.IndexOf(',');
                    if (comma >= 0)
                    {
                        alignmentPart = indexPart.Substring(comma + 1);
                        indexPart = indexPart.Substring(0, comma);
                        if (alignmentPart.Length == 0) return null;
                    }
                    if (!int.TryParse(indexPart, out var argIdx) || argIdx < 0) return null;
                    int alignment = 0;
                    if (alignmentPart != null && !int.TryParse(alignmentPart, out alignment))
                        return null;
                    segments.Add(new FormatSegment(argIdx, spec, alignment));
                    i = end + 1;
                }
                else if (c == '}')
                {
                    if (i + 1 < template.Length && template[i + 1] == '}') { literal.Append('}'); i += 2; continue; }
                    return null;
                }
                else { literal.Append(c); i++; }
            }
            if (literal.Length > 0) segments.Add(new FormatSegment(literal.ToString()));
            return segments;
        }
    }
}
