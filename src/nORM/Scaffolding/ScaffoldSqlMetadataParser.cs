#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
        public static bool TryConsumeSqlKeyword(string sql, ref int index, string keyword)
        {
            SkipSqlTrivia(sql, ref index);

            if (index + keyword.Length > sql.Length
                || !sql.AsSpan(index, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var end = index + keyword.Length;
            if (end < sql.Length && IsSqlIdentifierChar(sql[end]))
                return false;

            index = end;
            return true;
        }

        public static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
        {
            char? quote = null;
            string? dollarQuote = null;
            for (var i = startIndex; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (TryAdvancePostgresDollarQuote(sql, ref i, ref dollarQuote))
                    continue;
                if (TryAdvanceSqlComment(sql, ref i))
                    continue;

                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (i + keyword.Length <= sql.Length
                    && sql.AsSpan(i, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase)
                    && (i == 0 || !IsSqlIdentifierChar(sql[i - 1]))
                    && (i + keyword.Length == sql.Length || !IsSqlIdentifierChar(sql[i + keyword.Length])))
                {
                    return i;
                }
            }

            return -1;
        }

        public static bool HasBalancedOuterParentheses(string value)
        {
            var depth = 0;
            char? quote = null;
            string? dollarQuote = null;
            for (var i = 0; i < value.Length; i++)
            {
                var ch = value[i];
                if (TryAdvancePostgresDollarQuote(value, ref i, ref dollarQuote))
                    continue;
                if (TryAdvanceSqlComment(value, ref i))
                    continue;

                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < value.Length && value[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                    depth++;
                else if (ch == ')')
                    depth--;

                if (depth == 0 && i < value.Length - 1)
                    return false;
                if (depth < 0)
                    return false;
            }

            return depth == 0 && quote is null && dollarQuote is null;
        }

        public static bool TryAdvancePostgresDollarQuote(string sql, ref int index, ref string? dollarQuote)
        {
            if (dollarQuote is not null)
            {
                if (index + dollarQuote.Length <= sql.Length
                    && sql.AsSpan(index, dollarQuote.Length).Equals(dollarQuote.AsSpan(), StringComparison.Ordinal))
                {
                    index += dollarQuote.Length - 1;
                    dollarQuote = null;
                }

                return true;
            }

            if (!TryReadPostgresDollarQuoteTag(sql, index, out var tag))
                return false;

            dollarQuote = tag;
            index += tag.Length - 1;
            return true;
        }

        public static bool TryReadPostgresDollarQuoteTag(string sql, int index, out string tag)
        {
            tag = string.Empty;
            if (index >= sql.Length || sql[index] != '$')
                return false;

            var end = sql.IndexOf('$', index + 1);
            if (end < 0)
                return false;

            for (var i = index + 1; i < end; i++)
            {
                var ch = sql[i];
                if (!char.IsLetterOrDigit(ch) && ch != '_')
                    return false;
            }

            tag = sql.Substring(index, end - index + 1);
            return true;
        }

        public static bool TryAdvanceSqlComment(string sql, ref int index)
        {
            if (index + 1 >= sql.Length)
                return false;

            if (sql[index] == '-' && sql[index + 1] == '-')
            {
                var end = index + 2;
                while (end < sql.Length && sql[end] is not '\r' and not '\n')
                    end++;

                index = end < sql.Length ? end : sql.Length - 1;
                return true;
            }

            if (sql[index] == '/' && sql[index + 1] == '*')
            {
                var end = sql.IndexOf("*/", index + 2, StringComparison.Ordinal);
                index = end < 0 ? sql.Length - 1 : end + 1;
                return true;
            }

            return false;
        }

        public static int FindNextSqlTokenStart(string sql, int index)
        {
            while (index < sql.Length)
            {
                while (index < sql.Length && char.IsWhiteSpace(sql[index]))
                    index++;

                var commentStart = index;
                if (!TryAdvanceSqlComment(sql, ref index))
                    return index < sql.Length ? index : -1;

                if (index == commentStart)
                    return -1;

                index++;
            }

            return -1;
        }

        private static void SkipSqlTrivia(string sql, ref int index)
        {
            while (index < sql.Length)
            {
                while (index < sql.Length && char.IsWhiteSpace(sql[index]))
                    index++;

                var commentStart = index;
                if (!TryAdvanceSqlComment(sql, ref index))
                    return;

                if (index == commentStart)
                    return;

                index++;
            }
        }

        private static bool IsSqlIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_' || value == '$';

        private static bool IsTypeNameIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_';
    }
}
