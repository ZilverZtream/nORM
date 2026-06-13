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
    }
}
