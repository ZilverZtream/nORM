#nullable enable
using System;
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static int FindMatchingParenthesis(string sql, int openIndex)
        {
            var depth = 0;
            char? quote = null;
            for (var i = openIndex; i < sql.Length; i++)
            {
                var ch = sql[i];
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
                        continue;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }

            return -1;
        }

        public static IReadOnlyList<string> SplitTopLevelCommaSeparated(string sql)
        {
            var result = new List<string>();
            var start = 0;
            var depth = 0;
            char? quote = null;
            for (var i = 0; i < sql.Length; i++)
            {
                var ch = sql[i];
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
                        continue;
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
                else if (ch == ',' && depth == 0)
                {
                    result.Add(sql.Substring(start, i - start));
                    start = i + 1;
                }
            }

            result.Add(sql[start..]);
            return result;
        }

        private static bool StartsWithTableConstraint(string value)
            => value.StartsWith("CONSTRAINT ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("PRIMARY ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("FOREIGN ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("UNIQUE ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("CHECK ", StringComparison.OrdinalIgnoreCase);

        private static bool TryReadLeadingSqlIdentifier(string value, out string identifier, out int nextIndex)
        {
            identifier = string.Empty;
            nextIndex = 0;
            if (string.IsNullOrWhiteSpace(value))
                return false;

            var i = 0;
            while (i < value.Length && char.IsWhiteSpace(value[i]))
                i++;
            if (i >= value.Length)
                return false;

            var ch = value[i];
            if (ch is '"' or '`' or '[')
            {
                var close = ch == '[' ? ']' : ch;
                var start = ++i;
                var sb = new StringBuilder();
                while (i < value.Length)
                {
                    if (value[i] == close)
                    {
                        if (i + 1 < value.Length && value[i + 1] == close)
                        {
                            sb.Append(close);
                            i += 2;
                            continue;
                        }

                        identifier = sb.ToString();
                        nextIndex = i + 1;
                        return identifier.Length > 0;
                    }

                    sb.Append(value[i++]);
                }

                nextIndex = start;
                return false;
            }

            var begin = i;
            while (i < value.Length && (char.IsLetterOrDigit(value[i]) || value[i] == '_' || value[i] == '$'))
                i++;
            if (i == begin)
                return false;

            identifier = value.Substring(begin, i - begin);
            nextIndex = i;
            return true;
        }

        private static string UnquoteSqlIdentifier(string value)
        {
            var trimmed = value.Trim();
            if (trimmed.Length >= 2)
            {
                var first = trimmed[0];
                var last = trimmed[^1];
                if ((first == '"' && last == '"') || (first == '`' && last == '`'))
                    return trimmed.Substring(1, trimmed.Length - 2).Replace(new string(first, 2), first.ToString(), StringComparison.Ordinal);
                if (first == '[' && last == ']')
                    return trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]", StringComparison.Ordinal);
            }

            return trimmed;
        }

        private static bool IsDeclaredTypeTokenChar(char ch)
            => (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
    }
}
