#nullable enable
using System;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
        public static int FindCreateIndexKeyListOpen(string sql, int startIndex)
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

                if (ch == '(')
                    return i;
            }

            return -1;
        }

        private static string UnquoteCreateIndexIdentifier(string identifier)
        {
            var trimmed = identifier.Trim();
            if (trimmed.Length < 2)
                return trimmed;

            if (trimmed[0] == '[' && trimmed[^1] == ']')
                return trimmed[1..^1].Replace("]]", "]", StringComparison.Ordinal);

            var quote = trimmed[0];
            if ((quote == '"' || quote == '`') && trimmed[^1] == quote)
                return trimmed[1..^1].Replace(new string(quote, 2), quote.ToString(), StringComparison.Ordinal);

            return trimmed;
        }

        private static string NormalizeCreateIndexIdentifier(string identifier)
            => UnquoteCreateIndexIdentifier(RemoveSqlCommentsOutsideQuotes(identifier));

        private static string RemoveSqlCommentsOutsideQuotes(string sql)
        {
            var sb = new StringBuilder(sql.Length);
            char? quote = null;
            for (var i = 0; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (quote is not null)
                {
                    sb.Append(ch);
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == close)
                        {
                            sb.Append(sql[++i]);
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    sb.Append(ch);
                    continue;
                }

                if (TryAdvanceSqlComment(sql, ref i))
                    continue;

                sb.Append(ch);
            }

            return sb.ToString().Trim();
        }
    }
}
