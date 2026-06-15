#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static int FindMatchingParenthesis(string sql, int openIndex)
        {
            var depth = 0;
            char? quote = null;
            string? dollarQuote = null;
            for (var i = openIndex; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (ScaffoldSqlMetadataParser.TryAdvancePostgresDollarQuote(sql, ref i, ref dollarQuote))
                    continue;
                if (ScaffoldSqlMetadataParser.TryAdvanceSqlComment(sql, ref i))
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
            string? dollarQuote = null;
            for (var i = 0; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (ScaffoldSqlMetadataParser.TryAdvancePostgresDollarQuote(sql, ref i, ref dollarQuote))
                    continue;
                if (ScaffoldSqlMetadataParser.TryAdvanceSqlComment(sql, ref i))
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

        private static IReadOnlyList<string> SplitCreateTableBodyParts(string? createTableSql)
        {
            if (string.IsNullOrWhiteSpace(createTableSql))
                return Array.Empty<string>();

            var bodyOpen = createTableSql.IndexOf('(');
            if (bodyOpen < 0)
                return Array.Empty<string>();

            var bodyClose = FindMatchingParenthesis(createTableSql, bodyOpen);
            if (bodyClose <= bodyOpen)
                return Array.Empty<string>();

            return SplitTopLevelCommaSeparated(createTableSql.Substring(bodyOpen + 1, bodyClose - bodyOpen - 1));
        }
    }
}
