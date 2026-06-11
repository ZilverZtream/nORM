#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
        public static string? ExtractCreateIndexWhereClause(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return null;

            var onIndex = sql.IndexOf(" ON ", StringComparison.OrdinalIgnoreCase);
            if (onIndex < 0)
                return null;

            var openIndex = FindCreateIndexKeyListOpen(sql, onIndex);
            if (openIndex < 0)
                return null;

            var closeIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);
            if (closeIndex <= openIndex)
                return null;

            var where = FindSqlKeywordOutsideQuotes(sql, "WHERE", closeIndex + 1);
            return where < 0 ? null : sql[(where + 5)..].Trim();
        }

        public static bool IsCreateIndexUnique(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql))
                return false;

            var index = 0;
            return TryConsumeSqlKeyword(createIndexSql, ref index, "CREATE")
                   && TryConsumeSqlKeyword(createIndexSql, ref index, "UNIQUE");
        }

        public static string? ExtractCreateIndexExpressionSql(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql))
                return null;

            var onIndex = createIndexSql.IndexOf(" ON ", StringComparison.OrdinalIgnoreCase);
            if (onIndex < 0)
                return null;

            var openIndex = FindCreateIndexKeyListOpen(createIndexSql, onIndex);
            if (openIndex < 0)
                return null;

            var closeIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(createIndexSql, openIndex);
            if (closeIndex <= openIndex)
                return null;

            return createIndexSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
        }

        public static int FindCreateIndexKeyListOpen(string sql, int startIndex)
        {
            char? quote = null;
            for (var i = startIndex; i < sql.Length; i++)
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
    }
}
