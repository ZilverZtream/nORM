#nullable enable
using System;
using System.Linq;

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

        public static string[] ExtractCreateIndexIncludedColumnNames(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql))
                return Array.Empty<string>();

            var onIndex = createIndexSql.IndexOf(" ON ", StringComparison.OrdinalIgnoreCase);
            if (onIndex < 0)
                return Array.Empty<string>();

            var openIndex = FindCreateIndexKeyListOpen(createIndexSql, onIndex);
            if (openIndex < 0)
                return Array.Empty<string>();

            var closeIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(createIndexSql, openIndex);
            if (closeIndex <= openIndex)
                return Array.Empty<string>();

            var includeIndex = FindSqlKeywordOutsideQuotes(createIndexSql, "INCLUDE", closeIndex + 1);
            if (includeIndex < 0)
                return Array.Empty<string>();

            var index = includeIndex + "INCLUDE".Length;
            while (index < createIndexSql.Length && char.IsWhiteSpace(createIndexSql[index]))
                index++;
            if (index >= createIndexSql.Length || createIndexSql[index] != '(')
                return Array.Empty<string>();

            var includeCloseIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(createIndexSql, index);
            if (includeCloseIndex <= index)
                return Array.Empty<string>();

            var body = createIndexSql.Substring(index + 1, includeCloseIndex - index - 1);
            return ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(body)
                .Select(UnquoteCreateIndexIdentifier)
                .Where(static name => name.Length > 0)
                .ToArray();
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
    }
}
