#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
        public static string? ExtractCreateIndexWhereClause(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)
                || !TryGetCreateIndexKeyListBounds(sql, out _, out var closeIndex))
            {
                return null;
            }

            var where = FindSqlKeywordOutsideQuotes(sql, "WHERE", closeIndex + 1);
            if (where < 0)
                return null;

            var filterSql = TrimTrailingSqlStatementTerminator(sql[(where + 5)..]);
            return filterSql.Length == 0 ? null : filterSql;
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
            if (string.IsNullOrWhiteSpace(createIndexSql)
                || !TryGetCreateIndexKeyListBounds(createIndexSql, out var openIndex, out var closeIndex))
            {
                return null;
            }

            return createIndexSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
        }

        public static string[] ExtractCreateIndexIncludedColumnNames(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql)
                || !TryGetCreateIndexKeyListBounds(createIndexSql, out _, out var closeIndex))
            {
                return Array.Empty<string>();
            }

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
                .Select(NormalizeCreateIndexIdentifier)
                .Where(static name => name.Length > 0)
                .ToArray();
        }

        private static bool TryGetCreateIndexKeyListBounds(string sql, out int openIndex, out int closeIndex)
        {
            openIndex = -1;
            closeIndex = -1;

            var onIndex = FindCreateIndexOnKeyword(sql);
            if (onIndex < 0)
                return false;

            openIndex = FindCreateIndexKeyListOpen(sql, onIndex);
            if (openIndex < 0)
                return false;

            closeIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);
            return closeIndex > openIndex;
        }

        private static int FindCreateIndexOnKeyword(string sql)
            => FindSqlKeywordOutsideQuotes(sql, "ON", 0);

        private static string TrimTrailingSqlStatementTerminator(string sql)
        {
            var trimmed = sql.Trim();
            while (trimmed.EndsWith(";", StringComparison.Ordinal))
                trimmed = trimmed[..^1].TrimEnd();

            return trimmed;
        }
    }
}
