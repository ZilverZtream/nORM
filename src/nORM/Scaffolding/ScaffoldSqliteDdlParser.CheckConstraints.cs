#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyList<(string Name, string Sql)> ExtractCheckConstraints(
            string tableName,
            string? createTableSql)
        {
            if (string.IsNullOrWhiteSpace(createTableSql))
                return Array.Empty<(string Name, string Sql)>();

            var bodyOpen = createTableSql.IndexOf('(');
            if (bodyOpen < 0)
                return Array.Empty<(string Name, string Sql)>();

            var bodyClose = FindMatchingParenthesis(createTableSql, bodyOpen);
            if (bodyClose <= bodyOpen)
                return Array.Empty<(string Name, string Sql)>();

            var result = new List<(string Name, string Sql)>();
            var ordinal = 0;
            foreach (var part in SplitTopLevelCommaSeparated(createTableSql.Substring(bodyOpen + 1, bodyClose - bodyOpen - 1)))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0)
                    continue;

                var checkIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(trimmed, "CHECK", 0);
                if (checkIndex < 0)
                    continue;

                var openIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(trimmed, checkIndex + "CHECK".Length);
                if (openIndex < 0 || trimmed[openIndex] != '(')
                    continue;

                var closeIndex = FindMatchingParenthesis(trimmed, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var name = TryReadCheckConstraintName(trimmed, checkIndex, out var constraintName)
                    ? constraintName
                    : $"CK_{ScaffoldNameHelper.ToPascalCase(tableName)}_{++ordinal}";
                var sql = trimmed.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                if (!string.IsNullOrWhiteSpace(sql))
                    result.Add((name, sql));
            }

            return result;
        }

        private static bool TryReadCheckConstraintName(string sql, int checkIndex, out string name)
        {
            name = string.Empty;
            var searchIndex = 0;
            var constraintIndex = -1;
            while (searchIndex < checkIndex)
            {
                var next = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "CONSTRAINT", searchIndex);
                if (next < 0 || next >= checkIndex)
                    break;

                constraintIndex = next;
                searchIndex = next + "CONSTRAINT".Length;
            }

            if (constraintIndex < 0)
                return false;

            var identifierIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, constraintIndex + "CONSTRAINT".Length);
            return identifierIndex >= 0
                   && identifierIndex < checkIndex
                   && TryReadSqlIdentifier(sql, identifierIndex, out name, out var nextIndex)
                   && nextIndex <= checkIndex;
        }
    }
}
