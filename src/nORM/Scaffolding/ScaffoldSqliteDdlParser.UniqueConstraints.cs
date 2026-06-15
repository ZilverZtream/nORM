#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyDictionary<string, string> ExtractUniqueConstraintNamesByColumns(string? createTableSql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var part in SplitCreateTableBodyParts(createTableSql))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0
                    || !TryReadUniqueConstraintClause(trimmed, out var constraintName, out var columns)
                    || string.IsNullOrWhiteSpace(constraintName))
                {
                    continue;
                }

                result[BuildColumnListKey(columns)] = constraintName;
            }

            return result;
        }

        private static bool TryReadUniqueConstraintClause(
            string sql,
            out string? constraintName,
            out IReadOnlyList<string> columns)
            => TryReadTableUniqueConstraintClause(sql, out constraintName, out columns)
               || TryReadColumnUniqueConstraintClause(sql, out constraintName, out columns);

        private static bool TryReadTableUniqueConstraintClause(
            string sql,
            out string? constraintName,
            out IReadOnlyList<string> columns)
        {
            constraintName = null;
            columns = Array.Empty<string>();
            var uniqueIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "UNIQUE", 0);
            if (uniqueIndex < 0)
                return false;

            var openIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, uniqueIndex + "UNIQUE".Length);
            if (openIndex < 0 || sql[openIndex] != '(')
                return false;

            if (TryReadConstraintNameImmediatelyBefore(sql, uniqueIndex, out var name))
                constraintName = name;

            var closeIndex = FindMatchingParenthesis(sql, openIndex);
            if (closeIndex <= openIndex)
                return false;

            columns = SplitTopLevelCommaSeparated(sql.Substring(openIndex + 1, closeIndex - openIndex - 1))
                .Select(static column => UnquoteSqlIdentifier(column))
                .Where(static column => column.Length > 0)
                .ToArray();
            return columns.Count > 0;
        }

        private static bool TryReadColumnUniqueConstraintClause(
            string sql,
            out string? constraintName,
            out IReadOnlyList<string> columns)
        {
            constraintName = null;
            columns = Array.Empty<string>();
            if (StartsWithTableConstraint(sql)
                || !TryReadLeadingSqlIdentifier(sql, out var columnName, out var nextIndex))
            {
                return false;
            }

            var uniqueIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "UNIQUE", nextIndex);
            if (uniqueIndex < 0)
                return false;

            if (TryReadConstraintNameImmediatelyBefore(sql, uniqueIndex, out var name))
                constraintName = name;

            columns = new[] { columnName };
            return true;
        }
    }
}
