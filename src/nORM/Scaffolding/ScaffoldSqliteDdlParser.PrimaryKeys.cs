#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static string? ExtractPrimaryKeyConstraintName(string? createTableSql)
        {
            if (string.IsNullOrWhiteSpace(createTableSql))
                return null;

            var bodyOpen = createTableSql.IndexOf('(');
            if (bodyOpen < 0)
                return null;

            var bodyClose = FindMatchingParenthesis(createTableSql, bodyOpen);
            if (bodyClose <= bodyOpen)
                return null;

            foreach (var part in SplitTopLevelCommaSeparated(createTableSql.Substring(bodyOpen + 1, bodyClose - bodyOpen - 1)))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0
                    || !TryFindPrimaryKeyKeywordIndex(trimmed, out var primaryIndex)
                    || !TryReadConstraintNameImmediatelyBefore(trimmed, primaryIndex, out var constraintName))
                {
                    continue;
                }

                return constraintName;
            }

            return null;
        }

        private static bool TryFindPrimaryKeyKeywordIndex(string sql, out int primaryIndex)
        {
            var searchIndex = 0;
            while (searchIndex < sql.Length)
            {
                primaryIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "PRIMARY", searchIndex);
                if (primaryIndex < 0)
                    return false;

                var keyIndex = primaryIndex + "PRIMARY".Length;
                if (ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(sql, ref keyIndex, "KEY"))
                    return true;

                searchIndex = primaryIndex + "PRIMARY".Length;
            }

            primaryIndex = -1;
            return false;
        }

        private static bool TryReadConstraintNameImmediatelyBefore(string sql, int primaryIndex, out string name)
        {
            name = string.Empty;
            var searchIndex = 0;
            var constraintIndex = -1;
            while (searchIndex < primaryIndex)
            {
                var next = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "CONSTRAINT", searchIndex);
                if (next < 0 || next >= primaryIndex)
                    break;

                constraintIndex = next;
                searchIndex = next + "CONSTRAINT".Length;
            }

            if (constraintIndex < 0)
                return false;

            var identifierIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, constraintIndex + "CONSTRAINT".Length);
            if (identifierIndex < 0
                || identifierIndex >= primaryIndex
                || !TryReadSqlIdentifier(sql, identifierIndex, out name, out var nextIndex))
            {
                return false;
            }

            var nextTokenIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, nextIndex);
            return nextTokenIndex == primaryIndex;
        }
    }
}
