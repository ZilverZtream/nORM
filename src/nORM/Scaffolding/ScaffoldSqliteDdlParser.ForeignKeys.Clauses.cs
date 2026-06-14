#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        private static bool TryReadForeignKeyClause(
            string sql,
            out IReadOnlyList<string> dependentColumns,
            out string semanticTail)
            => TryReadTableForeignKeyClause(sql, out dependentColumns, out semanticTail)
               || TryReadColumnForeignKeyClause(sql, out dependentColumns, out semanticTail);

        private static bool TryReadTableForeignKeyClause(
            string sql,
            out IReadOnlyList<string> dependentColumns,
            out string semanticTail)
        {
            dependentColumns = Array.Empty<string>();
            semanticTail = string.Empty;
            var searchStart = SkipOptionalConstraintName(sql);
            var foreignIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "FOREIGN", searchStart);
            if (foreignIndex < 0)
                return false;

            var keyIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "KEY", foreignIndex + "FOREIGN".Length);
            if (keyIndex < 0)
                return false;

            var openIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, keyIndex + "KEY".Length);
            if (openIndex < 0 || sql[openIndex] != '(')
                return false;

            var closeIndex = FindMatchingParenthesis(sql, openIndex);
            if (closeIndex <= openIndex)
                return false;

            dependentColumns = SplitTopLevelCommaSeparated(sql.Substring(openIndex + 1, closeIndex - openIndex - 1))
                .Select(static column => UnquoteSqlIdentifier(column))
                .Where(static column => column.Length > 0)
                .ToArray();
            semanticTail = sql[(closeIndex + 1)..];
            return dependentColumns.Count > 0;
        }

        private static bool TryReadColumnForeignKeyClause(
            string sql,
            out IReadOnlyList<string> dependentColumns,
            out string semanticTail)
        {
            dependentColumns = Array.Empty<string>();
            semanticTail = string.Empty;
            if (StartsWithTableConstraint(sql)
                || !TryReadLeadingSqlIdentifier(sql, out var columnName, out var nextIndex))
            {
                return false;
            }

            var referencesIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "REFERENCES", nextIndex);
            if (referencesIndex < 0)
                return false;

            dependentColumns = new[] { columnName };
            semanticTail = sql[(referencesIndex + "REFERENCES".Length)..];
            return true;
        }

        private static int SkipOptionalConstraintName(string sql)
        {
            var constraintIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "CONSTRAINT", 0);
            if (constraintIndex != 0)
                return 0;

            var nameIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, "CONSTRAINT".Length);
            return nameIndex >= 0 && TryReadSqlIdentifier(sql, nameIndex, out _, out var nextIndex)
                ? nextIndex
                : "CONSTRAINT".Length;
        }
    }
}
