#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        /// <summary>
        /// True when <paramref name="columnName"/> is declared as a COLUMN-CONSTRAINT
        /// <c>INTEGER PRIMARY KEY … DESC</c>, which SQLite documents as NOT aliasing the rowid (unlike the
        /// plain/ASC column-constraint form). Such a key is app-assigned, not store-generated, so it must not
        /// be scaffolded as an identity. The table-constraint form <c>PRIMARY KEY(col DESC)</c> still aliases
        /// the rowid and is intentionally NOT matched here.
        /// </summary>
        public static bool IsIntegerPrimaryKeyDescColumn(string? createTableSql, string columnName)
        {
            if (string.IsNullOrWhiteSpace(createTableSql) || string.IsNullOrEmpty(columnName))
                return false;

            foreach (var part in SplitCreateTableBodyParts(createTableSql))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0 || StartsWithTableConstraint(trimmed))
                    continue;

                if (!TryReadLeadingSqlIdentifier(trimmed, out var colName, out var afterName)
                    || !string.Equals(colName, columnName, StringComparison.OrdinalIgnoreCase))
                    continue;

                // Column-constraint grammar: <col> <type> PRIMARY KEY [ASC|DESC] [conflict] [AUTOINCREMENT].
                // A DESC direction immediately after PRIMARY KEY disables rowid aliasing.
                var primaryIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(trimmed, "PRIMARY", afterName);
                if (primaryIndex < 0)
                    return false;

                var index = primaryIndex + "PRIMARY".Length;
                return ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(trimmed, ref index, "KEY")
                    && ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(trimmed, ref index, "DESC");
            }

            return false;
        }

        public static string? ExtractPrimaryKeyConstraintName(string? createTableSql)
        {
            foreach (var part in SplitCreateTableBodyParts(createTableSql))
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
