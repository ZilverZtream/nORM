#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyDictionary<string, string> ExtractColumnCollations(string? createTableSql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(createTableSql))
                return result;

            var open = createTableSql.IndexOf('(');
            if (open < 0)
                return result;
            var close = FindMatchingParenthesis(createTableSql, open);
            if (close <= open)
                return result;

            foreach (var part in SplitTopLevelCommaSeparated(createTableSql.Substring(open + 1, close - open - 1)))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0 || StartsWithTableConstraint(trimmed))
                    continue;

                if (!TryReadLeadingSqlIdentifier(trimmed, out var columnName, out _))
                    continue;

                var collateIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(trimmed, "COLLATE", 0);
                if (collateIndex < 0)
                    continue;

                var nameIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(trimmed, collateIndex + "COLLATE".Length);
                if (nameIndex < 0 || !TryReadSqliteCollationName(trimmed, nameIndex, out var collationName))
                    continue;

                result[columnName] = collationName;
            }

            return result;
        }

        private static bool TryReadSqliteCollationName(string value, int index, out string name)
        {
            name = string.Empty;
            if (index >= value.Length)
                return false;

            if (value[index] is '"' or '`' or '[')
                return TryReadSqlIdentifier(value, index, out name, out _);

            if (!char.IsLetter(value[index]) && value[index] != '_')
                return false;

            var start = index;
            while (index < value.Length
                   && (char.IsLetterOrDigit(value[index]) || value[index] is '_' or '-' or '$'))
            {
                index++;
            }

            name = value.Substring(start, index - start);
            return name.Length > 0;
        }
    }
}
