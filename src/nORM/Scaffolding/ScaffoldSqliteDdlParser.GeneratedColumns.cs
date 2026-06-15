#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyDictionary<string, (string Sql, bool Stored)> ExtractGeneratedColumns(
            string? createTableSql)
        {
            var result = new Dictionary<string, (string Sql, bool Stored)>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(createTableSql))
                return result;

            var bodyOpen = createTableSql.IndexOf('(');
            if (bodyOpen < 0)
                return result;

            var bodyClose = FindMatchingParenthesis(createTableSql, bodyOpen);
            if (bodyClose <= bodyOpen)
                return result;

            foreach (var part in SplitTopLevelCommaSeparated(createTableSql.Substring(bodyOpen + 1, bodyClose - bodyOpen - 1)))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0 || StartsWithTableConstraint(trimmed))
                    continue;

                if (!TryReadLeadingSqlIdentifier(trimmed, out var columnName, out var nextIndex))
                    continue;

                var generatedIndex = FindTopLevelSqlKeywordOutsideQuotes(trimmed, "GENERATED", nextIndex);
                var asIndex = FindTopLevelSqlKeywordOutsideQuotes(
                    trimmed,
                    "AS",
                    generatedIndex >= 0 ? generatedIndex + "GENERATED".Length : nextIndex);
                if (asIndex < 0)
                    continue;

                var openIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(trimmed, asIndex + "AS".Length);
                if (openIndex < 0 || trimmed[openIndex] != '(')
                    continue;

                var closeIndex = FindMatchingParenthesis(trimmed, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var suffix = trimmed[(closeIndex + 1)..];
                var stored = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(suffix, "STORED", 0) >= 0
                             || ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(suffix, "PERSISTED", 0) >= 0;
                var rawSql = trimmed.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                var (computedSql, normalizedStored) = ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(rawSql + (stored ? " STORED" : string.Empty));
                if (!string.IsNullOrWhiteSpace(computedSql))
                    result[columnName] = (computedSql, normalizedStored);
            }

            return result;
        }
    }
}
