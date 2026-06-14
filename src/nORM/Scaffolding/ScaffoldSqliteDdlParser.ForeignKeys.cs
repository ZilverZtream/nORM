#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyDictionary<string, string> ExtractForeignKeyProviderSemanticsByColumns(string? createTableSql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
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
                if (trimmed.Length == 0)
                    continue;

                if (!TryReadForeignKeyClause(trimmed, out var dependentColumns, out var semanticTail))
                    continue;

                var semantics = ExtractForeignKeyProviderSemantics(semanticTail);
                if (!string.IsNullOrWhiteSpace(semantics))
                    result[BuildForeignKeyColumnKey(dependentColumns)] = semantics;
            }

            return result;
        }

        public static string BuildForeignKeyColumnKey(IEnumerable<string> dependentColumns)
            => string.Join("\u001f", dependentColumns.Select(static column => column.Trim()));

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

        private static string ExtractForeignKeyProviderSemantics(string sql)
        {
            var pieces = new List<string>();
            if (TryExtractMatchSemantic(sql, out var matchSemantic))
                pieces.Add(matchSemantic);
            if (TryExtractDeferrableSemantic(sql, out var deferrableSemantic))
                pieces.Add(deferrableSemantic);
            return string.Join(" ", pieces);
        }

        private static bool TryExtractMatchSemantic(string sql, out string semantic)
        {
            semantic = string.Empty;
            var matchIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "MATCH", 0);
            if (matchIndex < 0)
                return false;

            var nameIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(sql, matchIndex + "MATCH".Length);
            if (nameIndex < 0 || !TryReadSqlIdentifier(sql, nameIndex, out var matchName, out _))
                return false;

            var normalized = matchName.Replace('_', ' ').Trim().ToUpperInvariant();
            if (normalized is "" or "NONE" or "SIMPLE")
                return false;

            semantic = "MATCH " + normalized;
            return true;
        }

        private static bool TryExtractDeferrableSemantic(string sql, out string semantic)
        {
            semantic = string.Empty;
            var deferrableIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "DEFERRABLE", 0);
            if (deferrableIndex < 0 || HasNotImmediatelyBefore(sql, deferrableIndex))
                return false;

            semantic = "DEFERRABLE";
            var initiallyIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, "INITIALLY", deferrableIndex + "DEFERRABLE".Length);
            if (initiallyIndex < 0)
                return true;

            if (ContainsKeywordAfter(sql, "DEFERRED", initiallyIndex + "INITIALLY".Length))
                semantic += " INITIALLY DEFERRED";
            else if (ContainsKeywordAfter(sql, "IMMEDIATE", initiallyIndex + "INITIALLY".Length))
                semantic += " INITIALLY IMMEDIATE";

            return true;
        }

        private static bool HasNotImmediatelyBefore(string sql, int index)
        {
            var prefix = sql[..index].TrimEnd();
            return prefix.EndsWith("NOT", StringComparison.OrdinalIgnoreCase)
                   && (prefix.Length == 3 || !char.IsLetterOrDigit(prefix[^4]));
        }

        private static bool ContainsKeywordAfter(string sql, string keyword, int startIndex)
            => ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, keyword, startIndex) >= 0;
    }
}
