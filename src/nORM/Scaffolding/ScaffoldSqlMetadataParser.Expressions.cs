#nullable enable
using System;
using nORM.Migration;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
        public static (string Sql, bool Stored) NormalizeScaffoldComputedSql(string raw)
        {
            var candidate = raw.Trim();
            if (candidate.EndsWith("generated column", StringComparison.OrdinalIgnoreCase))
                return (string.Empty, false);
            var stored = false;

            var generatedIndex = 0;
            if (TryConsumeSqlKeyword(candidate, ref generatedIndex, "GENERATED"))
            {
                var asIndex = FindSqlKeywordOutsideQuotes(candidate, "AS", generatedIndex);
                var open = asIndex >= 0 ? FindNextSqlTokenStart(candidate, asIndex + "AS".Length) : -1;
                var close = open >= 0 ? ScaffoldSqliteDdlParser.FindMatchingParenthesis(candidate, open) : -1;
                if (open >= 0 && open < candidate.Length && candidate[open] == '(' && close > open)
                {
                    var suffix = candidate[(close + 1)..];
                    stored = FindSqlKeywordOutsideQuotes(suffix, "STORED", 0) >= 0
                             || FindSqlKeywordOutsideQuotes(suffix, "PERSISTED", 0) >= 0;
                    candidate = candidate.Substring(open + 1, close - open - 1).Trim();
                }
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            if (TryTrimTrailingComputedStorageToken(candidate, "VIRTUAL", out var virtualTrimmed))
            {
                candidate = virtualTrimmed;
            }
            else if (TryTrimTrailingComputedStorageToken(candidate, "STORED", out var storedTrimmed))
            {
                candidate = storedTrimmed;
                stored = true;
            }
            else if (TryTrimTrailingComputedStorageToken(candidate, "PERSISTED", out var persistedTrimmed))
            {
                candidate = persistedTrimmed;
                stored = true;
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            return (candidate, stored);
        }

        public static bool TryTrimTrailingComputedStorageToken(string sql, string token, out string trimmedSql)
        {
            trimmedSql = sql;
            var trimmed = sql.TrimEnd();
            if (!trimmed.EndsWith(token, StringComparison.OrdinalIgnoreCase))
                return false;

            var tokenStart = trimmed.Length - token.Length;
            if (tokenStart <= 0 || !char.IsWhiteSpace(trimmed[tokenStart - 1]))
                return false;

            trimmedSql = trimmed[..tokenStart].TrimEnd();
            return true;
        }

        public static string NormalizeScaffoldCheckSql(string raw)
        {
            var candidate = raw.Trim();
            var keywordIndex = 0;
            if (TryConsumeSqlKeyword(candidate, ref keywordIndex, "CHECK"))
            {
                var open = FindNextSqlTokenStart(candidate, keywordIndex);
                var close = open >= 0 ? ScaffoldSqliteDdlParser.FindMatchingParenthesis(candidate, open) : -1;
                if (open >= 0 && open < candidate.Length && candidate[open] == '(' && close > open)
                    candidate = candidate.Substring(open + 1, close - open - 1).Trim();
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            return candidate;
        }

        public static bool TryNormalizeScaffoldDefaultSql(string? raw, out string defaultValueSql)
        {
            defaultValueSql = string.Empty;
            if (string.IsNullOrWhiteSpace(raw))
                return false;

            var candidate = raw.Trim();
            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            try
            {
                var validated = DefaultValueValidator.Validate(candidate);
                if (string.IsNullOrWhiteSpace(validated))
                    return false;

                defaultValueSql = validated;
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }
    }
}
