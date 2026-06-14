#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
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
