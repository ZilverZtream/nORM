#nullable enable
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

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

                var match = Regex.Match(
                    trimmed,
                    @"\bCOLLATE\s+(?<name>""[^""]+""|\[[^\]]+\]|`[^`]+`|[A-Za-z_][A-Za-z0-9_-]*)",
                    RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
                if (!match.Success)
                    continue;

                result[columnName] = UnquoteSqlIdentifier(match.Groups["name"].Value);
            }

            return result;
        }
    }
}
