#nullable enable
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyList<(string Name, string Sql)> ExtractCheckConstraints(
            string tableName,
            string? createTableSql)
        {
            if (string.IsNullOrWhiteSpace(createTableSql))
                return Array.Empty<(string Name, string Sql)>();

            var result = new List<(string Name, string Sql)>();
            var regex = new Regex(
                @"(?:(?:CONSTRAINT)\s+(?:""(?<name>[^""]+)""|\[(?<name>[^\]]+)\]|`(?<name>[^`]+)`|(?<name>[A-Za-z_][A-Za-z0-9_]*))\s+)?CHECK\s*\(",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            var ordinal = 0;
            foreach (Match match in regex.Matches(createTableSql))
            {
                var openIndex = createTableSql.IndexOf('(', match.Index + match.Length - 1);
                if (openIndex < 0)
                    continue;

                var closeIndex = FindMatchingParenthesis(createTableSql, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var name = match.Groups["name"].Success
                    ? match.Groups["name"].Value
                    : $"CK_{ScaffoldNameHelper.ToPascalCase(tableName)}_{++ordinal}";
                var sql = createTableSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                if (!string.IsNullOrWhiteSpace(sql))
                    result.Add((name, sql));
            }

            return result;
        }
    }
}
