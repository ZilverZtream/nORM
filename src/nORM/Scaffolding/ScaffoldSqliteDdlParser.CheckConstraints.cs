#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyList<(string Name, string Sql)> ExtractCheckConstraints(
            string tableName,
            string? createTableSql)
        {
            var result = new List<(string Name, string Sql)>();
            var ordinal = 0;
            foreach (var part in SplitCreateTableBodyParts(createTableSql))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0)
                    continue;

                var checkIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(trimmed, "CHECK", 0);
                if (checkIndex < 0)
                    continue;

                var openIndex = ScaffoldSqlMetadataParser.FindNextSqlTokenStart(trimmed, checkIndex + "CHECK".Length);
                if (openIndex < 0 || trimmed[openIndex] != '(')
                    continue;

                var closeIndex = FindMatchingParenthesis(trimmed, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var name = TryReadConstraintNameImmediatelyBefore(trimmed, checkIndex, out var constraintName)
                    ? constraintName
                    : $"CK_{ScaffoldNameHelper.ToPascalCase(tableName)}_{++ordinal}";
                var sql = trimmed.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                if (!string.IsNullOrWhiteSpace(sql))
                    result.Add((name, sql));
            }

            return result;
        }
    }
}
