#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyList<(string Name, string Sql)> ExtractCheckConstraints(string tableName, string? createTableSql)
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

        public static IReadOnlyDictionary<string, (string Sql, bool Stored)> ExtractGeneratedColumns(string? createTableSql)
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

                var generatedIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(trimmed, "GENERATED", nextIndex);
                if (generatedIndex < 0)
                    continue;

                var openIndex = trimmed.IndexOf('(', generatedIndex);
                if (openIndex < 0)
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

        public static bool IsProviderSpecificDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return IsUnsafeProviderSpecificDeclaredType(normalized);
        }

        public static bool IsUnsafeProviderSpecificDeclaredType(string normalizedDeclaredType)
            => ContainsDeclaredTypeToken(normalizedDeclaredType, "GEOMETRY")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "GEOGRAPHY")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "POINT")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "LINESTRING")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "POLYGON")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "MULTIPOINT")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "MULTILINESTRING")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "MULTIPOLYGON")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "GEOMETRYCOLLECTION")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "HIERARCHYID")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "SQL_VARIANT")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "INET")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "CIDR")
               || ContainsDeclaredTypeToken(normalizedDeclaredType, "MACADDR")
               || StartsWithDeclaredTypeToken(normalizedDeclaredType, "ENUM")
               || StartsWithDeclaredTypeToken(normalizedDeclaredType, "SET")
               || normalizedDeclaredType.EndsWith("[]", StringComparison.Ordinal);

        public static bool ContainsDeclaredTypeToken(string normalizedDeclaredType, string token)
        {
            var start = 0;
            while (start < normalizedDeclaredType.Length)
            {
                var index = normalizedDeclaredType.IndexOf(token, start, StringComparison.Ordinal);
                if (index < 0)
                    return false;

                var before = index == 0 || !IsDeclaredTypeTokenChar(normalizedDeclaredType[index - 1]);
                var afterIndex = index + token.Length;
                var after = afterIndex == normalizedDeclaredType.Length || !IsDeclaredTypeTokenChar(normalizedDeclaredType[afterIndex]);
                if (before && after)
                    return true;

                start = index + token.Length;
            }

            return false;
        }

        public static bool StartsWithDeclaredTypeToken(string normalizedDeclaredType, string token)
            => normalizedDeclaredType.StartsWith(token, StringComparison.Ordinal)
               && (normalizedDeclaredType.Length == token.Length
                   || !IsDeclaredTypeTokenChar(normalizedDeclaredType[token.Length]));

        public static int FindMatchingParenthesis(string sql, int openIndex)
        {
            var depth = 0;
            char? quote = null;
            for (var i = openIndex; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                        continue;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }

            return -1;
        }

        public static IReadOnlyList<string> SplitTopLevelCommaSeparated(string sql)
        {
            var result = new List<string>();
            var start = 0;
            var depth = 0;
            char? quote = null;
            for (var i = 0; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                        continue;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                    depth++;
                else if (ch == ')')
                    depth--;
                else if (ch == ',' && depth == 0)
                {
                    result.Add(sql.Substring(start, i - start));
                    start = i + 1;
                }
            }

            result.Add(sql[start..]);
            return result;
        }

        private static bool StartsWithTableConstraint(string value)
            => value.StartsWith("CONSTRAINT ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("PRIMARY ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("FOREIGN ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("UNIQUE ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("CHECK ", StringComparison.OrdinalIgnoreCase);

        private static bool TryReadLeadingSqlIdentifier(string value, out string identifier, out int nextIndex)
        {
            identifier = string.Empty;
            nextIndex = 0;
            if (string.IsNullOrWhiteSpace(value))
                return false;

            var i = 0;
            while (i < value.Length && char.IsWhiteSpace(value[i]))
                i++;
            if (i >= value.Length)
                return false;

            var ch = value[i];
            if (ch is '"' or '`' or '[')
            {
                var close = ch == '[' ? ']' : ch;
                var start = ++i;
                var sb = new StringBuilder();
                while (i < value.Length)
                {
                    if (value[i] == close)
                    {
                        if (i + 1 < value.Length && value[i + 1] == close)
                        {
                            sb.Append(close);
                            i += 2;
                            continue;
                        }

                        identifier = sb.ToString();
                        nextIndex = i + 1;
                        return identifier.Length > 0;
                    }

                    sb.Append(value[i++]);
                }

                nextIndex = start;
                return false;
            }

            var begin = i;
            while (i < value.Length && (char.IsLetterOrDigit(value[i]) || value[i] == '_' || value[i] == '$'))
                i++;
            if (i == begin)
                return false;

            identifier = value.Substring(begin, i - begin);
            nextIndex = i;
            return true;
        }

        private static string UnquoteSqlIdentifier(string value)
        {
            var trimmed = value.Trim();
            if (trimmed.Length >= 2)
            {
                var first = trimmed[0];
                var last = trimmed[^1];
                if ((first == '"' && last == '"') || (first == '`' && last == '`'))
                    return trimmed.Substring(1, trimmed.Length - 2).Replace(new string(first, 2), first.ToString(), StringComparison.Ordinal);
                if (first == '[' && last == ']')
                    return trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]", StringComparison.Ordinal);
            }

            return trimmed;
        }

        private static bool IsDeclaredTypeTokenChar(char ch)
            => (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
    }
}
