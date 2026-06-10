#nullable enable
using System;
using System.Globalization;
using System.Linq;
using nORM.Migration;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqlMetadataParser
    {
        public static string? ExtractCreateIndexWhereClause(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return null;

            var onIndex = sql.IndexOf(" ON ", StringComparison.OrdinalIgnoreCase);
            if (onIndex < 0)
                return null;

            var openIndex = FindCreateIndexKeyListOpen(sql, onIndex);
            if (openIndex < 0)
                return null;

            var closeIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);
            if (closeIndex <= openIndex)
                return null;

            var where = FindSqlKeywordOutsideQuotes(sql, "WHERE", closeIndex + 1);
            return where < 0 ? null : sql[(where + 5)..].Trim();
        }

        public static bool IsCreateIndexUnique(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql))
                return false;

            var index = 0;
            return TryConsumeSqlKeyword(createIndexSql, ref index, "CREATE")
                   && TryConsumeSqlKeyword(createIndexSql, ref index, "UNIQUE");
        }

        public static bool TryConsumeSqlKeyword(string sql, ref int index, string keyword)
        {
            while (index < sql.Length && char.IsWhiteSpace(sql[index]))
                index++;

            if (index + keyword.Length > sql.Length
                || !sql.AsSpan(index, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var end = index + keyword.Length;
            if (end < sql.Length && IsSqlIdentifierChar(sql[end]))
                return false;

            index = end;
            return true;
        }

        public static string? ExtractCreateIndexExpressionSql(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql))
                return null;

            var onIndex = createIndexSql.IndexOf(" ON ", StringComparison.OrdinalIgnoreCase);
            if (onIndex < 0)
                return null;

            var openIndex = FindCreateIndexKeyListOpen(createIndexSql, onIndex);
            if (openIndex < 0)
                return null;

            var closeIndex = ScaffoldSqliteDdlParser.FindMatchingParenthesis(createIndexSql, openIndex);
            if (closeIndex <= openIndex)
                return null;

            return createIndexSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
        }

        public static int FindCreateIndexKeyListOpen(string sql, int startIndex)
        {
            char? quote = null;
            for (var i = startIndex; i < sql.Length; i++)
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
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                    return i;
            }

            return -1;
        }

        public static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
        {
            char? quote = null;
            for (var i = startIndex; i < sql.Length; i++)
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
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (i + keyword.Length <= sql.Length
                    && sql.AsSpan(i, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase)
                    && (i == 0 || !IsSqlIdentifierChar(sql[i - 1]))
                    && (i + keyword.Length == sql.Length || !IsSqlIdentifierChar(sql[i + keyword.Length])))
                {
                    return i;
                }
            }

            return -1;
        }

        public static (string Sql, bool Stored) NormalizeScaffoldComputedSql(string raw)
        {
            var candidate = raw.Trim();
            if (candidate.EndsWith("generated column", StringComparison.OrdinalIgnoreCase))
                return (string.Empty, false);
            var stored = false;

            if (candidate.StartsWith("GENERATED", StringComparison.OrdinalIgnoreCase))
            {
                var open = candidate.IndexOf('(');
                var close = open >= 0 ? ScaffoldSqliteDdlParser.FindMatchingParenthesis(candidate, open) : -1;
                if (open >= 0 && close > open)
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
                var open = candidate.IndexOf('(', keywordIndex);
                var close = open >= 0 ? ScaffoldSqliteDdlParser.FindMatchingParenthesis(candidate, open) : -1;
                if (open >= 0 && close > open)
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

        public static bool HasBalancedOuterParentheses(string value)
        {
            var depth = 0;
            char? quote = null;
            for (var i = 0; i < value.Length; i++)
            {
                var ch = value[i];
                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < value.Length && value[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
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

                if (depth == 0 && i < value.Length - 1)
                    return false;
                if (depth < 0)
                    return false;
            }

            return depth == 0 && quote is null;
        }

        public static bool TryParseDecimalPrecision(string? typeName, out int precision, out int? scale)
        {
            precision = 0;
            scale = null;
            if (string.IsNullOrWhiteSpace(typeName))
                return false;

            var open = typeName.LastIndexOf('(');
            var close = typeName.IndexOf(')', open + 1);
            if (open < 0 || close < 0)
                return false;

            var baseName = typeName[..open].Trim();
            if (!EndsWithDelimitedTypeName(baseName, "decimal")
                && !EndsWithDelimitedTypeName(baseName, "numeric"))
            {
                return false;
            }

            var body = typeName.Substring(open + 1, close - open - 1);
            var parts = body.Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length is not (1 or 2)
                || parts.Any(static part => part.Length == 0)
                || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out precision)
                || precision <= 0)
            {
                precision = 0;
                return false;
            }

            if (parts.Length == 1)
                return true;

            if (!int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var parsedScale)
                || parsedScale < 0
                || parsedScale > precision)
            {
                precision = 0;
                return false;
            }

            scale = parsedScale;
            return true;
        }

        public static bool TryParseIdentityOptions(string? detail, out long seed, out long increment)
        {
            seed = 0;
            increment = 0;
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var keywordIndex = 0;
            if (!TryConsumeSqlKeyword(detail, ref keywordIndex, "IDENTITY"))
                return false;

            while (keywordIndex < detail.Length && char.IsWhiteSpace(detail[keywordIndex]))
                keywordIndex++;

            if (keywordIndex >= detail.Length || detail[keywordIndex] != '(')
                return false;

            var open = keywordIndex;
            var comma = detail.IndexOf(',', open + 1);
            var close = detail.IndexOf(')', comma + 1);
            if (open < 0 || comma < 0 || close < 0)
                return false;

            return long.TryParse(detail.AsSpan(open + 1, comma - open - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out seed)
                && long.TryParse(detail.AsSpan(comma + 1, close - comma - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out increment)
                && increment != 0;
        }

        public static bool EndsWithDelimitedTypeName(string text, string typeName)
        {
            var trimmed = text.TrimEnd();
            if (!trimmed.EndsWith(typeName, StringComparison.OrdinalIgnoreCase))
                return false;

            var prefixLength = trimmed.Length - typeName.Length;
            return prefixLength == 0 || !IsTypeNameIdentifierChar(trimmed[prefixLength - 1]);
        }

        private static bool IsSqlIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_' || value == '$';

        private static bool IsTypeNameIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_';
    }
}
