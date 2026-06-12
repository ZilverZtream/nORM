using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class SqlServerProvider
    {
        /// <summary>
        /// T-SQL has no native regex primitive. nORM translates a deliberately
        /// small ASCII-safe Regex.IsMatch subset to LIKE/PATINDEX-compatible
        /// patterns: literal text, ^ / $ anchors, simple bracket classes such
        /// as [A-Z], \d / \w, and anchored class+fixed-count suffix shapes
        /// such as ^[A-Z]+\d{2}$. More complex regex constructs still throw
        /// instead of drifting into provider-specific semantics.
        /// </summary>
        public override string GetRegexMatchSql(string inputSql, string patternLiteral)
        {
            if (TryBuildSqlServerRegexMatchSql(inputSql, patternLiteral, ignoreCase: false, out var sql))
                return sql;

            throw new NormUnsupportedFeatureException(
                "Regex.IsMatch is not translatable on SQL Server for this pattern: T-SQL has no " +
                "built-in regex primitive. nORM supports only a provider-mobile simple subset " +
                "(literal text, ^/$ anchors, simple ASCII bracket classes, \\d, \\w). Workarounds: " +
                "(a) deploy a CLR scalar function (RegExMatch) and call it via [SqlFunction], " +
                "(b) rewrite the predicate as StartsWith/EndsWith/Contains/Like when the shape " +
                "allows, or (c) materialise the rows first and filter in memory.");
        }

        /// <summary>
        /// Case-insensitive SQL Server Regex.IsMatch for the same simple subset
        /// as <see cref="GetRegexMatchSql"/>. The input is lowered and the pattern
        /// is folded to lower-case before conversion to LIKE.
        /// </summary>
        public override string GetRegexMatchIgnoreCaseSql(string inputSql, string patternLiteral)
        {
            if (TryBuildSqlServerRegexMatchSql(inputSql, patternLiteral, ignoreCase: true, out var sql))
                return sql;

            throw new NormUnsupportedFeatureException(
                "Regex.IsMatch(..., RegexOptions.IgnoreCase) is not translatable on SQL Server " +
                "for this pattern. nORM supports only a provider-mobile simple subset " +
                "(literal text, ^/$ anchors, simple ASCII bracket classes, \\d, \\w).");
        }

        private bool TryBuildSqlServerRegexMatchSql(string inputSql, string patternLiteral, bool ignoreCase, out string sql)
        {
            sql = string.Empty;
            if (!TryDecodeSqlStringLiteral(patternLiteral, out var pattern))
                return false;

            if (ignoreCase)
            {
                inputSql = $"LOWER({inputSql})";
                pattern = pattern.ToLowerInvariant();
            }
            else
            {
                inputSql = $"({inputSql} COLLATE Latin1_General_BIN2)";
            }

            if (TryConvertAnchoredQuantifiedRegexToSqlServerPredicate(inputSql, pattern, out sql))
                return true;

            if (!TryConvertSimpleRegexToLikePattern(pattern, out var likePattern))
                return false;

            var escapedLike = QuoteSqlString(likePattern);
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar);
            sql = $"({inputSql} LIKE {escapedLike} ESCAPE '{esc}')";
            return true;
        }

        private bool TryConvertAnchoredQuantifiedRegexToSqlServerPredicate(string inputSql, string pattern, out string sql)
        {
            sql = string.Empty;
            if (pattern.Length < 6 || pattern[0] != '^' || pattern[^1] != '$' || IsEscaped(pattern, pattern.Length - 1))
                return false;

            var end = pattern.Length - 1;
            var index = 1;
            if (!TryReadSimpleSqlServerLikeClass(pattern, ref index, end, out var repeatedClass))
                return false;
            if (index >= end || pattern[index++] != '+')
                return false;
            if (!TryReadSimpleSqlServerLikeClass(pattern, ref index, end, out var suffixClass))
                return false;
            if (index >= end || pattern[index++] != '{')
                return false;

            var countStart = index;
            while (index < end && char.IsDigit(pattern[index]))
                index++;
            if (countStart == index || index >= end || pattern[index++] != '}' || index != end)
                return false;
            if (!int.TryParse(pattern.Substring(countStart, index - countStart - 1), out var suffixCount) || suffixCount <= 0 || suffixCount > 32)
                return false;
            if (!TryNegateSimpleSqlServerLikeClass(repeatedClass, out var negatedRepeatedClass))
                return false;

            var suffixPattern = string.Concat(Enumerable.Repeat(suffixClass, suffixCount));
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar);
            sql = $"(LEN({inputSql}) >= {suffixCount + 1} AND " +
                  $"RIGHT({inputSql}, {suffixCount}) LIKE {QuoteSqlString(suffixPattern)} ESCAPE '{esc}' AND " +
                  $"LEFT({inputSql}, CASE WHEN LEN({inputSql}) > {suffixCount} THEN LEN({inputSql}) - {suffixCount} ELSE 0 END) NOT LIKE {QuoteSqlString("%" + negatedRepeatedClass + "%")} ESCAPE '{esc}')";
            return true;
        }

        private static bool TryReadSimpleSqlServerLikeClass(string pattern, ref int index, int end, out string likeClass)
        {
            likeClass = string.Empty;
            if (index >= end)
                return false;

            if (pattern[index] == '\\')
            {
                index++;
                if (index >= end)
                    return false;
                likeClass = pattern[index++] switch
                {
                    'd' => "[0-9]",
                    'w' => "[A-Za-z0-9_]",
                    's' => "[ ]",
                    _ => string.Empty
                };
                return likeClass.Length != 0;
            }

            if (pattern[index] != '[')
                return false;
            var close = pattern.IndexOf(']', index + 1);
            if (close <= index + 1 || close >= end)
                return false;
            var cls = pattern.Substring(index, close - index + 1);
            if (!IsSimpleAsciiBracketClass(cls))
                return false;
            likeClass = cls;
            index = close + 1;
            return true;
        }

        private static bool TryNegateSimpleSqlServerLikeClass(string likeClass, out string negated)
        {
            negated = string.Empty;
            if (likeClass.Length < 3 || likeClass[0] != '[' || likeClass[^1] != ']' || likeClass[1] == '^')
                return false;
            negated = "[^" + likeClass.Substring(1, likeClass.Length - 2) + "]";
            return true;
        }

        private bool TryConvertSimpleRegexToLikePattern(string pattern, out string likePattern)
        {
            likePattern = string.Empty;
            var anchoredStart = pattern.Length > 0 && pattern[0] == '^';
            var anchoredEnd = pattern.Length > 0 && pattern[^1] == '$' && !IsEscaped(pattern, pattern.Length - 1);
            var start = anchoredStart ? 1 : 0;
            var end = anchoredEnd ? pattern.Length - 1 : pattern.Length;

            var sb = new System.Text.StringBuilder(pattern.Length + 2);
            if (!anchoredStart) sb.Append('%');

            for (var i = start; i < end; i++)
            {
                var c = pattern[i];
                if (c == '\\')
                {
                    if (++i >= end)
                        return false;

                    var escaped = pattern[i];
                    if (escaped == 'd') sb.Append("[0-9]");
                    else if (escaped == 'w') sb.Append("[A-Za-z0-9_]");
                    else if (escaped == 's') sb.Append(' ');
                    else if (IsRegexMeta(escaped)) AppendEscapedLikeLiteral(sb, escaped);
                    else AppendEscapedLikeLiteral(sb, escaped);
                    continue;
                }

                if (c == '[')
                {
                    var close = pattern.IndexOf(']', i + 1);
                    if (close <= i + 1)
                        return false;

                    var cls = pattern.Substring(i, close - i + 1);
                    if (!IsSimpleAsciiBracketClass(cls))
                        return false;

                    sb.Append(cls);
                    i = close;
                    continue;
                }

                if (IsRegexMeta(c))
                    return false;

                AppendEscapedLikeLiteral(sb, c);
            }

            if (!anchoredEnd) sb.Append('%');
            likePattern = sb.ToString();
            return true;
        }

        private void AppendEscapedLikeLiteral(System.Text.StringBuilder sb, char c)
        {
            var s = c.ToString();
            sb.Append(EscapeLikePattern(s));
        }

        private static bool TryDecodeSqlStringLiteral(string sqlLiteral, out string value)
        {
            value = string.Empty;
            var text = sqlLiteral.Trim();
            if (text.Length < 2 || text[0] != '\'' || text[^1] != '\'')
                return false;

            value = text.Substring(1, text.Length - 2).Replace("''", "'");
            return true;
        }

        private static string QuoteSqlString(string value)
            => "'" + value.Replace("'", "''") + "'";

        private static bool IsRegexMeta(char c)
            => c is '.' or '*' or '+' or '?' or '(' or ')' or '|' or '{' or '}';

        private static bool IsEscaped(string text, int index)
        {
            var slashCount = 0;
            for (var i = index - 1; i >= 0 && text[i] == '\\'; i--)
                slashCount++;
            return (slashCount & 1) == 1;
        }

        private static bool IsSimpleAsciiBracketClass(string cls)
        {
            if (cls.Length < 3 || cls[0] != '[' || cls[^1] != ']')
                return false;
            if (cls[1] == '^')
                return false;

            for (var i = 1; i < cls.Length - 1; i++)
            {
                var c = cls[i];
                if (c > 127)
                    return false;
                if (c is '[' or ']' or '\\')
                    return false;
                if (c == '-' && (i == 1 || i == cls.Length - 2))
                    return false;
            }

            return true;
        }
        /// <summary>
        /// SQL Server has no regexp_replace primitive. nORM translates the safe
        /// literal-pattern subset to T-SQL REPLACE with explicit collation and
        /// rejects regex constructs, captures, and replacement substitutions.
        /// </summary>
        public override string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
        {
            if (TryBuildSqlServerRegexReplaceSql(inputSql, patternLiteral, replacementLiteral, ignoreCase: false, out var sql))
                return sql;

            throw new NormUnsupportedFeatureException(
                "Regex.Replace is not translatable on SQL Server for this pattern/replacement. " +
                "nORM supports only literal patterns with literal replacements. Workarounds: " +
                "(a) deploy a CLR scalar function (RegExReplace) and call it via [SqlFunction], " +
                "or (b) materialise the rows first and apply Regex.Replace in memory.");
        }

        /// <summary>
        /// Case-insensitive SQL Server Regex.Replace for the same literal-pattern
        /// subset, using a deterministic case-insensitive collation.
        /// </summary>
        public override string GetRegexReplaceIgnoreCaseSql(string inputSql, string patternLiteral, string replacementLiteral)
        {
            if (TryBuildSqlServerRegexReplaceSql(inputSql, patternLiteral, replacementLiteral, ignoreCase: true, out var sql))
                return sql;

            throw new NormUnsupportedFeatureException(
                "Regex.Replace(..., RegexOptions.IgnoreCase) is not translatable on SQL Server " +
                "for this pattern/replacement. nORM supports only literal patterns with literal replacements.");
        }

        private bool TryBuildSqlServerRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral, bool ignoreCase, out string sql)
        {
            sql = string.Empty;
            if (!TryDecodeSqlStringLiteral(patternLiteral, out var pattern)
                || !TryDecodeSqlStringLiteral(replacementLiteral, out var replacement))
            {
                return false;
            }

            if (replacement.Contains('$'))
                return false;

            if (!TryConvertRegexLiteralPattern(pattern, out var literalPattern))
                return false;

            var collation = ignoreCase ? "Latin1_General_CI_AS" : "Latin1_General_BIN2";
            sql = $"REPLACE(({inputSql} COLLATE {collation}), {QuoteSqlString(literalPattern)}, {QuoteSqlString(replacement)})";
            return true;
        }

        private static bool TryConvertRegexLiteralPattern(string pattern, out string literal)
        {
            literal = string.Empty;
            var sb = new System.Text.StringBuilder(pattern.Length);
            for (var i = 0; i < pattern.Length; i++)
            {
                var c = pattern[i];
                if (c == '\\')
                {
                    if (++i >= pattern.Length)
                        return false;

                    var escaped = pattern[i];
                    if (escaped is 'd' or 'D' or 'w' or 'W' or 's' or 'S')
                        return false;
                    sb.Append(escaped);
                    continue;
                }

                if (c is '^' or '$' or '[' or ']')
                    return false;
                if (IsRegexMeta(c))
                    return false;

                sb.Append(c);
            }

            literal = sb.ToString();
            return literal.Length > 0;
        }
    }
}
