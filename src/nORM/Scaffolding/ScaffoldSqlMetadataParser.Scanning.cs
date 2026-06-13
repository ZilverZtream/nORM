#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
        public static bool TryAdvancePostgresDollarQuote(string sql, ref int index, ref string? dollarQuote)
        {
            if (dollarQuote is not null)
            {
                if (index + dollarQuote.Length <= sql.Length
                    && sql.AsSpan(index, dollarQuote.Length).Equals(dollarQuote.AsSpan(), StringComparison.Ordinal))
                {
                    index += dollarQuote.Length - 1;
                    dollarQuote = null;
                }

                return true;
            }

            if (!TryReadPostgresDollarQuoteTag(sql, index, out var tag))
                return false;

            dollarQuote = tag;
            index += tag.Length - 1;
            return true;
        }

        public static bool TryReadPostgresDollarQuoteTag(string sql, int index, out string tag)
        {
            tag = string.Empty;
            if (index >= sql.Length || sql[index] != '$')
                return false;

            var end = sql.IndexOf('$', index + 1);
            if (end < 0)
                return false;

            for (var i = index + 1; i < end; i++)
            {
                var ch = sql[i];
                if (!char.IsLetterOrDigit(ch) && ch != '_')
                    return false;
            }

            tag = sql.Substring(index, end - index + 1);
            return true;
        }

        public static bool TryAdvanceSqlComment(string sql, ref int index)
        {
            if (index + 1 >= sql.Length)
                return false;

            if (sql[index] == '-' && sql[index + 1] == '-')
            {
                var end = index + 2;
                while (end < sql.Length && sql[end] is not '\r' and not '\n')
                    end++;

                index = end < sql.Length ? end : sql.Length - 1;
                return true;
            }

            if (sql[index] == '/' && sql[index + 1] == '*')
            {
                var end = sql.IndexOf("*/", index + 2, StringComparison.Ordinal);
                index = end < 0 ? sql.Length - 1 : end + 1;
                return true;
            }

            return false;
        }

        public static int FindNextSqlTokenStart(string sql, int index)
        {
            while (index < sql.Length)
            {
                while (index < sql.Length && char.IsWhiteSpace(sql[index]))
                    index++;

                var commentStart = index;
                if (!TryAdvanceSqlComment(sql, ref index))
                    return index < sql.Length ? index : -1;

                if (index == commentStart)
                    return -1;

                index++;
            }

            return -1;
        }

        private static void SkipSqlTrivia(string sql, ref int index)
        {
            while (index < sql.Length)
            {
                while (index < sql.Length && char.IsWhiteSpace(sql[index]))
                    index++;

                var commentStart = index;
                if (!TryAdvanceSqlComment(sql, ref index))
                    return;

                if (index == commentStart)
                    return;

                index++;
            }
        }

        private static bool IsSqlIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_' || value == '$';

        private static bool IsTypeNameIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_';
    }
}
