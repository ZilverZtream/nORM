#nullable enable
using System;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlMetadataParser
    {
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
    }
}
