#nullable enable
using System;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
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

        public static bool TryParseDeclaredStringBinaryFacet(string? declaredType, out ScaffoldColumnFacet facet)
        {
            facet = default;
            if (!TrySplitDeclaredType(declaredType, out var baseName, out var body)
                || string.IsNullOrWhiteSpace(body)
                || !int.TryParse(body, NumberStyles.None, CultureInfo.InvariantCulture, out var maxLength)
                || maxLength <= 0)
            {
                return false;
            }

            var normalizedBase = NormalizeDeclaredTypeBase(baseName);
            var isFixedLength = normalizedBase is "CHAR" or "CHARACTER" or "NCHAR" or "NATIVE CHARACTER" or "BINARY";
            var isSupported = isFixedLength
                              || normalizedBase is "VARCHAR" or "CHARACTER VARYING" or "VARYING CHARACTER" or "NVARCHAR" or "VARBINARY";
            if (!isSupported)
                return false;

            facet = new ScaffoldColumnFacet(maxLength, null, isFixedLength);
            return true;
        }

        public static bool TryParseDeclaredDecimalPrecision(string? declaredType, out ScaffoldDecimalPrecisionInfo precision)
        {
            precision = default;
            if (!ScaffoldSqlMetadataParser.TryParseDecimalPrecision(declaredType, out var parsedPrecision, out var scale))
                return false;

            precision = new ScaffoldDecimalPrecisionInfo(parsedPrecision, scale);
            return true;
        }

        private static bool TrySplitDeclaredType(string? declaredType, out string baseName, out string body)
        {
            baseName = string.Empty;
            body = string.Empty;
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var open = declaredType.IndexOf('(');
            if (open < 0)
                return false;

            var close = declaredType.IndexOf(')', open + 1);
            if (close < 0)
                return false;

            baseName = declaredType[..open].Trim();
            body = declaredType.Substring(open + 1, close - open - 1).Trim();
            return baseName.Length > 0 && body.Length > 0;
        }

        private static string NormalizeDeclaredTypeBase(string baseName)
        {
            var normalized = baseName.Trim()
                .Replace('_', ' ')
                .ToUpperInvariant();
            while (normalized.Contains("  ", StringComparison.Ordinal))
                normalized = normalized.Replace("  ", " ", StringComparison.Ordinal);

            return normalized;
        }
    }
}
