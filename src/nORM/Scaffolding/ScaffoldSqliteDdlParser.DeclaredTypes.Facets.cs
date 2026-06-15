#nullable enable
using System;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
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
    }
}
