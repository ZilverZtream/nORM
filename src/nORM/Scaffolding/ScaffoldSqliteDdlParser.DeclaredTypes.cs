#nullable enable
using System;

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
    }
}
