#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlTypeClassifier
    {
        public static bool IsSafeMySqlUnsignedDecimalType(string? detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = NormalizeMySqlUnsignedTypeDetail(detail);
            return normalized is "decimal unsigned" or "numeric unsigned";
        }

        public static bool TryMapMySqlUnsignedType(string? detail, out Type type)
        {
            type = typeof(object);
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = NormalizeMySqlUnsignedTypeDetail(detail);
            if (!normalized.Contains("unsigned", StringComparison.Ordinal))
                return false;

            type = normalized switch
            {
                "tinyint unsigned" => typeof(byte),
                "smallint unsigned" => typeof(ushort),
                "mediumint unsigned" or "int unsigned" or "integer unsigned" => typeof(uint),
                "bigint unsigned" => typeof(ulong),
                _ => typeof(object)
            };

            return type != typeof(object);
        }

        public static string NormalizeMySqlUnsignedTypeDetail(string detail)
        {
            var normalized = detail.Trim().ToLowerInvariant();
            var unsignedIndex = normalized.IndexOf("unsigned", StringComparison.Ordinal);
            if (unsignedIndex < 0)
                return normalized;

            var baseType = normalized[..unsignedIndex].Trim();
            var paren = baseType.IndexOf('(');
            if (paren >= 0)
                baseType = baseType[..paren].Trim();

            return baseType.Length == 0 ? normalized : baseType + " unsigned";
        }
    }
}
