#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
        {
            castType = string.Empty;
            return TryGetPostgresArrayElementType(normalized, out var element)
                   && TryNormalizePostgresArrayElementCastType(element, out castType);
        }

        public static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
        {
            arrayType = typeof(object[]);
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = detail.Trim().ToLowerInvariant();
            if (normalized.Contains("domain", StringComparison.OrdinalIgnoreCase))
                normalized = GetPostgresDomainProbeCastType(detail).Trim().ToLowerInvariant();

            return TryMapPostgresArrayCastType(normalized, out arrayType);
        }

        public static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
        {
            arrayType = typeof(object[]);
            if (TryMapPostgresArrayProbeCastType(normalized, out var canonicalCastType))
                normalized = canonicalCastType;

            if (!TryGetPostgresArrayElementType(normalized, out var element)
                || !TryMapPostgresArrayElementClrType(element, out var elementType))
            {
                return false;
            }

            arrayType = elementType.MakeArrayType();
            return true;
        }
    }
}
