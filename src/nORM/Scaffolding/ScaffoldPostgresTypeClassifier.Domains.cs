#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static string GetPostgresDomainProbeCastType(string detail)
        {
            return TryGetPostgresDomainBaseTypeText(detail, out var typeText)
                && TryNormalizeSafePostgresDomainProbeCastType(typeText, out var castType)
                ? castType
                : "text";
        }

        public static bool TryGetPostgresDomainBaseTypeText(string? detail, out string typeText)
        {
            typeText = string.Empty;
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var trimmed = detail.Trim();
            if (!trimmed.StartsWith("DOMAIN", StringComparison.OrdinalIgnoreCase))
                return false;

            var arrowIndex = trimmed.IndexOf("->", StringComparison.Ordinal);
            if (arrowIndex < 0)
                return false;

            typeText = trimmed[(arrowIndex + 2)..].Trim();
            if (typeText.EndsWith(")", StringComparison.Ordinal))
                typeText = typeText[..^1].Trim();

            var udtSuffixIndex = typeText.LastIndexOf(" (", StringComparison.Ordinal);
            if (udtSuffixIndex >= 0
                && typeText.EndsWith(")", StringComparison.Ordinal)
                && !typeText.StartsWith("ARRAY", StringComparison.OrdinalIgnoreCase)
                && !typeText.StartsWith("ENUM", StringComparison.OrdinalIgnoreCase)
                && !typeText.StartsWith("USER-DEFINED", StringComparison.OrdinalIgnoreCase))
            {
                typeText = typeText[..udtSuffixIndex].Trim();
            }

            return !string.IsNullOrWhiteSpace(typeText);
        }

        public static string NormalizePostgresDomainProbeCastType(string typeText)
            => TryNormalizeSafePostgresDomainProbeCastType(typeText, out var castType)
                ? castType
                : "text";

        public static bool IsSafePostgresDomainColumnType(string? detail)
            => TryGetPostgresDomainBaseTypeText(detail, out var typeText)
               && TryNormalizeSafePostgresDomainProbeCastType(typeText, out _);
    }
}
