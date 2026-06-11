#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static bool TryParsePostgresDomainEnumValues(string? detail, out string[] values)
        {
            values = Array.Empty<string>();
            return TryGetPostgresDomainBaseTypeText(detail, out var typeText)
                   && TryParsePostgresEnumValues(typeText, out values);
        }

        public static bool TryParsePostgresEnumValues(string? detail, out string[] values)
        {
            values = Array.Empty<string>();
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var trimmed = detail.Trim();
            if (!trimmed.StartsWith("ENUM (", StringComparison.OrdinalIgnoreCase) || !trimmed.EndsWith(")", StringComparison.Ordinal))
                return false;

            var colon = trimmed.IndexOf(':');
            if (colon < 0)
                return false;

            var valueList = trimmed.Substring(colon + 1, trimmed.Length - colon - 2).Trim();
            return ScaffoldSqlStringLiteralParser.TryParseSqlStringLiteralList(valueList, out values);
        }
    }
}
