#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        private static bool TryMapPostgresDomainFunctionArgumentCastType(string? dataType, out string castType)
        {
            castType = string.Empty;
            return TryGetPostgresRoutineDomainName(dataType, out var domainName)
                   && ScaffoldProviderSpecificTypeClassifier.TryGetPostgresDomainBaseTypeText(dataType, out var typeText)
                   && ScaffoldProviderSpecificTypeClassifier.TryNormalizeSafePostgresDomainProbeCastType(typeText, out _)
                   && TryFormatSimplePostgresCastIdentifier(domainName, out castType);
        }

        private static bool TryGetPostgresRoutineDomainName(string? dataType, out string domainName)
        {
            domainName = string.Empty;
            if (string.IsNullOrWhiteSpace(dataType))
                return false;

            var trimmed = dataType.Trim();
            if (!trimmed.StartsWith("DOMAIN", StringComparison.OrdinalIgnoreCase))
                return false;

            var open = trimmed.IndexOf('(');
            var arrow = trimmed.IndexOf("->", StringComparison.Ordinal);
            if (open < 0 || arrow <= open)
                return false;

            domainName = trimmed.Substring(open + 1, arrow - open - 1).Trim();
            return domainName.Length > 0;
        }

        private static bool TryFormatSimplePostgresCastIdentifier(string identifier, out string castType)
        {
            castType = string.Empty;
            var parts = identifier.Split('.', StringSplitOptions.TrimEntries);
            if (parts.Length == 0 || parts.Length > 2)
                return false;

            foreach (var part in parts)
            {
                if (!IsSimplePostgresIdentifierPart(part))
                    return false;
            }

            castType = string.Join(".", parts);
            return true;
        }

        private static bool IsSimplePostgresIdentifierPart(string value)
        {
            if (value.Length == 0 || !IsPostgresIdentifierStart(value[0]))
                return false;

            for (var i = 1; i < value.Length; i++)
            {
                if (!IsPostgresIdentifierPart(value[i]))
                    return false;
            }

            return true;
        }

        private static bool IsPostgresIdentifierStart(char ch)
            => ch is >= 'a' and <= 'z' or '_';

        private static bool IsPostgresIdentifierPart(char ch)
            => IsPostgresIdentifierStart(ch)
               || ch is >= '0' and <= '9'
               || ch == '$';
    }
}
