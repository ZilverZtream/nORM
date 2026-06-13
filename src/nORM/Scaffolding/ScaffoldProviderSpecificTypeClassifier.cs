#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldProviderSpecificTypeClassifier
    {
        public static bool IsScaffoldableProviderSpecificColumnType(string? detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = detail.Trim().ToLowerInvariant();
            return normalized == "xml"
                   || normalized == "json"
                   || normalized == "jsonb"
                   || normalized == "uuid"
                   || ScaffoldPostgresTypeClassifier.IsSafePostgresUserDefinedScalarColumnType(normalized)
                   || (!normalized.StartsWith("domain", StringComparison.Ordinal)
                       && ScaffoldPostgresTypeClassifier.TryMapPostgresArrayType(detail, out _))
                   || ScaffoldMySqlTypeClassifier.TryParseMySqlEnumValues(detail, out _)
                   || ScaffoldMySqlTypeClassifier.TryParseBoundedMySqlSetValues(detail, out _)
                   || ScaffoldPostgresTypeClassifier.TryParsePostgresEnumValues(detail, out _)
                   || normalized == "year";
        }

        public static bool HasWriteBlockingProviderSpecificColumnTypes(IReadOnlyDictionary<string, string>? providerSpecificColumnTypes)
            => providerSpecificColumnTypes?.Values.Any(IsWriteBlockingProviderSpecificColumnType) == true;

        public static bool IsWriteBlockingProviderSpecificColumnType(string? detail)
            => !string.IsNullOrWhiteSpace(detail)
               && !IsScaffoldableProviderSpecificColumnType(detail)
               && !ScaffoldMySqlTypeClassifier.TryMapMySqlUnsignedType(detail, out _)
               && !ScaffoldMySqlTypeClassifier.IsSafeMySqlUnsignedDecimalType(detail)
               && !ScaffoldSqlServerAliasTypeClassifier.IsSafeSqlServerAliasColumnType(detail)
               && !ScaffoldPostgresTypeClassifier.IsSafePostgresDomainColumnType(detail);

        public static bool TryParseEnumValues(string? detail, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseMySqlEnumValues(detail, out values)
               || ScaffoldPostgresTypeClassifier.TryParsePostgresEnumValues(detail, out values);

        public static bool TryParseSqlStringLiteralList(string body, out string[] values)
            => ScaffoldSqlStringLiteralParser.TryParseSqlStringLiteralList(body, out values);
    }
}
