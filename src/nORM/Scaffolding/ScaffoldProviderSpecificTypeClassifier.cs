#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldProviderSpecificTypeClassifier
    {
        public static bool RequiresPostgresSchemaProbeCast(string detail)
            => ScaffoldPostgresTypeClassifier.RequiresPostgresSchemaProbeCast(detail);

        public static bool TryGetPostgresSchemaProbeCastType(string detail, out string castType)
            => ScaffoldPostgresTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out castType);

        public static string GetPostgresDomainProbeCastType(string detail)
            => ScaffoldPostgresTypeClassifier.GetPostgresDomainProbeCastType(detail);

        public static bool TryGetPostgresDomainBaseTypeText(string? detail, out string typeText)
            => ScaffoldPostgresTypeClassifier.TryGetPostgresDomainBaseTypeText(detail, out typeText);

        public static string NormalizePostgresDomainProbeCastType(string typeText)
            => ScaffoldPostgresTypeClassifier.NormalizePostgresDomainProbeCastType(typeText);

        public static bool TryNormalizeSafePostgresDomainProbeCastType(string typeText, out string castType)
            => ScaffoldPostgresTypeClassifier.TryNormalizeSafePostgresDomainProbeCastType(typeText, out castType);

        public static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => ScaffoldPostgresTypeClassifier.TryNormalizePostgresParameterizedProbeCastType(normalized, out castType);

        public static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
            => ScaffoldPostgresTypeClassifier.TryParsePostgresTypeArguments(normalized, typeName, out args);

        public static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
            => ScaffoldPostgresTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);

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
                   || (normalized.StartsWith("array", StringComparison.Ordinal) && ScaffoldPostgresTypeClassifier.TryMapPostgresArrayType(detail, out _))
                   || ScaffoldMySqlTypeClassifier.TryParseMySqlEnumValues(detail, out _)
                   || ScaffoldMySqlTypeClassifier.TryParseBoundedMySqlSetValues(detail, out _)
                   || ScaffoldPostgresTypeClassifier.TryParsePostgresEnumValues(detail, out _)
                   || normalized == "year";
        }

        public static bool IsSafePostgresUserDefinedScalarColumnType(string normalized)
            => ScaffoldPostgresTypeClassifier.IsSafePostgresUserDefinedScalarColumnType(normalized);

        public static bool HasWriteBlockingProviderSpecificColumnTypes(IReadOnlyDictionary<string, string>? providerSpecificColumnTypes)
            => providerSpecificColumnTypes?.Values.Any(IsWriteBlockingProviderSpecificColumnType) == true;

        public static bool IsWriteBlockingProviderSpecificColumnType(string? detail)
            => !string.IsNullOrWhiteSpace(detail)
               && !IsScaffoldableProviderSpecificColumnType(detail)
               && !ScaffoldMySqlTypeClassifier.TryMapMySqlUnsignedType(detail, out _)
               && !ScaffoldMySqlTypeClassifier.IsSafeMySqlUnsignedDecimalType(detail)
               && !ScaffoldSqlServerAliasTypeClassifier.IsSafeSqlServerAliasColumnType(detail)
               && !ScaffoldPostgresTypeClassifier.IsSafePostgresDomainColumnType(detail);

        public static bool IsSafeMySqlUnsignedDecimalType(string? detail)
            => ScaffoldMySqlTypeClassifier.IsSafeMySqlUnsignedDecimalType(detail);

        public static bool IsSafeSqlServerAliasColumnType(string? detail)
            => ScaffoldSqlServerAliasTypeClassifier.IsSafeSqlServerAliasColumnType(detail);

        public static bool TryGetSqlServerAliasBaseTypeText(string? detail, out string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.TryGetSqlServerAliasBaseTypeText(detail, out typeText);

        public static bool IsSafeSqlServerAliasBaseType(string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.IsSafeSqlServerAliasBaseType(typeText);

        public static bool IsSafePostgresDomainColumnType(string? detail)
            => ScaffoldPostgresTypeClassifier.IsSafePostgresDomainColumnType(detail);

        public static bool TryParseEnumValues(string? detail, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseMySqlEnumValues(detail, out values)
               || ScaffoldPostgresTypeClassifier.TryParsePostgresEnumValues(detail, out values);

        public static bool TryParsePostgresDomainEnumValues(string? detail, out string[] values)
            => ScaffoldPostgresTypeClassifier.TryParsePostgresDomainEnumValues(detail, out values);

        public static bool TryParseMySqlEnumValues(string? detail, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseMySqlEnumValues(detail, out values);

        public static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseBoundedMySqlSetValues(detail, out values);

        public static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseMySqlQuotedTypeValues(detail, typeName, out values);

        public static bool TryParsePostgresEnumValues(string? detail, out string[] values)
            => ScaffoldPostgresTypeClassifier.TryParsePostgresEnumValues(detail, out values);

        public static bool TryParseSqlStringLiteralList(string body, out string[] values)
            => ScaffoldSqlStringLiteralParser.TryParseSqlStringLiteralList(body, out values);

        public static bool TryMapSqlServerAliasBaseClrType(string? detail, out Type type)
            => ScaffoldSqlServerAliasTypeClassifier.TryMapSqlServerAliasBaseClrType(detail, out type);

        public static bool TryMapSqlServerAliasBaseClrTypeName(string? typeText, out Type type)
            => ScaffoldSqlServerAliasTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(typeText, out type);

        public static string NormalizeSqlServerAliasBaseTypeName(string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.NormalizeSqlServerAliasBaseTypeName(typeText);

        public static int? GetSqlServerAliasBaseMaxLength(string? detail)
            => ScaffoldSqlServerAliasTypeClassifier.GetSqlServerAliasBaseMaxLength(detail);

        public static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);

        public static bool TryMapMySqlUnsignedType(string? detail, out Type type)
            => ScaffoldMySqlTypeClassifier.TryMapMySqlUnsignedType(detail, out type);

        public static string NormalizeMySqlUnsignedTypeDetail(string detail)
            => ScaffoldMySqlTypeClassifier.NormalizeMySqlUnsignedTypeDetail(detail);

        public static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
            => ScaffoldPostgresTypeClassifier.TryMapPostgresArrayType(detail, out arrayType);

        public static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
            => ScaffoldPostgresTypeClassifier.TryMapPostgresArrayCastType(normalized, out arrayType);
    }
}
