using System;
using System.Data;
using System.Data.Common;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        private static int? GetScaffoldMaxLength(Type clrType, DataRow row)
            => DynamicEntityTableSchemaReader.GetScaffoldMaxLength(clrType, row);

        private static bool IsUnboundedScaffoldMaxLength(int size)
            => DynamicEntityTableSchemaReader.IsUnboundedScaffoldMaxLength(size);

        /// <summary>
        /// Maps a CLR type and its nullability to the property type for the generated entity.
        /// Value types that allow null are wrapped in <see cref="Nullable{T}"/>; reference types
        /// are returned as-is since nullability is implicit.
        /// </summary>
        private static Type GetPropertyType(Type type, bool allowNull)
            => DynamicEntityTableSchemaReader.GetPropertyType(type, allowNull);

        private static Type NormalizeScaffoldClrType(DbConnection connection, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null)
            => DynamicEntityTableSchemaReader.NormalizeScaffoldClrType(connection, clrType, allowNull, isKey, isAuto, declaredType);

        private static bool TryMapSqlServerAliasBaseClrType(string? typeText, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(typeText, out type);

        private static string NormalizeSqlServerAliasBaseTypeName(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeSqlServerAliasBaseTypeName(typeText);

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string? typeText)
            => string.IsNullOrWhiteSpace(typeText)
                ? null
                : ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);

        private static bool TryMapMySqlUnsignedType(string? detail, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(detail, out type);

        private static string NormalizeMySqlUnsignedTypeDetail(string detail)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeMySqlUnsignedTypeDetail(detail);

        private static string NormalizePostgresDomainProbeCastType(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizePostgresDomainProbeCastType(typeText);

        private static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryNormalizePostgresParameterizedProbeCastType(normalized, out castType);

        private static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresTypeArguments(normalized, typeName, out args);

        private static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);

        private static bool TryMapPostgresArrayCastType(string castType, out Type arrayType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(castType.Trim().ToLowerInvariant(), out arrayType);

        private static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(detail, out values);

        private static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseMySqlQuotedTypeValues(detail, typeName, out values);

        private static bool IsSqliteUuidDeclaredType(string? declaredType)
            => DynamicEntityTableSchemaReader.IsSqliteUuidDeclaredType(declaredType);

    }
}
