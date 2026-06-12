#nullable enable
using System;
using System.Data;
using System.Text;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static (byte? Precision, byte? Scale) GetRoutineParameterPrecisionScale(string? dataType)
            => ScaffoldRoutineTypeMapper.GetRoutineParameterPrecisionScale(dataType);

        private static void AppendNullableDirective(StringBuilder sb, bool useNullableReferenceTypes)
            => sb.AppendLine(useNullableReferenceTypes ? "#nullable enable" : "#nullable disable");

        /// <summary>
        /// Returns a C# type name with correct nullability for value or reference types.
        /// </summary>
        private static string GetTypeName(Type type, bool allowNull, bool useNullableReferenceTypes = true)
            => ScaffoldTypeNameHelper.GetTypeName(type, allowNull, useNullableReferenceTypes);

        private static int? GetScaffoldMaxLength(Type clrType, DataRow row)
            => ScaffoldEntitySourceBuilder.GetScaffoldMaxLength(clrType, row);

        private static bool IsUnboundedScaffoldMaxLength(int size)
            => ScaffoldEntitySourceBuilder.IsUnboundedScaffoldMaxLength(size);

        private static Type NormalizeScaffoldClrType(DatabaseProvider provider, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null, string? providerSpecificColumnType = null)
            => ScaffoldEntitySourceBuilder.NormalizeScaffoldClrType(
                provider,
                clrType,
                allowNull,
                isKey,
                isAuto,
                declaredType,
                providerSpecificColumnType);

        private static Type NormalizeScaffoldClrType(DatabaseProvider provider, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType, string? providerSpecificColumnType, string? columnStoreType)
            => ScaffoldEntitySourceBuilder.NormalizeScaffoldClrType(
                provider,
                clrType,
                allowNull,
                isKey,
                isAuto,
                declaredType,
                providerSpecificColumnType,
                columnStoreType);

        private static bool TryMapSqlServerAliasBaseClrType(string? detail, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrType(detail, out type);

        private static string NormalizeSqlServerAliasBaseTypeName(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeSqlServerAliasBaseTypeName(typeText);

        private static int? GetSqlServerAliasBaseMaxLength(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLength(detail);

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);

        private static bool TryMapMySqlUnsignedType(string? detail, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(detail, out type);

        private static string NormalizeMySqlUnsignedTypeDetail(string detail)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeMySqlUnsignedTypeDetail(detail);

        private static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayType(detail, out arrayType);

        private static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(normalized, out arrayType);

        private static bool IsSqliteUuidDeclaredType(string? declaredType)
            => ScaffoldEntitySourceBuilder.IsSqliteUuidDeclaredType(declaredType);
    }
}
