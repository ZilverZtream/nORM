#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        public static Type GetPropertyType(Type type, bool allowNull)
        {
            if (!type.IsValueType)
                return type;

            if (allowNull)
                return typeof(Nullable<>).MakeGenericType(type);

            return type;
        }

        public static Type NormalizeScaffoldClrType(DbConnection connection, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null)
            => NormalizeScaffoldClrType(connection, clrType, allowNull, isKey, isAuto, declaredType, null);

        public static Type NormalizeScaffoldClrType(
            DbConnection connection,
            Type clrType,
            bool allowNull,
            bool isKey,
            bool isAuto,
            string? declaredType,
            string? columnStoreType)
        {
            if (ScaffoldStoreTypeClrMapper.TryMapStoreType(connection, columnStoreType, out var storeClrType))
                return storeClrType;

            if (DynamicEntityConnectionKind.IsSqlite(connection)
                && IsSqliteUuidDeclaredType(declaredType))
            {
                return typeof(Guid);
            }

            if (DynamicEntityConnectionKind.IsSqlite(connection)
                && isKey
                && isAuto
                && !allowNull
                && clrType == typeof(int))
            {
                // SQLite INTEGER PRIMARY KEY aliases the 64-bit rowid even when
                // provider schema metadata reports Int32 for small test values.
                return typeof(long);
            }

            return clrType;
        }

        private static Type ResolveProviderSpecificClrType(
            DbConnection connection,
            Type normalizedClrType,
            string columnName,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes,
            string? sqlServerAliasBaseType,
            IReadOnlyDictionary<string, string> mySqlUnsignedColumnTypes)
        {
            if (DynamicEntityConnectionKind.IsPostgres(connection)
                && normalizedClrType == typeof(Array)
                && postgresDomainColumnCastTypes.TryGetValue(columnName, out var domainCastType)
                && ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(domainCastType.Trim().ToLowerInvariant(), out var arrayClrType))
            {
                return arrayClrType;
            }

            if (DynamicEntityConnectionKind.IsSqlServer(connection)
                && ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(sqlServerAliasBaseType, out var aliasClrType))
            {
                return aliasClrType;
            }

            if (DynamicEntityConnectionKind.IsMySql(connection)
                && mySqlUnsignedColumnTypes.TryGetValue(columnName, out var unsignedColumnType)
                && ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(unsignedColumnType, out var unsignedClrType))
            {
                return unsignedClrType;
            }

            return normalizedClrType;
        }

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && DynamicEntityReadOnlyClassifier.ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string? typeText)
            => string.IsNullOrWhiteSpace(typeText)
                ? null
                : ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);
    }
}
