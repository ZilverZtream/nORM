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

            if (TryNormalizeSqliteScaffoldClrType(connection, clrType, allowNull, isKey, isAuto, declaredType, out var sqliteClrType))
                return sqliteClrType;

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
            if (TryResolvePostgresProviderSpecificClrType(
                    connection,
                    normalizedClrType,
                    columnName,
                    postgresDomainColumnCastTypes,
                    out var postgresClrType))
                return postgresClrType;

            if (TryResolveSqlServerProviderSpecificClrType(connection, sqlServerAliasBaseType, out var sqlServerClrType))
                return sqlServerClrType;

            if (TryResolveMySqlProviderSpecificClrType(
                    connection,
                    columnName,
                    mySqlUnsignedColumnTypes,
                    out var mySqlClrType))
                return mySqlClrType;

            return normalizedClrType;
        }
    }
}
