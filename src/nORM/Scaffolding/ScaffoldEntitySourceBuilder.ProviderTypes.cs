#nullable enable
using System;
using System.Data;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        public static int? GetScaffoldMaxLength(Type clrType, DataRow row)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            if (!row.Table.Columns.Contains("ColumnSize") || row["ColumnSize"] == DBNull.Value)
                return null;

            return int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0 && !IsUnboundedScaffoldMaxLength(size)
                ? size
                : null;
        }

        public static bool IsUnboundedScaffoldMaxLength(int size)
            => size == int.MaxValue
               || size == 1073741823;

        public static Type NormalizeScaffoldClrType(DatabaseProvider provider, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null, string? providerSpecificColumnType = null)
            => NormalizeScaffoldClrType(provider, clrType, allowNull, isKey, isAuto, declaredType, providerSpecificColumnType, null);

        public static Type NormalizeScaffoldClrType(
            DatabaseProvider provider,
            Type clrType,
            bool allowNull,
            bool isKey,
            bool isAuto,
            string? declaredType,
            string? providerSpecificColumnType,
            string? columnStoreType)
        {
            if (ScaffoldStoreTypeClrMapper.TryMapStoreType(provider, columnStoreType, out var storeClrType))
                return storeClrType;

            if (TryNormalizeSqliteScaffoldClrType(provider, clrType, allowNull, isKey, isAuto, declaredType, out var sqliteClrType))
                return sqliteClrType;

            if (TryNormalizePostgresScaffoldClrType(provider, providerSpecificColumnType, out var postgresClrType))
                return postgresClrType;

            if (TryNormalizeSqlServerScaffoldClrType(provider, providerSpecificColumnType, out var sqlServerClrType))
                return sqlServerClrType;

            if (TryNormalizeMySqlScaffoldClrType(provider, providerSpecificColumnType, out var mySqlClrType))
                return mySqlClrType;

            return clrType;
        }
    }
}
