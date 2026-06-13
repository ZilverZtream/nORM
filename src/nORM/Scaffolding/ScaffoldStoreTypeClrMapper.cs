#nullable enable
using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoreTypeClrMapper
    {
        public static bool TryMapStoreType(DatabaseProvider provider, string? storeType, out Type clrType)
        {
            clrType = typeof(object);
            if (string.IsNullOrWhiteSpace(storeType))
                return false;

            var normalized = Normalize(storeType);
            if (ScaffoldProviderKind.IsSqlServer(provider))
                return TryMapSqlServerStoreType(normalized, out clrType);
            if (ScaffoldProviderKind.IsPostgres(provider))
                return TryMapPostgresStoreType(normalized, out clrType);
            if (ScaffoldProviderKind.IsMySql(provider))
                return TryMapMySqlStoreType(normalized, out clrType);
            if (ScaffoldProviderKind.IsSqlite(provider))
                return TryMapSqliteStoreType(storeType, out clrType);

            return false;
        }

        public static bool TryMapStoreType(DbConnection connection, string? storeType, out Type clrType)
        {
            clrType = typeof(object);
            if (string.IsNullOrWhiteSpace(storeType))
                return false;

            var normalized = Normalize(storeType);
            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return TryMapSqlServerStoreType(normalized, out clrType);
            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return TryMapPostgresStoreType(normalized, out clrType);
            if (DynamicEntityConnectionKind.IsMySql(connection))
                return TryMapMySqlStoreType(normalized, out clrType);
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return TryMapSqliteStoreType(storeType, out clrType);

            return false;
        }
    }
}
