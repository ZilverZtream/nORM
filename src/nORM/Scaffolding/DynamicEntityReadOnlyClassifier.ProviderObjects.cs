#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        public static bool IsDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return IsSqliteDynamicQueryObject(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return IsSqlServerDynamicQueryObject(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return IsPostgresDynamicQueryObject(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return IsMySqlDynamicQueryObject(connection, schemaName, tableName);

            return false;
        }

        public static bool IsProviderOwnedSynonym(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityConnectionKind.IsSqlServer(connection)
                && IsSqlServerProviderOwnedSynonym(connection, schemaName, tableName);

        public static bool IsProviderNativeTemporalTable(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityConnectionKind.IsSqlServer(connection)
                && IsSqlServerProviderNativeTemporalTable(connection, schemaName, tableName);

        public static bool HasProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return HasSqliteProviderOwnedTriggers(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return HasSqlServerProviderOwnedTriggers(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return HasPostgresProviderOwnedTriggers(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return HasMySqlProviderOwnedTriggers(connection, schemaName, tableName);

            return false;
        }
    }
}
