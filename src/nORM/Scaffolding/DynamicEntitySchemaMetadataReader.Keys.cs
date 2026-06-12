#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteIdentityColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return GetSqlServerIdentityColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return GetPostgresIdentityColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return GetMySqlIdentityColumns(connection, schemaName, tableName);

            return EmptyColumnNameSet();
        }

        public static IReadOnlyDictionary<string, int> GetPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqlitePrimaryKeyOrdinals(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return GetSqlServerPrimaryKeyOrdinals(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return GetPostgresPrimaryKeyOrdinals(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return GetMySqlPrimaryKeyOrdinals(connection, schemaName, tableName);

            return EmptyColumnOrdinalMap();
        }

        public static IReadOnlySet<string> GetRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityConnectionKind.IsSqlServer(connection)
                ? GetSqlServerRowVersionColumns(connection, schemaName, tableName)
                : EmptyColumnNameSet();

        private static IReadOnlySet<string> EmptyColumnNameSet()
            => new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, int> EmptyColumnOrdinalMap()
            => new Dictionary<string, int>(0, StringComparer.OrdinalIgnoreCase);
    }
}
