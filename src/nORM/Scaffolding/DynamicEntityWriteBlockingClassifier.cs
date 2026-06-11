#nullable enable
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityWriteBlockingClassifier
    {
        public static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return HasWriteBlockingSqliteColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return HasWriteBlockingSqlServerColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return HasWriteBlockingPostgresColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return HasWriteBlockingMySqlColumns(connection, schemaName, tableName);

            return false;
        }
    }
}
