#nullable enable
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityWriteBlockingClassifier
    {
        public static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
                return HasWriteBlockingSqliteColumns(connection, schemaName, tableName);

            if (IsSqlServerConnection(connectionName))
                return HasWriteBlockingSqlServerColumns(connection, schemaName, tableName);

            if (IsPostgresConnection(connectionName))
                return HasWriteBlockingPostgresColumns(connection, schemaName, tableName);

            if (IsMySqlConnection(connectionName))
                return HasWriteBlockingMySqlColumns(connection, schemaName, tableName);

            return false;
        }
    }
}
