using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        private static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        private static IReadOnlyList<string> GetPostgresColumnNames(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);
    }
}
