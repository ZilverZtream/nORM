#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        public static IReadOnlyList<string> GetPostgresColumnNames(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);

    }
}
