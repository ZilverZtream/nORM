#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetComputedColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.GetComputedColumns(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetSqliteDeclaredColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetSqliteDeclaredColumnTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetColumnStoreTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetColumnStoreTypes(connection, schemaName, tableName);

        public static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetIdentityColumns(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, int> GetPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPrimaryKeyOrdinals(connection, schemaName, tableName);

        public static IReadOnlySet<string> GetRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetRowVersionColumns(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        private static DynamicColumnMetadata ReadColumnMetadata(
            DbConnection connection,
            string? schemaName,
            string tableName,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
            => new(
                postgresDomainColumnCastTypes,
                GetComputedColumns(connection, schemaName, tableName),
                GetIdentityColumns(connection, schemaName, tableName),
                GetRowVersionColumns(connection, schemaName, tableName),
                GetSqliteDeclaredColumnTypes(connection, schemaName, tableName),
                GetColumnStoreTypes(connection, schemaName, tableName),
                GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName),
                GetMySqlUnsignedColumnTypes(connection, schemaName, tableName),
                GetDecimalPrecisions(connection, schemaName, tableName),
                GetStringBinaryFacets(connection, schemaName, tableName),
                GetPrimaryKeyOrdinals(connection, schemaName, tableName));
    }
}
