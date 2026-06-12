using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        private static IReadOnlyList<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetTableSchema(connection, schemaName, tableName);

        private static string BuildSchemaProbeSql(
            DbConnection connection,
            string? schemaName,
            string tableName,
            string qualified,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
            => DynamicEntityTableSchemaReader.BuildSchemaProbeSql(
                connection,
                schemaName,
                tableName,
                qualified,
                postgresDomainColumnCastTypes);

        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetStringBinaryFacets(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetStringBinaryFacets(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecision> GetDecimalPrecisions(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetDecimalPrecisions(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> GetComputedColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetComputedColumns(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetSqliteDeclaredColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetSqliteDeclaredColumnTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetColumnStoreTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetColumnStoreTypes(connection, schemaName, tableName);

        private static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetIdentityColumns(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, int> GetPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetPrimaryKeyOrdinals(connection, schemaName, tableName);

        private static IReadOnlySet<string> GetRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetRowVersionColumns(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityTableSchemaReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        private static IReadOnlyList<string> GetPostgresColumnNames(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);

        private static (string? schema, string table) SplitSchema(string identifier)
            => DynamicEntitySchemaResolver.SplitSchema(identifier);

        private static (string? schema, string table, List<ColumnInfo> columns, bool isReadOnlyEntity) ResolveTableSchema(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.ResolveTableSchema(
                connection,
                tableName,
                static (cn, schema, table) => GetTableSchema(cn, schema, table).ToList(),
                static () => new List<ColumnInfo>(),
                IsReadOnlyDynamicObject);

        private static string? ResolveUniqueUnqualifiedSchema(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.ResolveUniqueUnqualifiedSchema(connection, tableName);

        private static IReadOnlyList<string> GetMatchingObjectSchemas(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.GetMatchingObjectSchemas(connection, tableName);

        private static IReadOnlyList<string> GetSqliteMatchingObjectSchemas(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.GetSqliteMatchingObjectSchemas(connection, tableName);

        private static IReadOnlyList<string> QuerySchemaNameList(DbConnection connection, string sql, string tableName)
            => DynamicEntitySchemaResolver.QuerySchemaNameList(connection, sql, tableName);

        private static bool TryGetTableSchema(DbConnection connection, string? schemaName, string tableName, out List<ColumnInfo> columns)
        {
            var found = DynamicEntitySchemaResolver.TryGetTableSchema(
                connection,
                schemaName,
                tableName,
                static (cn, schema, table) => GetTableSchema(cn, schema, table).ToList(),
                static () => new List<ColumnInfo>(),
                out var resolvedColumns);
            columns = resolvedColumns;
            return found;
        }

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => DynamicEntitySchemaResolver.ReaderHasColumn(reader, name);
    }
}
