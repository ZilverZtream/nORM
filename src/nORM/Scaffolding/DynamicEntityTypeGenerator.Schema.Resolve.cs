using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
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
