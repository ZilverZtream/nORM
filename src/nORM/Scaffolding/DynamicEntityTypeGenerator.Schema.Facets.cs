using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
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
    }
}
