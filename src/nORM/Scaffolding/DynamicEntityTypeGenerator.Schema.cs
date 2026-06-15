using System.Collections.Generic;
using System.Data.Common;

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
    }
}
