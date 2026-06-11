#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        public static string BuildSchemaProbeSql(
            DbConnection connection,
            string? schemaName,
            string tableName,
            string qualified,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
        {
            if (!DynamicEntityConnectionKind.IsPostgres(connection) || postgresDomainColumnCastTypes.Count == 0)
                return $"SELECT * FROM {qualified} WHERE 1=0";

            var columnNames = DynamicEntitySchemaMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);
            if (columnNames.Count == 0)
                return $"SELECT * FROM {qualified} WHERE 1=0";

            var projection = columnNames.Select(column =>
            {
                var escaped = DynamicEntityConnectionKind.EscapeIdentifier(connection, column);
                return postgresDomainColumnCastTypes.TryGetValue(column, out var castType)
                    ? $"{escaped}::{castType} AS {escaped}"
                    : escaped;
            });

            return $"SELECT {string.Join(", ", projection)} FROM {qualified} WHERE 1=0";
        }
    }
}
