#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldTableInfo>> GetSchemaTablesAsync(DbConnection connection)
        {
            var schema = await connection.GetSchemaAsync("Tables").ConfigureAwait(false);
            var tables = new List<ScaffoldTableInfo>();
            foreach (DataRow row in schema.Rows)
            {
                var tableType = row.Table.Columns.Contains("TABLE_TYPE") ? row["TABLE_TYPE"]?.ToString() : null;
                if (tableType != null && !string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase))
                    continue;

                var tableName = row["TABLE_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(tableName))
                    continue;

                var schemaName = row.Table.Columns.Contains("TABLE_SCHEMA")
                    ? row["TABLE_SCHEMA"]?.ToString()
                    : null;

                tables.Add(new ScaffoldTableInfo(tableName, string.IsNullOrWhiteSpace(schemaName) ? null : schemaName));
            }

            return SortTables(tables);
        }
    }
}
