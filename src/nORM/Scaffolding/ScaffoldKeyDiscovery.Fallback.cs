#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldKeyDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetProviderSchemaPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var result = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {IdentifierEscaping.EscapeTable(provider, table.Name, table.Schema)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var keyColumns = new List<string>();
                foreach (DataRow row in schema.Rows)
                {
                    // KeyInfo pads a view's schema with hidden base-table key columns (all flagged IsKey);
                    // treating them as the view's primary key would be wrong, so skip them.
                    if (ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(row))
                        continue;
                    if (row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool isKey && isKey)
                        keyColumns.Add(row["ColumnName"]!.ToString()!);
                }

                result[TableKey(table.Schema, table.Name)] = keyColumns;
            }

            return result;
        }
    }
}
