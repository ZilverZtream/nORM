#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetSqliteColumnStoreTypesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var result = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var name = Convert.ToString(reader["name"]);
                    var type = Convert.ToString(reader["type"]);
                    if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(type))
                        columns[name!] = type!;
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
        }
    }
}
