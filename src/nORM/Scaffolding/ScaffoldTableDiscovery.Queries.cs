#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldTableInfo>> QueryTablesAsync(DbConnection connection, string sql)
        {
            var tables = new List<ScaffoldTableInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = reader.IsDBNull(0) ? null : reader.GetString(0);
                var table = reader.GetString(1);
                tables.Add(new ScaffoldTableInfo(table, string.IsNullOrWhiteSpace(schema) ? null : schema));
            }

            return SortTables(tables);
        }

        private static IReadOnlyList<ScaffoldTableInfo> SortTables(IEnumerable<ScaffoldTableInfo> tables)
            => tables
                .OrderBy(t => t.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(t => t.Name, StringComparer.Ordinal)
                .ToArray();
    }
}
