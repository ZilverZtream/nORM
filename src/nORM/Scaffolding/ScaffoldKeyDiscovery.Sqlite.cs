#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldKeyDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetSqlitePrimaryKeyColumnNameMapAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var sqliteResult = new Dictionary<string, List<(int Ordinal, string Column)>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                var keyColumns = new List<(int Ordinal, string Column)>();
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    if (!ReaderHasColumn(reader, "name")
                        || !ReaderHasColumn(reader, "pk"))
                    {
                        continue;
                    }

                    var ordinal = Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture);
                    if (ordinal <= 0)
                        continue;

                    var name = Convert.ToString(reader["name"]);
                    if (!string.IsNullOrWhiteSpace(name))
                        keyColumns.Add((ordinal, name));
                }

                sqliteResult[TableKey(table.Schema, table.Name)] = keyColumns;
            }

            return ToOrderedColumnDictionary(sqliteResult);
        }
    }
}
