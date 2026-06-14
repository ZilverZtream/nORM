#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteIndexDiscovery
    {
        private static async Task<IReadOnlyList<SqliteIndexCatalogRow>> GetSqliteTableIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table)
        {
            await using var listCommand = connection.CreateCommand();
            listCommand.CommandText = SqlitePragma(provider, table.Schema, "index_list", table.Name);
            await using var listReader = await listCommand.ExecuteReaderAsync().ConfigureAwait(false);

            var tableIndexes = new List<SqliteIndexCatalogRow>();
            while (await listReader.ReadAsync().ConfigureAwait(false))
            {
                var name = Convert.ToString(listReader["name"]);
                if (string.IsNullOrWhiteSpace(name))
                    continue;

                tableIndexes.Add(new SqliteIndexCatalogRow(
                    name,
                    Convert.ToInt32(listReader["unique"], CultureInfo.InvariantCulture) != 0,
                    Convert.ToString(listReader["origin"]) ?? string.Empty,
                    ReaderHasColumn(listReader, "partial")
                        && Convert.ToInt32(listReader["partial"], CultureInfo.InvariantCulture) != 0));
            }

            return tableIndexes;
        }
    }
}
