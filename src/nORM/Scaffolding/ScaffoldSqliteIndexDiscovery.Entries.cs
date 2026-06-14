#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteIndexDiscovery
    {
        private static async Task AddSqliteIndexAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            string name,
            bool isUnique,
            string origin,
            bool isPartial,
            ICollection<ScaffoldIndexInfo> indexes)
        {
            var filterSql = isPartial
                ? await GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name).ConfigureAwait(false)
                : null;

            await using var infoCommand = connection.CreateCommand();
            infoCommand.CommandText = SqlitePragma(provider, table.Schema, "index_xinfo", name);
            await using var infoReader = await infoCommand.ExecuteReaderAsync().ConfigureAwait(false);

            var columns = new List<SqliteIndexColumnRow>();
            var hasUnsupportedKeyPart = false;
            while (await infoReader.ReadAsync().ConfigureAwait(false))
            {
                if (ReaderHasColumn(infoReader, "key")
                    && Convert.ToInt32(infoReader["key"], CultureInfo.InvariantCulture) == 0)
                {
                    continue;
                }

                if (ReaderHasColumn(infoReader, "cid")
                    && Convert.ToInt32(infoReader["cid"], CultureInfo.InvariantCulture) < 0)
                {
                    hasUnsupportedKeyPart = true;
                    continue;
                }

                var columnName = Convert.ToString(infoReader["name"]);
                if (!string.IsNullOrWhiteSpace(columnName))
                {
                    columns.Add(new SqliteIndexColumnRow(
                        Convert.ToInt32(infoReader["seqno"], CultureInfo.InvariantCulture),
                        columnName,
                        ReaderHasColumn(infoReader, "desc")
                            && Convert.ToInt32(infoReader["desc"], CultureInfo.InvariantCulture) != 0));
                }
            }

            if (hasUnsupportedKeyPart)
                return;

            var isSyntheticName = string.Equals(origin, "u", StringComparison.OrdinalIgnoreCase)
                                  || name.StartsWith("sqlite_autoindex_", StringComparison.OrdinalIgnoreCase);
            foreach (var column in columns.OrderBy(static c => c.Ordinal))
            {
                indexes.Add(new ScaffoldIndexInfo(
                    TableKey(table.Schema, table.Name),
                    column.Name,
                    name,
                    isUnique,
                    columns.Count,
                    column.Ordinal,
                    column.IsDescending,
                    false,
                    IndexNullSortOrder.Default,
                    false,
                    filterSql,
                    isSyntheticName));
            }
        }
    }
}
