#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteUnsupportedFeatureDiscovery
    {
        private static async Task AddIndexFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            ICollection<ScaffoldUnsupportedFeatureInfo> features)
        {
            await using var listCommand = connection.CreateCommand();
            listCommand.CommandText = SqlitePragma(provider, table.Schema, "index_list", table.Name);
            await using var listReader = await listCommand.ExecuteReaderAsync().ConfigureAwait(false);
            var indexRows = new List<(string Name, bool IsPartial, string Origin)>();
            while (await listReader.ReadAsync().ConfigureAwait(false))
            {
                var indexName = Convert.ToString(listReader["name"]);
                if (string.IsNullOrWhiteSpace(indexName))
                    continue;

                indexRows.Add((
                    indexName,
                    ReaderHasColumn(listReader, "partial")
                        && Convert.ToInt32(listReader["partial"], CultureInfo.InvariantCulture) != 0,
                    Convert.ToString(listReader["origin"]) ?? string.Empty));
            }

            foreach (var (indexName, isPartial, origin) in indexRows)
            {
                if (string.Equals(origin, "pk", StringComparison.OrdinalIgnoreCase))
                    continue;

                await AddIndexFeatureAsync(connection, provider, table, indexName, isPartial, features).ConfigureAwait(false);
            }
        }

        private static async Task AddIndexFeatureAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            string indexName,
            bool isPartial,
            ICollection<ScaffoldUnsupportedFeatureInfo> features)
        {
            var tableKey = TableKey(table.Schema, table.Name);
            string? indexSql = null;
            if (isPartial)
            {
                indexSql = await GetIndexSqlAsync(connection, provider, table.Schema, indexName).ConfigureAwait(false);
                features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "PartialIndex", indexName, indexSql ?? "SQLite partial index"));
            }

            await using var infoCommand = connection.CreateCommand();
            infoCommand.CommandText = SqlitePragma(provider, table.Schema, "index_xinfo", indexName);
            await using var infoReader = await infoCommand.ExecuteReaderAsync().ConfigureAwait(false);
            var reportedDescending = false;
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
                    indexSql ??= await GetIndexSqlAsync(connection, provider, table.Schema, indexName).ConfigureAwait(false);
                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "ExpressionIndex", indexName, indexSql ?? "SQLite expression index"));
                    if (!reportedDescending
                        && ReaderHasColumn(infoReader, "desc")
                        && Convert.ToInt32(infoReader["desc"], CultureInfo.InvariantCulture) != 0)
                    {
                        features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "DescendingIndex", indexName, indexSql ?? "SQLite descending expression index key"));
                        reportedDescending = true;
                    }

                    break;
                }

                if (!reportedDescending
                    && ReaderHasColumn(infoReader, "desc")
                    && Convert.ToInt32(infoReader["desc"], CultureInfo.InvariantCulture) != 0)
                {
                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "DescendingIndex", indexName, "SQLite descending index key"));
                    reportedDescending = true;
                }
            }
        }
    }
}
