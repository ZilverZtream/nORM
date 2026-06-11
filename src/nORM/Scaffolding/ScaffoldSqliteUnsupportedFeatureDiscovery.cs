#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            var sqliteCreateSqlByTable = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

            foreach (var table in tables)
                await AddColumnFeaturesAsync(connection, provider, table, tableKeys, sqliteCreateSqlByTable, features).ConfigureAwait(false);

            foreach (var table in tables)
                AddCreateTableSqlFeatures(table, sqliteCreateSqlByTable, features);

            await AddTriggerFeaturesAsync(connection, provider, tableKeys, features).ConfigureAwait(false);

            foreach (var table in tables)
                await AddIndexFeaturesAsync(connection, provider, table, features).ConfigureAwait(false);

            return features;
        }
    }
}
