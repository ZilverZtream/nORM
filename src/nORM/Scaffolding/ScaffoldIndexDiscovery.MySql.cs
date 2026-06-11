#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetMySqlIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var indexes = await QueryIndexesAsync(connection, MySqlIndexSql).ConfigureAwait(false);
            var expressionIndexKeys = (await ScaffoldMySqlUnsupportedFeatureDiscovery.GetExpressionIndexFeaturesAsync(connection, provider, tables).ConfigureAwait(false))
                .Select(static feature => feature.TableKey + "\u001f" + feature.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (expressionIndexKeys.Count == 0)
                return indexes;

            return indexes
                .Where(index => !expressionIndexKeys.Contains(index.TableKey + "\u001f" + index.IndexName))
                .ToArray();
        }
    }
}
