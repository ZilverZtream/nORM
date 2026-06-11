using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private static async Task MarkSystemNamedCheckConstraintFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
        {
            var systemNamedChecks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SqlServerSystemNamedCheckConstraintSql;
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var tableKey = TableKey(
                        NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])),
                        Convert.ToString(reader["TableName"]) ?? string.Empty);
                    var constraintName = NullIfWhiteSpace(Convert.ToString(reader["ConstraintName"]));
                    if (constraintName is not null && tableKeys.Contains(tableKey))
                        systemNamedChecks.Add(tableKey + "\u001f" + constraintName);
                }
            }

            if (systemNamedChecks.Count == 0)
                return;

            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                if (!string.Equals(feature.Kind, "CheckConstraint", StringComparison.OrdinalIgnoreCase)
                    || !systemNamedChecks.Contains(feature.TableKey + "\u001f" + feature.Name))
                {
                    continue;
                }

                var metadata = feature.Metadata is null
                    ? new Dictionary<string, object?>(StringComparer.Ordinal)
                    : new Dictionary<string, object?>(feature.Metadata, StringComparer.Ordinal);
                metadata["isSyntheticName"] = true;
                features[i] = feature with { Metadata = metadata };
            }
        }
    }
}
