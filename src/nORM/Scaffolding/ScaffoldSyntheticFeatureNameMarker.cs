#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSyntheticFeatureNameMarker
    {
        public static async Task MarkFeaturesAsync(
            DbConnection connection,
            IList<ScaffoldUnsupportedFeatureInfo> features,
            IReadOnlySet<string> tableKeys,
            string sql,
            string kind)
        {
            var syntheticNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var tableKey = TableKey(
                        NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])),
                        Convert.ToString(reader["TableName"]) ?? string.Empty);
                    var name = NullIfWhiteSpace(Convert.ToString(reader["ConstraintName"]));
                    if (name is not null && tableKeys.Contains(tableKey))
                        syntheticNames.Add(tableKey + "\u001f" + name);
                }
            }

            if (syntheticNames.Count == 0)
                return;

            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                if (!string.Equals(feature.Kind, kind, StringComparison.OrdinalIgnoreCase)
                    || !syntheticNames.Contains(feature.TableKey + "\u001f" + feature.Name))
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
