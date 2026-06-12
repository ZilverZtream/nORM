#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private static async Task MarkNamedDefaultConstraintFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
        {
            var names = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SqlServerNamedDefaultConstraintSql;
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var tableKey = TableKey(
                        NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])),
                        Convert.ToString(reader["TableName"]) ?? string.Empty);
                    if (!tableKeys.Contains(tableKey))
                        continue;

                    var columnName = NullIfWhiteSpace(Convert.ToString(reader["ColumnName"]));
                    var constraintName = NullIfWhiteSpace(Convert.ToString(reader["ConstraintName"]));
                    if (columnName is not null && constraintName is not null)
                        names[tableKey + "\u001f" + columnName] = constraintName;
                }
            }

            if (names.Count == 0)
                return;

            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                if (!string.Equals(feature.Kind, "Default", StringComparison.OrdinalIgnoreCase)
                    || !names.TryGetValue(feature.TableKey + "\u001f" + feature.Name, out var constraintName))
                {
                    continue;
                }

                var metadata = feature.Metadata is null
                    ? new Dictionary<string, object?>(StringComparer.Ordinal)
                    : new Dictionary<string, object?>(feature.Metadata, StringComparer.Ordinal);
                metadata["defaultConstraintName"] = constraintName;
                features[i] = feature with { Metadata = metadata };
            }
        }
    }
}
