#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteUnsupportedFeatureDiscovery
    {
        private static async Task AddTriggerFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            HashSet<string> tableKeys,
            ICollection<ScaffoldUnsupportedFeatureInfo> features)
        {
            foreach (var schema in await ScaffoldSkippedObjectDiscovery.GetSqliteSchemasAsync(connection).ConfigureAwait(false))
            {
                await using var triggerCmd = connection.CreateCommand();
                triggerCmd.CommandText = $"SELECT {ScaffoldSkippedObjectDiscovery.SqliteSchemaResult(schema)} AS TableSchema, tbl_name AS TableName, name AS TriggerName, sql AS TriggerSql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'trigger'";
                await using var triggerReader = await triggerCmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await triggerReader.ReadAsync().ConfigureAwait(false))
                {
                    var tableName = Convert.ToString(triggerReader["TableName"]);
                    var triggerName = Convert.ToString(triggerReader["TriggerName"]);
                    var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(triggerReader["TableSchema"])), tableName ?? string.Empty);
                    if (string.IsNullOrWhiteSpace(tableName) || !tableKeys.Contains(tableKey))
                        continue;

                    var triggerSql = NullIfWhiteSpace(Convert.ToString(triggerReader["TriggerSql"]));
                    var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["provider"] = "SQLite",
                        ["providerObjectKind"] = "Trigger",
                        ["table"] = tableKey,
                        ["triggerName"] = triggerName ?? string.Empty,
                        ["providerOwnedDdl"] = true,
                        ["generatedModelConfigurationSupported"] = false,
                        ["definitionAvailable"] = triggerSql is not null
                    };
                    if (triggerSql is not null)
                        metadata["triggerSql"] = triggerSql;

                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "Trigger", triggerName ?? string.Empty, "SQLite trigger")
                    {
                        Metadata = metadata
                    });
                }
            }
        }
    }
}
