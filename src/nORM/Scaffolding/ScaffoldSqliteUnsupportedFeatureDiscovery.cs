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
    internal static class ScaffoldSqliteUnsupportedFeatureDiscovery
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

        private static async Task AddColumnFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            HashSet<string> tableKeys,
            IDictionary<string, string?> sqliteCreateSqlByTable,
            ICollection<ScaffoldUnsupportedFeatureInfo> features)
        {
            var tableKey = TableKey(table.Schema, table.Name);
            var createSql = await GetCreateTableSqlAsync(connection, provider, table).ConfigureAwait(false);
            sqliteCreateSqlByTable[tableKey] = createSql;
            var generatedColumns = ScaffoldSqliteDdlParser.ExtractGeneratedColumns(createSql);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var name = Convert.ToString(reader["name"]) ?? string.Empty;
                var declaredType = Convert.ToString(reader["type"]);
                var defaultValue = Convert.ToString(reader["dflt_value"]);
                var hidden = Convert.ToInt32(reader["hidden"], CultureInfo.InvariantCulture);
                if (!string.IsNullOrWhiteSpace(defaultValue))
                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "Default", name, defaultValue));
                if (hidden is 2 or 3)
                {
                    var detail = generatedColumns.TryGetValue(name, out var generated)
                        ? generated.Sql + (generated.Stored ? " STORED" : " VIRTUAL")
                        : "SQLite generated column";
                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "Computed", name, detail));
                }
                if (ScaffoldSqliteDdlParser.IsProviderSpecificDeclaredType(declaredType))
                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "ProviderSpecificColumnType", name, declaredType!));
            }
        }

        private static void AddCreateTableSqlFeatures(
            ScaffoldTableInfo table,
            IReadOnlyDictionary<string, string?> sqliteCreateSqlByTable,
            ICollection<ScaffoldUnsupportedFeatureInfo> features)
        {
            var tableKey = TableKey(table.Schema, table.Name);
            sqliteCreateSqlByTable.TryGetValue(tableKey, out var createSql);
            foreach (var check in ScaffoldSqliteDdlParser.ExtractCheckConstraints(table.Name, createSql))
                features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "CheckConstraint", check.Name, check.Sql));

            foreach (var (columnName, collation) in ScaffoldSqliteDdlParser.ExtractColumnCollations(createSql))
                features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "Collation", columnName, collation));
        }

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

        private static async Task<string?> GetCreateTableSqlAsync(DbConnection connection, DatabaseProvider provider, ScaffoldTableInfo table)
        {
            await using var command = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(table.Schema) ? "main" : table.Schema!;
            command.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name = @tableName";
            var tableNameParameter = command.CreateParameter();
            tableNameParameter.ParameterName = "@tableName";
            tableNameParameter.Value = table.Name;
            command.Parameters.Add(tableNameParameter);
            return Convert.ToString(await command.ExecuteScalarAsync().ConfigureAwait(false));
        }

        private static async Task<string?> GetIndexSqlAsync(DbConnection connection, DatabaseProvider provider, string? schemaName, string indexName)
        {
            await using var cmd = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            cmd.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'index' AND name = @indexName";
            var parameter = cmd.CreateParameter();
            parameter.ParameterName = "@indexName";
            parameter.Value = indexName;
            cmd.Parameters.Add(parameter);
            return Convert.ToString(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
        }

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
