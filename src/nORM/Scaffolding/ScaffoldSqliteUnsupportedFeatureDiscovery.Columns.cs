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
                if (ScaffoldSqliteDdlParser.TryParseDeclaredDecimalPrecision(declaredType, out _))
                    features.Add(new ScaffoldUnsupportedFeatureInfo(tableKey, "PrecisionScale", name, declaredType!));
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
    }
}
