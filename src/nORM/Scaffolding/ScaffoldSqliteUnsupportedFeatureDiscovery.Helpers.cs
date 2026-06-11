#nullable enable
using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteUnsupportedFeatureDiscovery
    {
        private static async Task<string?> GetCreateTableSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table)
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

        private static async Task<string?> GetIndexSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string indexName)
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

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }
    }
}
