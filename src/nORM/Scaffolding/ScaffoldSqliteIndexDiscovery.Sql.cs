#nullable enable
using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteIndexDiscovery
    {
        private static async Task<string?> GetSqliteIndexFilterSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string indexName)
        {
            var sql = await GetSqliteIndexSqlAsync(connection, provider, schemaName, indexName).ConfigureAwait(false);
            return string.IsNullOrWhiteSpace(sql)
                ? null
                : ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(sql);
        }

        private static async Task<string?> GetSqliteIndexSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string indexName)
        {
            await using var cmd = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            cmd.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'index' AND name = @name";
            var p = cmd.CreateParameter();
            p.ParameterName = "@name";
            p.Value = indexName;
            cmd.Parameters.Add(p);
            return Convert.ToString(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
        }

        private static async Task<string?> GetSqliteCreateTableSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName)
        {
            await using var cmd = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            cmd.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name = @tableName";
            var p = cmd.CreateParameter();
            p.ParameterName = "@tableName";
            p.Value = tableName;
            cmd.Parameters.Add(p);
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
