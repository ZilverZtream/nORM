#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static readonly IReadOnlySet<string> EmptyStringSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public static string BuildSchemaProbeSql(
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            IReadOnlyDictionary<string, string>? columnPropertyNames,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlySet<string>? postgresTextCastColumns = null)
        {
            var qualified = IdentifierEscaping.EscapeTable(provider, tableName, schemaName);
            if (!provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase)
                || ((providerSpecificColumnTypes is null
                     || providerSpecificColumnTypes.Count == 0
                     || !providerSpecificColumnTypes.Values.Any(ScaffoldProviderSpecificTypeClassifier.RequiresPostgresSchemaProbeCast))
                    && (postgresTextCastColumns is null || postgresTextCastColumns.Count == 0))
                || columnPropertyNames is null
                || columnPropertyNames.Count == 0)
            {
                return $"SELECT * FROM {qualified} WHERE 1=0";
            }

            var columns = columnPropertyNames.Keys.Select(column =>
            {
                var escaped = provider.Escape(column);
                return providerSpecificColumnTypes is not null
                       && providerSpecificColumnTypes.TryGetValue(column, out var detail)
                       && ScaffoldProviderSpecificTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out var castType)
                    ? $"{escaped}::{castType} AS {escaped}"
                    : postgresTextCastColumns is not null && postgresTextCastColumns.Contains(column)
                        ? $"{escaped}::text AS {escaped}"
                        : escaped;
            });

            return $"SELECT {string.Join(", ", columns)} FROM {qualified} WHERE 1=0";
        }

        public static async Task<IReadOnlySet<string>> GetPostgresUserDefinedColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName)
        {
            if (!provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return EmptyStringSet;

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND domain_name IS NULL
                  AND data_type = 'USER-DEFINED'
                """;
            AddParameter(cmd, "@tableName", tableName);
            AddParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? DBNull.Value : schemaName);
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var columnName = Convert.ToString(reader["column_name"]);
                if (!string.IsNullOrWhiteSpace(columnName))
                    result.Add(columnName);
            }

            return result.Count == 0 ? EmptyStringSet : result;
        }

        private static void AddParameter(DbCommand command, string name, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }
    }
}
