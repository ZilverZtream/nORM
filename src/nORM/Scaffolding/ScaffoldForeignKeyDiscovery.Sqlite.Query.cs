#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private readonly record struct SqliteForeignKeyRow(
            long Id,
            long Seq,
            string PrincipalTable,
            string DependentColumn,
            string PrincipalColumn,
            string OnUpdate,
            string OnDelete,
            string Match);

        private static async Task<string?> GetSqliteCreateTableSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table)
        {
            await using var command = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(table.Schema) ? "main" : table.Schema!;
            command.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name = @tableName";
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = table.Name;
            command.Parameters.Add(parameter);
            return await command.ExecuteScalarAsync().ConfigureAwait(false) as string;
        }

        private static async Task<IReadOnlyList<SqliteForeignKeyRow>> ReadSqliteForeignKeyRowsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(provider, table.Schema, "foreign_key_list", table.Name);
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            var rows = new List<SqliteForeignKeyRow>();
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                rows.Add(new SqliteForeignKeyRow(
                    Convert.ToInt64(reader["id"], CultureInfo.InvariantCulture),
                    Convert.ToInt64(reader["seq"], CultureInfo.InvariantCulture),
                    Convert.ToString(reader["table"]) ?? string.Empty,
                    Convert.ToString(reader["from"]) ?? string.Empty,
                    Convert.ToString(reader["to"]) ?? string.Empty,
                    Convert.ToString(reader["on_update"]) ?? string.Empty,
                    Convert.ToString(reader["on_delete"]) ?? string.Empty,
                    ReaderHasColumn(reader, "match") ? Convert.ToString(reader["match"]) ?? string.Empty : string.Empty));
            }

            return rows;
        }
    }
}
