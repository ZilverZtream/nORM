#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> GetSqliteForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var foreignKeys = new List<ScaffoldForeignKeyInfo>();
            foreach (var table in tables)
            {
                var providerSemanticsByColumns = ScaffoldSqliteDdlParser.ExtractForeignKeyProviderSemanticsByColumns(
                    await GetSqliteCreateTableSqlAsync(connection, provider, table).ConfigureAwait(false));
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "foreign_key_list", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                var rows = new List<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn, string OnUpdate, string OnDelete, string Match)>();
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    rows.Add((
                        Convert.ToInt64(reader["id"], CultureInfo.InvariantCulture),
                        Convert.ToInt64(reader["seq"], CultureInfo.InvariantCulture),
                        Convert.ToString(reader["table"]) ?? string.Empty,
                        Convert.ToString(reader["from"]) ?? string.Empty,
                        Convert.ToString(reader["to"]) ?? string.Empty,
                        Convert.ToString(reader["on_update"]) ?? string.Empty,
                        Convert.ToString(reader["on_delete"]) ?? string.Empty,
                        ReaderHasColumn(reader, "match") ? Convert.ToString(reader["match"]) ?? string.Empty : string.Empty));
                }

                foreach (var group in rows.GroupBy(static row => row.Id))
                    AddSqliteForeignKeyRows(table, group.OrderBy(static row => row.Seq).ToArray(), providerSemanticsByColumns, foreignKeys);
            }

            return foreignKeys;
        }

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

        private static void AddSqliteForeignKeyRows(
            ScaffoldTableInfo table,
            IReadOnlyList<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn, string OnUpdate, string OnDelete, string Match)> rows,
            IReadOnlyDictionary<string, string> providerSemanticsByColumns,
            ICollection<ScaffoldForeignKeyInfo> foreignKeys)
        {
            var providerSemantics = TryGetSqliteProviderSemantics(rows, providerSemanticsByColumns);
            foreach (var row in rows)
            {
                if (string.IsNullOrWhiteSpace(row.PrincipalTable)
                    || string.IsNullOrWhiteSpace(row.DependentColumn)
                    || string.IsNullOrWhiteSpace(row.PrincipalColumn))
                {
                    continue;
                }

                foreignKeys.Add(new ScaffoldForeignKeyInfo(
                    DependentSchema: table.Schema,
                    DependentTable: table.Name,
                    DependentColumn: row.DependentColumn,
                    PrincipalSchema: table.Schema,
                    PrincipalTable: row.PrincipalTable,
                    PrincipalColumn: row.PrincipalColumn,
                    ConstraintName: "sqlite_fk_" + row.Id,
                    ColumnCount: rows.Count,
                    OnDelete: ScaffoldReferentialAction.Normalize(row.OnDelete),
                    OnUpdate: NormalizeSqliteReferentialAction(row.OnUpdate, row.Match, providerSemantics),
                    IsSyntheticConstraintName: true));
            }
        }

        private static string? TryGetSqliteProviderSemantics(
            IReadOnlyList<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn, string OnUpdate, string OnDelete, string Match)> rows,
            IReadOnlyDictionary<string, string> providerSemanticsByColumns)
        {
            var columnKey = ScaffoldSqliteDdlParser.BuildForeignKeyColumnKey(rows.Select(static row => row.DependentColumn));
            return providerSemanticsByColumns.TryGetValue(columnKey, out var semantics) ? semantics : null;
        }

        private static string NormalizeSqliteReferentialAction(string action, string match, string? providerSemantics)
        {
            var normalized = ScaffoldReferentialAction.Normalize(action);
            var normalizedSemantics = string.IsNullOrWhiteSpace(providerSemantics)
                ? NormalizeSqliteMatch(match)
                : providerSemantics;
            return normalizedSemantics is null ? normalized : normalized + " " + normalizedSemantics;
        }

        private static string? NormalizeSqliteMatch(string? match)
        {
            if (string.IsNullOrWhiteSpace(match))
                return null;

            var normalized = match.Replace('_', ' ').Trim().ToUpperInvariant();
            return normalized is "NONE" or "SIMPLE" ? null : "MATCH " + normalized;
        }
    }
}
