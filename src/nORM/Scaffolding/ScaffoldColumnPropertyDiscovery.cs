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
    internal static class ScaffoldColumnPropertyDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetColumnPropertyNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            bool useDatabaseNames)
        {
            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var orderedColumns = await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName, ordinal_position AS Ordinal
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    """).ConfigureAwait(false);
                return ScaffoldColumnPropertyNameBuilder.BuildColumnPropertyNameMap(orderedColumns, entityByTable, useDatabaseNames);
            }

            var result = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {IdentifierEscaping.EscapeTable(provider, table.Name, table.Schema)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var tableKey = TableKey(table.Schema, table.Name);
                var existingNames = ScaffoldColumnPropertyNameBuilder.CreateReservedMemberNameSet();
                if (entityByTable.TryGetValue(tableKey, out var entityName))
                    existingNames.Add(entityName);
                result[tableKey] = ScaffoldColumnPropertyNameBuilder.BuildColumnPropertyNames(
                    schema.Rows.Cast<DataRow>()
                        .Where(row => !ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(row))
                        .Select(row => row["ColumnName"]!.ToString()!),
                    existingNames,
                    useDatabaseNames);
            }

            return result;
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> QueryOrderedColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = tableKeys.ToDictionary(
                key => key,
                _ => new List<(int Ordinal, string Column)>(),
                StringComparer.OrdinalIgnoreCase);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                var ordinal = ReaderHasColumn(reader, "Ordinal")
                    ? Convert.ToInt32(reader["Ordinal"], System.Globalization.CultureInfo.InvariantCulture)
                    : result[tableKey].Count + 1;
                result[tableKey].Add((ordinal, columnName));
            }

            return ToOrderedColumnDictionary(result);
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> ToOrderedColumnDictionary(
            Dictionary<string, List<(int Ordinal, string Column)>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyList<string>)pair.Value
                    .OrderBy(item => item.Ordinal)
                    .Select(item => item.Column)
                    .ToArray(),
                StringComparer.OrdinalIgnoreCase);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static string TableKey(string? schema, string table)
            => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
