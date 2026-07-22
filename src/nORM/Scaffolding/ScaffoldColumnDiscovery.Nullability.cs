#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetNonNullableColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            if (ScaffoldProviderKind.IsSqlite(provider))
            {
                var sqliteResult = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var tableColumns = new List<(string ColumnName, bool NotNull, int PrimaryKeyOrdinal)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        if (!ReaderHasColumn(reader, "name"))
                            continue;

                        var columnName = Convert.ToString(reader["name"]);
                        if (string.IsNullOrWhiteSpace(columnName))
                            continue;

                        var notNull = ReaderHasColumn(reader, "notnull")
                            && Convert.ToInt32(reader["notnull"], CultureInfo.InvariantCulture) != 0;
                        var primaryKeyOrdinal = ReaderHasColumn(reader, "pk")
                            ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                            : 0;
                        tableColumns.Add((columnName, notNull, primaryKeyOrdinal));
                    }

                    var primaryKeyColumnCount = tableColumns.Count(static column => column.PrimaryKeyOrdinal > 0);
                    foreach (var column in tableColumns)
                    {
                        if (column.NotNull
                            || (column.PrimaryKeyOrdinal > 0 && primaryKeyColumnCount == 1))
                        {
                            columns.Add(column.ColumnName);
                        }
                    }

                    sqliteResult[TableKey(table.Schema, table.Name)] = columns;
                }

                return sqliteResult;
            }

            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND is_nullable = 'NO'
                    """).ConfigureAwait(false);
            }

            var result = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {IdentifierEscaping.EscapeTable(provider, table.Name, table.Schema)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (DataRow row in schema.Rows)
                {
                    // Skip KeyInfo-padded hidden columns (base-table keys surfaced for a view): they are
                    // non-nullable and would otherwise mark a visible view column of the same name as
                    // non-nullable, mistyping a nullable (e.g. LEFT JOIN) column.
                    if (ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(row))
                        continue;
                    var columnName = row["ColumnName"]!.ToString()!;
                    var allowNull = row["AllowDBNull"] is bool b && b;
                    if (!allowNull)
                        columns.Add(columnName);
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
        }
    }
}
