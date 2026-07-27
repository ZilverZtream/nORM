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
    internal static partial class ScaffoldColumnDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlite(provider))
            {
                var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        rows.Add((
                            Convert.ToString(reader["name"]) ?? string.Empty,
                            Convert.ToString(reader["type"]) ?? string.Empty,
                            ReaderHasColumn(reader, "pk")
                                ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                                : 0));
                    }

                    var primaryKeyColumns = rows.Where(row => row.PrimaryKeyOrdinal > 0).ToArray();
                    if (primaryKeyColumns.Length != 1)
                        continue;

                    var key = primaryKeyColumns[0];
                    // Only an EXACTLY-INTEGER single-column PK aliases the store-generated rowid; BIGINT /
                    // INT / SMALLINT / etc. are app-assigned despite their INTEGER affinity. Contains("INT")
                    // wrongly flagged them, emitting [DatabaseGenerated(Identity)] on an app-assigned key.
                    // And only in a rowid table — a WITHOUT ROWID table has no rowid, so even an INTEGER PK
                    // is app-assigned there.
                    if (string.Equals(key.Type.Trim(), "INTEGER", StringComparison.OrdinalIgnoreCase)
                        && !await IsSqliteWithoutRowidTableAsync(connection, provider, table.Schema, table.Name).ConfigureAwait(false))
                    {
                        var tableKey = TableKey(table.Schema, table.Name);
                        result[tableKey] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { key.Name };
                    }
                }

                return ToReadOnlySetDictionary(result);
            }

            if (ScaffoldProviderKind.IsSqlServer(provider))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName
                    FROM sys.identity_columns ic
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    INNER JOIN sys.tables t ON t.object_id = ic.object_id
                    WHERE t.is_ms_shipped = 0
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns c
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (
                          is_identity = 'YES'
                          OR (
                              column_default LIKE 'nextval(%'
                              AND pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL
                          )
                      )
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsMySql(provider))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND LOWER(extra) LIKE '%auto_increment%'
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        // True when the SQLite table was created WITHOUT ROWID (so even an INTEGER PRIMARY KEY is
        // app-assigned, not a store-generated rowid alias). SQLite exposes no pragma for this, so match the
        // table-level option on the stored CREATE statement.
        private static async Task<bool> IsSqliteWithoutRowidTableAsync(DbConnection connection, DatabaseProvider provider, string? schema, string tableName)
        {
            var prefix = string.IsNullOrWhiteSpace(schema) ? string.Empty : provider.Escape(schema!) + ".";
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT sql FROM {prefix}sqlite_master WHERE type='table' AND name = $name";
            var p = cmd.CreateParameter();
            p.ParameterName = "$name";
            p.Value = tableName;
            cmd.Parameters.Add(p);
            var sql = Convert.ToString(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
            return sql != null && sql.IndexOf("WITHOUT ROWID", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}
