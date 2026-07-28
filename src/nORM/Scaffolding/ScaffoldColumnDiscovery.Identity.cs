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
                return await GetSqliteIdentityColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);

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

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetSqliteIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
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
                if (!string.Equals(key.Type.Trim(), "INTEGER", StringComparison.OrdinalIgnoreCase))
                    continue;

                var createSql = await GetSqliteTableCreateSqlAsync(connection, provider, table.Schema, table.Name).ConfigureAwait(false);
                // A rowid table aliases its INTEGER PK to the store-generated rowid, EXCEPT:
                // - WITHOUT ROWID tables have no rowid, so even an INTEGER PK is app-assigned; and
                // - a column-constraint `INTEGER PRIMARY KEY DESC` disables aliasing (SQLite docs), so that
                //   key is app-assigned too. Both would otherwise scaffold as an identity, and nORM would
                //   then omit the column from INSERTs and read back a NULL it never assigned.
                var isWithoutRowid = createSql != null
                    && createSql.IndexOf("WITHOUT ROWID", StringComparison.OrdinalIgnoreCase) >= 0;
                var isDescColumnPk = ScaffoldSqliteDdlParser.IsIntegerPrimaryKeyDescColumn(createSql, key.Name);
                if (!isWithoutRowid && !isDescColumnPk)
                    result[TableKey(table.Schema, table.Name)] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { key.Name };
            }

            return ToReadOnlySetDictionary(result);
        }

        // Returns the stored CREATE TABLE statement for a SQLite table. SQLite exposes no pragma for the
        // table-level WITHOUT ROWID option or a column's key direction, so both are read off this DDL.
        private static async Task<string?> GetSqliteTableCreateSqlAsync(DbConnection connection, DatabaseProvider provider, string? schema, string tableName)
        {
            var prefix = string.IsNullOrWhiteSpace(schema) ? string.Empty : provider.Escape(schema!) + ".";
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT sql FROM {prefix}sqlite_master WHERE type='table' AND name = $name";
            var p = cmd.CreateParameter();
            p.ParameterName = "$name";
            p.Value = tableName;
            cmd.Parameters.Add(p);
            return Convert.ToString(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
        }
    }
}
