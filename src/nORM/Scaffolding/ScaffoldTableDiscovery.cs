#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldTableDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldTableInfo>> GetTablesAsync(
            DbConnection connection,
            DatabaseProvider provider)
        {
            if (ScaffoldProviderKind.IsSqlite(provider))
            {
                var tables = new List<ScaffoldTableInfo>();
                foreach (var schema in await ScaffoldSkippedObjectDiscovery.GetSqliteSchemasAsync(connection).ConfigureAwait(false))
                {
                    tables.AddRange(await QueryTablesAsync(
                        connection,
                        $"""
                        SELECT {ScaffoldSkippedObjectDiscovery.SqliteSchemaResult(schema)} AS TABLE_SCHEMA, m.name AS TABLE_NAME
                        FROM {provider.Escape(schema)}.sqlite_master m
                        WHERE m.type = 'table'
                          AND m.name NOT LIKE 'sqlite_%'
                          AND UPPER(COALESCE(m.sql, '')) NOT LIKE 'CREATE VIRTUAL TABLE%'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {provider.Escape(schema)}.sqlite_master vt
                              WHERE vt.type = 'table'
                                AND UPPER(COALESCE(vt.sql, '')) LIKE 'CREATE VIRTUAL TABLE%'
                                AND m.name IN (
                                    vt.name || '_data',
                                    vt.name || '_idx',
                                    vt.name || '_content',
                                    vt.name || '_docsize',
                                    vt.name || '_config',
                                    vt.name || '_segments',
                                    vt.name || '_segdir',
                                    vt.name || '_stat',
                                    vt.name || '_node',
                                    vt.name || '_parent',
                                    vt.name || '_rowid'
                                )
                          )
                        ORDER BY m.name
                        """).ConfigureAwait(false));
                }

                return tables;
            }

            if (ScaffoldProviderKind.IsSqlServer(provider))
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT s.name AS TABLE_SCHEMA, t.name AS TABLE_NAME FROM sys.tables t INNER JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0 ORDER BY s.name, t.name").ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name").ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsMySql(provider))
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT NULL AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = DATABASE() ORDER BY table_name").ConfigureAwait(false);
            }

            return await GetSchemaTablesAsync(connection).ConfigureAwait(false);
        }

        private static async Task<IReadOnlyList<ScaffoldTableInfo>> QueryTablesAsync(DbConnection connection, string sql)
        {
            var tables = new List<ScaffoldTableInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = reader.IsDBNull(0) ? null : reader.GetString(0);
                var table = reader.GetString(1);
                tables.Add(new ScaffoldTableInfo(table, string.IsNullOrWhiteSpace(schema) ? null : schema));
            }

            return tables
                .OrderBy(t => t.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(t => t.Name, StringComparer.Ordinal)
                .ToArray();
        }

        private static async Task<IReadOnlyList<ScaffoldTableInfo>> GetSchemaTablesAsync(DbConnection connection)
        {
            var schema = await connection.GetSchemaAsync("Tables").ConfigureAwait(false);
            var tables = new List<ScaffoldTableInfo>();
            foreach (DataRow row in schema.Rows)
            {
                var tableType = row.Table.Columns.Contains("TABLE_TYPE") ? row["TABLE_TYPE"]?.ToString() : null;
                if (tableType != null && !string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase))
                    continue;

                var tableName = row["TABLE_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(tableName))
                    continue;

                var schemaName = row.Table.Columns.Contains("TABLE_SCHEMA")
                    ? row["TABLE_SCHEMA"]?.ToString()
                    : null;

                tables.Add(new ScaffoldTableInfo(tableName, string.IsNullOrWhiteSpace(schemaName) ? null : schemaName));
            }

            return tables
                .OrderBy(t => t.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(t => t.Name, StringComparer.Ordinal)
                .ToArray();
        }
    }
}
