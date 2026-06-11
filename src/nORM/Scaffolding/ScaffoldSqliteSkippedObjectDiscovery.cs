#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldSkippedObjectQuery;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqliteSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = new List<ScaffoldSkippedObjectInfo>();
            foreach (var schema in await GetSqliteSchemasAsync(connection).ConfigureAwait(false))
            {
                objects.AddRange(await QuerySkippedObjectsAsync(
                    connection,
                    $"""
                    SELECT {SqliteSchemaResult(schema)} AS ObjectSchema, name AS ObjectName, 'View' AS Kind, 'SQLite view' AS Detail
                    FROM {provider.Escape(schema)}.sqlite_master
                    WHERE type = 'view'
                    UNION ALL
                    SELECT {SqliteSchemaResult(schema)}, name, 'VirtualTable', 'SQLite virtual table'
                    FROM {provider.Escape(schema)}.sqlite_master
                    WHERE type = 'table' AND UPPER(sql) LIKE 'CREATE VIRTUAL TABLE%'
                    UNION ALL
                    SELECT {SqliteSchemaResult(schema)}, m.name, 'VirtualTableShadow', 'SQLite virtual table shadow table'
                    FROM {provider.Escape(schema)}.sqlite_master m
                    WHERE m.type = 'table'
                      AND m.name NOT LIKE 'sqlite_%'
                      AND UPPER(COALESCE(m.sql, '')) NOT LIKE 'CREATE VIRTUAL TABLE%'
                      AND EXISTS (
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
                    ORDER BY ObjectName
                    """).ConfigureAwait(false));
            }

            return objects;
        }

        public static async Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
        {
            var schemas = new List<string>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA database_list";
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = Convert.ToString(reader["name"]);
                if (string.IsNullOrWhiteSpace(schema)
                    || string.Equals(schema, "temp", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                schemas.Add(schema);
            }

            return schemas.Count == 0 ? new[] { "main" } : schemas;
        }

        public static string SqliteSchemaResult(string schema)
            => string.Equals(schema, "main", StringComparison.OrdinalIgnoreCase)
                ? "NULL"
                : "'" + schema.Replace("'", "''") + "'";
    }
}
