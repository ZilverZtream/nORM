#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldTableInfo>> GetSqliteTablesAsync(
            DbConnection connection,
            DatabaseProvider provider)
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
    }
}
