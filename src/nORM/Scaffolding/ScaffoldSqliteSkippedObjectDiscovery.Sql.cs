#nullable enable
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteSkippedObjectDiscovery
    {
        private static string GetSqliteSkippedObjectSql(DatabaseProvider provider, string schema)
            => $"""
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
               """;
    }
}
