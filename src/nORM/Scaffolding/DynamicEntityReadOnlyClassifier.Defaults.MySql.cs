#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool HasMySqlUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
            => QueryHasUnmodeledDefault(connection, """
                SELECT CASE
                           WHEN LOWER(COALESCE(extra, '')) LIKE '%on update%'
                           THEN TRIM(CONCAT(COALESCE(column_default, ''), ' ', extra))
                           WHEN data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set')
                                AND (LOWER(column_default) LIKE 'lower(%' OR LOWER(column_default) LIKE 'upper(%')
                           THEN REPLACE(column_default, CHAR(92, 39), CHAR(39))
                           WHEN data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set')
                           THEN QUOTE(column_default)
                           WHEN data_type IN ('date', 'datetime', 'timestamp', 'time')
                                AND LOWER(column_default) NOT LIKE 'current_timestamp%'
                                AND LOWER(column_default) NOT LIKE 'current_time%'
                                AND LOWER(column_default) NOT LIKE 'localtime%'
                                AND LOWER(column_default) NOT LIKE 'localtimestamp%'
                                AND LOWER(column_default) NOT LIKE 'now(%'
                                AND LOWER(column_default) NOT LIKE 'utc_timestamp(%'
                                AND LOWER(column_default) NOT LIKE 'sysdate(%'
                                AND LOWER(column_default) NOT LIKE 'current_date%'
                                AND column_default NOT LIKE '%()'
                           THEN QUOTE(column_default)
                           ELSE column_default
                       END AS DefaultSql
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND (
                      column_default IS NOT NULL
                      OR LOWER(COALESCE(extra, '')) LIKE '%on update%'
                  )
                """, schemaName, tableName);
    }
}
