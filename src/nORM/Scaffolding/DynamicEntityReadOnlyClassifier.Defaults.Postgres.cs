#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool HasPostgresUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
            => QueryHasUnmodeledDefault(connection, """
                SELECT column_default AS DefaultSql
                FROM information_schema.columns c
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND column_default IS NOT NULL
                  AND is_identity <> 'YES'
                  AND NOT (
                      column_default LIKE 'nextval(%'
                      AND pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL
                  )
                """, schemaName, tableName);
    }
}
