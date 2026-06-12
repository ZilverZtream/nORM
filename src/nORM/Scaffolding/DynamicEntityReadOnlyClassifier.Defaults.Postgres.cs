#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool HasPostgresUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
            => QueryHasUnmodeledDefault(connection, """
                SELECT column_default AS DefaultSql
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND column_default IS NOT NULL
                  AND is_identity <> 'YES'
                  AND column_default NOT LIKE 'nextval(%'
                """, schemaName, tableName);
    }
}
