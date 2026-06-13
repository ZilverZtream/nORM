#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectQuery
    {
        private static Task<IReadOnlyDictionary<string, string>> GetPostgresSkippedObjectCommentsAsync(DbConnection connection)
            => QuerySkippedObjectCommentsAsync(connection, """
                SELECT n.nspname AS ObjectSchema,
                       p.proname AS ObjectName,
                       'Routine' AS Kind,
                       obj_description(p.oid, 'pg_proc') AS ObjectComment
                FROM pg_proc p
                INNER JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_proc sibling
                      WHERE sibling.pronamespace = p.pronamespace
                        AND sibling.proname = p.proname
                        AND sibling.oid <> p.oid
                  )
                UNION ALL
                SELECT n.nspname,
                       cls.relname,
                       'Sequence',
                       obj_description(cls.oid, 'pg_class')
                FROM pg_class cls
                INNER JOIN pg_namespace n ON n.oid = cls.relnamespace
                WHERE cls.relkind = 'S'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                """);
    }
}
