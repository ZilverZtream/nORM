#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCommentDiscovery
    {
        private static Task<IReadOnlyDictionary<string, ScaffoldComments>> GetPostgresScaffoldCommentsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryScaffoldCommentsAsync(connection, tableKeys, """
                SELECT n.nspname AS TableSchema,
                       cls.relname AS TableName,
                       obj_description(cls.oid, 'pg_class') AS TableComment,
                       a.attname AS ColumnName,
                       col_description(cls.oid, a.attnum) AS ColumnComment
                FROM pg_class cls
                INNER JOIN pg_namespace n ON n.oid = cls.relnamespace
                INNER JOIN pg_attribute a ON a.attrelid = cls.oid
                WHERE cls.relkind IN ('r', 'p', 'v', 'm')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                """);
    }
}
