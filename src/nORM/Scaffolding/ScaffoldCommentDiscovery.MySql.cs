#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCommentDiscovery
    {
        private static Task<IReadOnlyDictionary<string, ScaffoldComments>> GetMySqlScaffoldCommentsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryScaffoldCommentsAsync(connection, tableKeys, """
                SELECT NULL AS TableSchema,
                       t.table_name AS TableName,
                       CASE WHEN t.table_type = 'BASE TABLE' THEN t.table_comment ELSE NULL END AS TableComment,
                       c.column_name AS ColumnName,
                       c.column_comment AS ColumnComment
                FROM information_schema.tables t
                INNER JOIN information_schema.columns c
                  ON c.table_schema = t.table_schema
                 AND c.table_name = t.table_name
                WHERE t.table_schema = DATABASE()
                  AND t.table_type IN ('BASE TABLE', 'VIEW')
                """);
    }
}
