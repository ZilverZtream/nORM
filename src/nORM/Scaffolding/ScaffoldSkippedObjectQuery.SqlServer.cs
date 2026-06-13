#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectQuery
    {
        private static Task<IReadOnlyDictionary<string, string>> GetSqlServerSkippedObjectCommentsAsync(DbConnection connection)
            => QuerySkippedObjectCommentsAsync(connection, """
                SELECT SCHEMA_NAME(o.schema_id) AS ObjectSchema,
                       o.name AS ObjectName,
                       'Routine' AS Kind,
                       CAST(comment.value AS nvarchar(max)) AS ObjectComment
                FROM sys.objects o
                LEFT JOIN sys.extended_properties comment
                  ON comment.major_id = o.object_id
                 AND comment.minor_id = 0
                 AND comment.name = N'MS_Description'
                WHERE o.is_ms_shipped = 0
                  AND o.type IN ('P', 'FN', 'IF', 'TF')
                UNION ALL
                SELECT SCHEMA_NAME(seq.schema_id),
                       seq.name,
                       'Sequence',
                       CAST(comment.value AS nvarchar(max))
                FROM sys.sequences seq
                LEFT JOIN sys.extended_properties comment
                  ON comment.major_id = seq.object_id
                 AND comment.minor_id = 0
                 AND comment.name = N'MS_Description'
                """);
    }
}
