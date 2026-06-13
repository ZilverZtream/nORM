#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectQuery
    {
        private static Task<IReadOnlyDictionary<string, string>> GetMySqlSkippedObjectCommentsAsync(DbConnection connection)
            => QuerySkippedObjectCommentsAsync(connection, """
                SELECT NULL AS ObjectSchema,
                       routine_name AS ObjectName,
                       'Routine' AS Kind,
                       routine_comment AS ObjectComment
                FROM information_schema.routines
                WHERE routine_schema = DATABASE()
                """);
    }
}
