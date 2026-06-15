#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldSkippedObjectQuery;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
            => await QuerySkippedObjectsAsync(connection, GetSkippedObjectSql()).ConfigureAwait(false);

        private static string GetSkippedObjectSql()
            => string.Join(
                "\nUNION ALL\n",
                ViewSkippedObjectSql,
                StoredProcedureSkippedObjectSql,
                FunctionSkippedObjectSql,
                SequenceSkippedObjectSql,
                SynonymSkippedObjectSql)
            + "\nORDER BY ObjectSchema, ObjectName";
    }
}
