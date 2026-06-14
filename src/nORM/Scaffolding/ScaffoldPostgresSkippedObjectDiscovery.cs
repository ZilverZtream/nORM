#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldSkippedObjectQuery;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = await QuerySkippedObjectsAsync(connection, GetSkippedObjectSql()).ConfigureAwait(false);
            return await AttachSkippedObjectCommentsAsync(connection, provider, objects).ConfigureAwait(false);
        }
    }
}
