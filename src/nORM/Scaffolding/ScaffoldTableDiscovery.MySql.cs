#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableDiscovery
    {
        private static Task<IReadOnlyList<ScaffoldTableInfo>> GetMySqlTablesAsync(DbConnection connection)
            => QueryTablesAsync(
                connection,
                "SELECT NULL AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = DATABASE() ORDER BY table_name");
    }
}
