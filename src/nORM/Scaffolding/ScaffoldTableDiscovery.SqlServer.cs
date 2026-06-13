#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableDiscovery
    {
        private static Task<IReadOnlyList<ScaffoldTableInfo>> GetSqlServerTablesAsync(DbConnection connection)
            => QueryTablesAsync(
                connection,
                "SELECT s.name AS TABLE_SCHEMA, t.name AS TABLE_NAME FROM sys.tables t INNER JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0 ORDER BY s.name, t.name");
    }
}
