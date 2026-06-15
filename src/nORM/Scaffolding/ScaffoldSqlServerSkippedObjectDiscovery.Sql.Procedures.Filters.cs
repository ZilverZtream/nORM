#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string StoredProcedureSkippedObjectSourceSql = """

            FROM sys.procedures p
            """;

        private const string StoredProcedureSkippedObjectFilterSql = """

            WHERE p.is_ms_shipped = 0
            """;
    }
}
