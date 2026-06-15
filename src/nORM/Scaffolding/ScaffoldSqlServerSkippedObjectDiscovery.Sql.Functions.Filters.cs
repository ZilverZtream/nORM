namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string FunctionSkippedObjectSourceSql = """

            FROM sys.objects o
            """;

        private const string FunctionSkippedObjectFilterSql = """

            WHERE o.is_ms_shipped = 0
              AND o.type IN ('FN', 'IF', 'TF')
            """;
    }
}
