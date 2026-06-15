#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string StoredProcedureResultColumnsSql = """
                          '; resultColumns=',
                          COALESCE(NULLIF((
                              SELECT STRING_AGG(CONCAT(
                                  COALESCE(rs.name, ''),
                                  ':',
                                  COALESCE(rs.system_type_name, ''),
                                  ':',
                                  CONVERT(varchar(1), COALESCE(rs.is_nullable, 0))), '|') WITHIN GROUP (ORDER BY rs.column_ordinal)
                              FROM sys.dm_exec_describe_first_result_set_for_object(p.object_id, NULL) rs
                              WHERE rs.error_number IS NULL
                                AND rs.is_hidden = 0
                          ), ''), ''))
            """;
    }
}
