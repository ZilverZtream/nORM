#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlySet<string> GetPostgresIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnNameSet(connection, """
                SELECT column_name AS ColumnName
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND (
                      is_identity = 'YES'
                      OR column_default LIKE 'nextval(%'
                  )
                """, schemaName, tableName);

        private static IReadOnlyDictionary<string, int> GetPostgresPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnOrdinalMap(connection, """
                SELECT att.attname AS ColumnName, keys.ordinality AS Ordinal
                FROM pg_constraint con
                INNER JOIN pg_class cls ON cls.oid = con.conrelid
                INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
                INNER JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = keys.attnum
                WHERE con.contype = 'p'
                  AND cls.relname = @tableName
                  AND (@schemaName IS NULL OR ns.nspname = @schemaName)
                """, schemaName, tableName);
    }
}
