#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityWriteBlockingClassifier
    {
        private static bool HasWriteBlockingPostgresColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM information_schema.columns c
                WHERE c.table_name = @tableName
                  AND (@schemaName IS NULL OR c.table_schema = @schemaName)
                  AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND (
                      c.udt_name IN ('inet', 'cidr', 'macaddr', 'macaddr8', 'tsvector', 'tsquery')
                      OR (
                          c.data_type = 'ARRAY'
                          AND COALESCE(c.udt_name, '') NOT IN (
                              '_int2',
                              '_int4',
                              '_int8',
                              '_float4',
                              '_float8',
                              '_numeric',
                              '_bool',
                              '_uuid',
                              '_text',
                              '_varchar',
                              '_bpchar',
                              '_citext',
                              '_bytea',
                              '_date',
                              '_time',
                              '_timetz',
                              '_interval',
                              '_timestamp',
                              '_timestamptz'
                          )
                      )
                      OR (
                          c.data_type = 'USER-DEFINED'
                          AND COALESCE(c.udt_name, '') NOT IN ('citext', 'json', 'jsonb', 'xml', 'uuid')
                          AND NOT EXISTS (
                              SELECT 1
                              FROM pg_type enum_type
                              INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                              WHERE enum_ns.nspname = COALESCE(c.udt_schema, c.table_schema)
                                AND enum_type.typname = c.udt_name
                                AND enum_type.typtype = 'e'
                          )
                      )
                  )
                """, schemaName, tableName);
    }
}
