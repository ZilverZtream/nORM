#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityWriteBlockingClassifier
    {
        public static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
            {
                var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"PRAGMA {EscapeIdentifier(connection, schema)}.table_xinfo({EscapeIdentifier(connection, tableName)})";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    if (!ReaderHasColumn(reader, "type"))
                        continue;

                    if (IsWriteBlockingSqliteDeclaredType(Convert.ToString(reader["type"])))
                        return true;
                }

                return false;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    LEFT JOIN sys.types base_ty
                      ON ty.is_user_defined = 1
                     AND base_ty.user_type_id = ty.system_type_id
                     AND base_ty.is_user_defined = 0
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                      AND t.is_ms_shipped = 0
                      AND (
                          ty.name IN ('geography', 'geometry', 'hierarchyid', 'sql_variant')
                          OR (
                              ty.is_user_defined = 1
                              AND (
                                  base_ty.name IS NULL
                                  OR base_ty.name NOT IN (
                                      'int', 'bigint', 'smallint', 'tinyint', 'bit',
                                      'decimal', 'numeric', 'money', 'smallmoney',
                                      'float', 'real',
                                      'date', 'time', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset',
                                      'uniqueidentifier', 'sysname',
                                      'char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext', 'xml',
                                      'binary', 'varbinary', 'image'
                                  )
                              )
                          )
                      )
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryExists(connection, """
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

            if (IsMySqlConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND data_type IN (
                          'geometry',
                          'point',
                          'linestring',
                          'polygon',
                          'multipoint',
                          'multilinestring',
                          'multipolygon',
                          'geometrycollection'
                      )
                    """, schemaName, tableName)
                    || HasWriteBlockingMySqlSetColumns(connection, schemaName, tableName);
            }

            return false;
        }

        public static bool HasWriteBlockingMySqlSetColumns(DbConnection connection, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_type AS ColumnType
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND data_type = 'set'
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(Convert.ToString(reader["ColumnType"]), out _))
                    return true;
            }

            return false;
        }

        public static bool IsWriteBlockingSqliteDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return IsUnsafeSqliteProviderSpecificDeclaredType(normalized);
        }

        public static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "GEOMETRY")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "GEOGRAPHY")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "POINT")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "LINESTRING")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "POLYGON")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MULTIPOINT")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MULTILINESTRING")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MULTIPOLYGON")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "GEOMETRYCOLLECTION")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "HIERARCHYID")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "SQL_VARIANT")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "INET")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "CIDR")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MACADDR")
               || StartsWithSqliteDeclaredTypeToken(normalizedDeclaredType, "ENUM")
               || StartsWithSqliteDeclaredTypeToken(normalizedDeclaredType, "SET")
               || normalizedDeclaredType.EndsWith("[]", StringComparison.Ordinal);

        public static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
        {
            var start = 0;
            while (start < normalizedDeclaredType.Length)
            {
                var index = normalizedDeclaredType.IndexOf(token, start, StringComparison.Ordinal);
                if (index < 0)
                    return false;

                var before = index == 0 || !IsSqliteDeclaredTypeTokenChar(normalizedDeclaredType[index - 1]);
                var afterIndex = index + token.Length;
                var after = afterIndex == normalizedDeclaredType.Length || !IsSqliteDeclaredTypeTokenChar(normalizedDeclaredType[afterIndex]);
                if (before && after)
                    return true;

                start = index + token.Length;
            }

            return false;
        }

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }

        private static bool StartsWithSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => normalizedDeclaredType.StartsWith(token, StringComparison.Ordinal)
               && (normalizedDeclaredType.Length == token.Length
                   || !IsSqliteDeclaredTypeTokenChar(normalizedDeclaredType[token.Length]));

        private static bool IsSqliteDeclaredTypeTokenChar(char ch)
            => (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');

        private static bool QueryExists(DbConnection connection, string sql, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            return cmd.ExecuteScalar() is not null;
        }
    }
}
