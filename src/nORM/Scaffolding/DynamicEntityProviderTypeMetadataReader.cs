#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityProviderTypeMetadataReader
    {
        public static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (!IsPostgresConnection(connection.GetType().Name))
                return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_name AS ColumnName,
                       CASE
                           WHEN data_type = 'USER-DEFINED'
                                AND EXISTS (
                                    SELECT 1
                                    FROM pg_type enum_type
                                    INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                                    WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                                      AND enum_type.typname = udt_name
                                      AND enum_type.typtype = 'e'
                                )
                           THEN 'text'
                           WHEN data_type IN ('ARRAY', 'USER-DEFINED')
                                AND udt_name IS NOT NULL
                                AND udt_name <> ''
                           THEN data_type || ' (' || udt_name || ')'
                           WHEN data_type IN ('character varying', 'character')
                                AND character_maximum_length IS NOT NULL
                           THEN data_type || '(' || character_maximum_length::text || ')'
                           WHEN data_type = 'numeric'
                                AND numeric_precision IS NOT NULL
                                AND numeric_scale IS NULL
                           THEN data_type || '(' || numeric_precision::text || ')'
                           WHEN data_type = 'numeric'
                                AND numeric_precision IS NOT NULL
                                AND numeric_scale IS NOT NULL
                           THEN data_type || '(' || numeric_precision::text || ',' || numeric_scale::text || ')'
                           ELSE data_type
                       END AS DataType
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND (
                      domain_name IS NOT NULL
                      OR data_type = 'USER-DEFINED'
                      OR (
                          data_type = 'ARRAY'
                          AND COALESCE(udt_name, '') IN (
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
                  )
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                var dataType = Convert.ToString(reader["DataType"]) ?? string.Empty;
                result[columnName] = ScaffoldProviderSpecificTypeClassifier.NormalizePostgresDomainProbeCastType(dataType);
            }

            return result;
        }

        public static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (!IsMySqlConnection(connection.GetType().Name))
                return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_name AS ColumnName, column_type AS ColumnType
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND LOWER(COALESCE(column_type, '')) LIKE '%unsigned%'
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                result[columnName] = Convert.ToString(reader["ColumnType"]) ?? string.Empty;
            }

            return result;
        }

        public static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (!IsSqlServerConnection(connection.GetType().Name))
                return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT c.name AS ColumnName,
                       base_ty.name +
                       CASE
                           WHEN base_ty.name IN ('nchar', 'nvarchar') AND c.max_length = -1 THEN '(max)'
                           WHEN base_ty.name IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN '(' + CONVERT(varchar(11), c.max_length / 2) + ')'
                           WHEN base_ty.name IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length = -1 THEN '(max)'
                           WHEN base_ty.name IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length > 0 THEN '(' + CONVERT(varchar(11), c.max_length) + ')'
                           WHEN base_ty.name IN ('decimal', 'numeric') THEN '(' + CONVERT(varchar(10), c.precision) + ',' + CONVERT(varchar(10), c.scale) + ')'
                           ELSE ''
                       END AS BaseType
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                INNER JOIN sys.types base_ty
                  ON ty.is_user_defined = 1
                 AND base_ty.user_type_id = ty.system_type_id
                 AND base_ty.is_user_defined = 0
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                result[columnName] = Convert.ToString(reader["BaseType"]) ?? string.Empty;
            }

            return result;
        }

        public static IReadOnlyList<string> GetPostgresColumnNames(DbConnection connection, string? schemaName, string tableName)
        {
            var result = new List<string>();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_name AS ColumnName
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                ORDER BY ordinal_position
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (!string.IsNullOrWhiteSpace(columnName))
                    result.Add(columnName);
            }

            return result;
        }
    }
}
