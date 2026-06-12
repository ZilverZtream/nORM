#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetStringBinaryFacets(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlServer(connection))
            {
                return QueryColumnFacetMap(connection, """
                    SELECT c.name AS ColumnName,
                           CASE
                               WHEN COALESCE(base_ty.name, ty.name) IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN CONVERT(int, c.max_length / 2)
                               WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length > 0 THEN CONVERT(int, c.max_length)
                               ELSE NULL
                           END AS MaxLength,
                           CASE
                               WHEN COALESCE(base_ty.name, ty.name) IN ('nchar', 'nvarchar') THEN CONVERT(int, 1)
                               WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'varchar') THEN CONVERT(int, 0)
                               ELSE NULL
                           END AS IsUnicode,
                           CASE
                               WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'nchar', 'binary') THEN CONVERT(int, 1)
                               ELSE CONVERT(int, 0)
                           END AS IsFixedLength
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
                      AND COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary')
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsPostgres(connection))
            {
                return QueryColumnFacetMap(connection, """
                    SELECT column_name AS ColumnName,
                           character_maximum_length::int AS MaxLength,
                           NULL::int AS IsUnicode,
                           CASE WHEN data_type = 'character' OR udt_name = 'bpchar' THEN 1 ELSE 0 END AS IsFixedLength
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (data_type IN ('character varying', 'character') OR udt_name IN ('varchar', 'bpchar'))
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsMySql(connection))
            {
                return QueryColumnFacetMap(connection, """
                    SELECT column_name AS ColumnName,
                           character_maximum_length AS MaxLength,
                           NULL AS IsUnicode,
                           CASE WHEN data_type IN ('char', 'binary') THEN 1 ELSE 0 END AS IsFixedLength
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND table_name = @tableName
                      AND data_type IN ('char', 'varchar', 'binary', 'varbinary')
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteDeclaredStringBinaryFacets(connection, schemaName, tableName);

            return new Dictionary<string, ScaffoldColumnFacet>(0, StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetDecimalPrecisions(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlServer(connection))
            {
                return QueryDecimalPrecisionMap(connection, """
                    SELECT c.name AS ColumnName,
                           CONVERT(int, c.precision) AS DecimalPrecision,
                           CONVERT(int, c.scale) AS DecimalScale
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
                      AND COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric')
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsPostgres(connection))
            {
                return QueryDecimalPrecisionMap(connection, """
                    SELECT column_name AS ColumnName,
                           numeric_precision AS DecimalPrecision,
                           numeric_scale AS DecimalScale
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND data_type = 'numeric'
                      AND numeric_precision IS NOT NULL
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsMySql(connection))
            {
                return QueryDecimalPrecisionMap(connection, """
                    SELECT column_name AS ColumnName,
                           numeric_precision AS DecimalPrecision,
                           numeric_scale AS DecimalScale
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND data_type IN ('decimal', 'numeric')
                      AND numeric_precision IS NOT NULL
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteDeclaredDecimalPrecisions(connection, schemaName, tableName);

            return new Dictionary<string, ScaffoldDecimalPrecisionInfo>(0, StringComparer.OrdinalIgnoreCase);
        }

        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetSqliteDeclaredStringBinaryFacets(
            DbConnection connection,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["name"]);
                var declaredType = Convert.ToString(reader["type"]);
                if (!string.IsNullOrWhiteSpace(columnName)
                    && ScaffoldSqliteDdlParser.TryParseDeclaredStringBinaryFacet(declaredType, out var facet))
                {
                    result[columnName!] = facet;
                }
            }

            return result;
        }

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetSqliteDeclaredDecimalPrecisions(
            DbConnection connection,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, ScaffoldDecimalPrecisionInfo>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["name"]);
                var declaredType = Convert.ToString(reader["type"]);
                if (!string.IsNullOrWhiteSpace(columnName)
                    && ScaffoldSqliteDdlParser.TryParseDeclaredDecimalPrecision(declaredType, out var precision))
                {
                    result[columnName!] = precision;
                }
            }

            return result;
        }

        private static string SqlitePragma(DbConnection connection, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : DynamicEntityConnectionKind.EscapeIdentifier(connection, schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({DynamicEntityConnectionKind.EscapeIdentifier(connection, argument)})";
        }
    }
}
