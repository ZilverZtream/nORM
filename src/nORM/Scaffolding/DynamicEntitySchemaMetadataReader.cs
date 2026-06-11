#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetStringBinaryFacets(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqlServerConnection(connectionName))
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

            if (IsPostgresConnection(connectionName))
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

            if (IsMySqlConnection(connectionName))
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

            return new Dictionary<string, ScaffoldColumnFacet>(0, StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetDecimalPrecisions(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqlServerConnection(connectionName))
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

            if (IsPostgresConnection(connectionName))
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

            if (IsMySqlConnection(connectionName))
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

            return new Dictionary<string, ScaffoldDecimalPrecisionInfo>(0, StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlyDictionary<string, string> GetSqliteDeclaredColumnTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (!IsSqliteConnection(connection.GetType().Name))
                return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                ? string.Empty
                : EscapeIdentifier(connection, schemaName!) + ".";
            cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({EscapeIdentifier(connection, tableName)})";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "name") || !ReaderHasColumn(reader, "type"))
                    continue;

                var name = Convert.ToString(reader["name"]);
                var type = Convert.ToString(reader["type"]);
                if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(type))
                    result[name] = type;
            }

            return result;
        }

        public static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;

            if (IsSqliteConnection(connectionName))
            {
                var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
                using var cmd = connection.CreateCommand();
                var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                    ? string.Empty
                    : EscapeIdentifier(connection, schemaName!) + ".";
                cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({EscapeIdentifier(connection, tableName)})";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    rows.Add((
                        Convert.ToString(reader["name"]) ?? string.Empty,
                        Convert.ToString(reader["type"]) ?? string.Empty,
                        ReaderHasColumn(reader, "pk")
                            ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                            : 0));
                }

                var primaryKeyColumns = rows.Where(row => row.PrimaryKeyOrdinal > 0).ToArray();
                if (primaryKeyColumns.Length == 1
                    && primaryKeyColumns[0].Type.Contains("INT", StringComparison.OrdinalIgnoreCase))
                {
                    return new HashSet<string>(StringComparer.OrdinalIgnoreCase) { primaryKeyColumns[0].Name };
                }

                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryColumnNameSet(connection, """
                    SELECT c.name AS ColumnName
                    FROM sys.identity_columns ic
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    INNER JOIN sys.tables t ON t.object_id = ic.object_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryColumnNameSet(connection, """
                    SELECT column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND (
                          is_identity = 'YES'
                          OR column_default LIKE 'nextval(%'
                      )
                    """, schemaName, tableName);
            }

            if (IsMySqlConnection(connectionName))
            {
                return QueryColumnNameSet(connection, """
                    SELECT column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND LOWER(extra) LIKE '%auto_increment%'
                    """, schemaName, tableName);
            }

            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlyDictionary<string, int> GetPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;

            if (IsSqliteConnection(connectionName))
            {
                var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                using var cmd = connection.CreateCommand();
                var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                    ? string.Empty
                    : EscapeIdentifier(connection, schemaName!) + ".";
                cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({EscapeIdentifier(connection, tableName)})";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    if (!ReaderHasColumn(reader, "name") || !ReaderHasColumn(reader, "pk"))
                        continue;

                    var ordinal = Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture);
                    var name = Convert.ToString(reader["name"]);
                    if (ordinal > 0 && !string.IsNullOrWhiteSpace(name))
                        result[name] = ordinal;
                }

                return result;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryColumnOrdinalMap(connection, """
                    SELECT c.name AS ColumnName, ic.key_ordinal AS Ordinal
                    FROM sys.tables t
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    INNER JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
                    INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryColumnOrdinalMap(connection, """
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

            if (IsMySqlConnection(connectionName))
            {
                return QueryColumnOrdinalMap(connection, """
                    SELECT column_name AS ColumnName, ordinal_position AS Ordinal
                    FROM information_schema.key_column_usage
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND constraint_name = 'PRIMARY'
                    """, schemaName, tableName);
            }

            return new Dictionary<string, int>(0, StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlySet<string> GetRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (!IsSqlServerConnection(connectionName))
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            return QueryColumnNameSet(connection, """
                SELECT c.name AS ColumnName
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND ty.name IN ('timestamp', 'rowversion')
                """, schemaName, tableName);
        }

        public static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        public static IReadOnlyList<string> GetPostgresColumnNames(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityProviderTypeMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);

    }
}
