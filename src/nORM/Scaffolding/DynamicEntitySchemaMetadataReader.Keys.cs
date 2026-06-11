#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
            {
                var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
                using var cmd = connection.CreateCommand();
                var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                    ? string.Empty
                    : DynamicEntityConnectionKind.EscapeIdentifier(connection, schemaName!) + ".";
                cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({DynamicEntityConnectionKind.EscapeIdentifier(connection, tableName)})";
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

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
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

            if (DynamicEntityConnectionKind.IsPostgres(connection))
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

            if (DynamicEntityConnectionKind.IsMySql(connection))
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
            if (DynamicEntityConnectionKind.IsSqlite(connection))
            {
                var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                using var cmd = connection.CreateCommand();
                var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                    ? string.Empty
                    : DynamicEntityConnectionKind.EscapeIdentifier(connection, schemaName!) + ".";
                cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({DynamicEntityConnectionKind.EscapeIdentifier(connection, tableName)})";
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

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
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

            if (DynamicEntityConnectionKind.IsPostgres(connection))
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

            if (DynamicEntityConnectionKind.IsMySql(connection))
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
            if (!DynamicEntityConnectionKind.IsSqlServer(connection))
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
    }
}
