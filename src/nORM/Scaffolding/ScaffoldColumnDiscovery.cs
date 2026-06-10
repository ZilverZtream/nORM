#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldColumnDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetStringBinaryFacetsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(table => TableKey(table.Schema, table.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (tableKeys.Count == 0)
                return new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);

            var providerName = provider.GetType().Name;
            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnFacetMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                           t.name AS TableName,
                           c.name AS ColumnName,
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
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    LEFT JOIN sys.types base_ty
                      ON ty.is_user_defined = 1
                     AND base_ty.user_type_id = ty.system_type_id
                     AND base_ty.is_user_defined = 0
                    WHERE t.is_ms_shipped = 0
                      AND COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary')
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnFacetMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema,
                           table_name AS TableName,
                           column_name AS ColumnName,
                           character_maximum_length::int AS MaxLength,
                           NULL::int AS IsUnicode,
                           CASE WHEN data_type = 'character' OR udt_name = 'bpchar' THEN 1 ELSE 0 END AS IsFixedLength
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (data_type IN ('character varying', 'character') OR udt_name IN ('varchar', 'bpchar'))
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnFacetMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema,
                           table_name AS TableName,
                           column_name AS ColumnName,
                           character_maximum_length AS MaxLength,
                           NULL AS IsUnicode,
                           CASE WHEN data_type IN ('char', 'binary') THEN 1 ELSE 0 END AS IsFixedLength
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND data_type IN ('char', 'varchar', 'binary', 'varbinary')
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);
        }

        public static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetNonNullableColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            if (provider is SqliteProvider)
            {
                var sqliteResult = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var tableColumns = new List<(string ColumnName, bool NotNull, int PrimaryKeyOrdinal)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        if (!ReaderHasColumn(reader, "name"))
                            continue;

                        var columnName = Convert.ToString(reader["name"]);
                        if (string.IsNullOrWhiteSpace(columnName))
                            continue;

                        var notNull = ReaderHasColumn(reader, "notnull")
                            && Convert.ToInt32(reader["notnull"], CultureInfo.InvariantCulture) != 0;
                        var primaryKeyOrdinal = ReaderHasColumn(reader, "pk")
                            ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                            : 0;
                        tableColumns.Add((columnName, notNull, primaryKeyOrdinal));
                    }

                    var primaryKeyColumnCount = tableColumns.Count(static column => column.PrimaryKeyOrdinal > 0);
                    foreach (var column in tableColumns)
                    {
                        if (column.NotNull
                            || (column.PrimaryKeyOrdinal > 0 && primaryKeyColumnCount == 1))
                        {
                            columns.Add(column.ColumnName);
                        }
                    }

                    sqliteResult[TableKey(table.Schema, table.Name)] = columns;
                }

                return sqliteResult;
            }

            if (provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND is_nullable = 'NO'
                    """).ConfigureAwait(false);
            }

            var result = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {EscapeQualified(provider, table.Schema, table.Name)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (DataRow row in schema.Rows)
                {
                    var columnName = row["ColumnName"]!.ToString()!;
                    var allowNull = row["AllowDBNull"] is bool b && b;
                    if (!allowNull)
                        columns.Add(columnName);
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
        }

        public static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (provider is SqliteProvider)
            {
                var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        rows.Add((
                            Convert.ToString(reader["name"]) ?? string.Empty,
                            Convert.ToString(reader["type"]) ?? string.Empty,
                            ReaderHasColumn(reader, "pk")
                                ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                                : 0));
                    }

                    var primaryKeyColumns = rows.Where(row => row.PrimaryKeyOrdinal > 0).ToArray();
                    if (primaryKeyColumns.Length != 1)
                        continue;

                    var key = primaryKeyColumns[0];
                    if (key.Type.Contains("INT", StringComparison.OrdinalIgnoreCase))
                    {
                        var tableKey = TableKey(table.Schema, table.Name);
                        result[tableKey] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { key.Name };
                    }
                }

                return ToReadOnlySetDictionary(result);
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName
                    FROM sys.identity_columns ic
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    INNER JOIN sys.tables t ON t.object_id = ic.object_id
                    WHERE t.is_ms_shipped = 0
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (
                          is_identity = 'YES'
                          OR column_default LIKE 'nextval(%'
                      )
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND LOWER(extra) LIKE '%auto_increment%'
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> QueryColumnFacetMapAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, Dictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(columnName))
                    continue;

                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName!);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var maxLength = reader["MaxLength"] == DBNull.Value
                    ? (int?)null
                    : Convert.ToInt32(reader["MaxLength"], CultureInfo.InvariantCulture);
                var isUnicode = reader["IsUnicode"] == DBNull.Value
                    ? (bool?)null
                    : Convert.ToInt32(reader["IsUnicode"], CultureInfo.InvariantCulture) != 0;
                var isFixedLength = reader["IsFixedLength"] != DBNull.Value
                    && Convert.ToInt32(reader["IsFixedLength"], CultureInfo.InvariantCulture) != 0;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns[columnName!] = new ScaffoldColumnFacet(
                    maxLength is > 0 ? maxLength : null,
                    isUnicode,
                    isFixedLength);
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, ScaffoldColumnFacet>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> QueryColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns.Add(columnName);
            }

            return ToReadOnlySetDictionary(result);
        }

        private static IReadOnlyDictionary<string, IReadOnlySet<string>> ToReadOnlySetDictionary(
            Dictionary<string, HashSet<string>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlySet<string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static string EscapeQualified(DatabaseProvider provider, string? schema, string table)
            => string.IsNullOrEmpty(schema)
                ? IdentifierEscaping.EscapeSingle(provider, table)
                : $"{provider.Escape(schema!)}.{IdentifierEscaping.EscapeSingle(provider, table)}";

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
