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
    internal static class ScaffoldKeyDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (provider is SqliteProvider)
            {
                var sqliteResult = new Dictionary<string, List<(int Ordinal, string Column)>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var keyColumns = new List<(int Ordinal, string Column)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        if (!ReaderHasColumn(reader, "name")
                            || !ReaderHasColumn(reader, "pk"))
                        {
                            continue;
                        }

                        var ordinal = Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture);
                        if (ordinal <= 0)
                        {
                            continue;
                        }

                        var name = Convert.ToString(reader["name"]);
                        if (!string.IsNullOrWhiteSpace(name))
                            keyColumns.Add((ordinal, name));
                    }

                    sqliteResult[TableKey(table.Schema, table.Name)] = keyColumns;
                }

                return ToOrderedColumnDictionary(sqliteResult);
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName, ic.key_ordinal AS Ordinal
                    FROM sys.tables t
                    INNER JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
                    INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE t.is_ms_shipped = 0
                    ORDER BY SCHEMA_NAME(t.schema_id), t.name, ic.key_ordinal
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT ns.nspname AS TableSchema, cls.relname AS TableName, att.attname AS ColumnName, keys.ordinality AS Ordinal
                    FROM pg_constraint con
                    INNER JOIN pg_class cls ON cls.oid = con.conrelid
                    INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                    CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
                    INNER JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = keys.attnum
                    WHERE con.contype = 'p'
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY ns.nspname, cls.relname, keys.ordinality
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName, ordinal_position AS Ordinal
                    FROM information_schema.key_column_usage
                    WHERE table_schema = DATABASE()
                      AND constraint_name = 'PRIMARY'
                    ORDER BY table_name, ordinal_position
                    """).ConfigureAwait(false);
            }

            var result = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {IdentifierEscaping.EscapeTable(provider, table.Name, table.Schema)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var keyColumns = new List<string>();
                foreach (DataRow row in schema.Rows)
                {
                    if (row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool isKey && isKey)
                        keyColumns.Add(row["ColumnName"]!.ToString()!);
                }

                result[TableKey(table.Schema, table.Name)] = keyColumns;
            }

            return result;
        }

        public static async Task<IReadOnlyDictionary<string, string>> GetPrimaryKeyConstraintNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                           t.name AS TableName,
                           kc.name AS ConstraintName
                    FROM sys.tables t
                    INNER JOIN sys.key_constraints kc ON kc.parent_object_id = t.object_id AND kc.type = 'PK'
                    WHERE t.is_ms_shipped = 0
                      AND kc.is_system_named = 0
                    ORDER BY SCHEMA_NAME(t.schema_id), t.name
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, """
                    SELECT ns.nspname AS TableSchema,
                           cls.relname AS TableName,
                           con.conname AS ConstraintName
                    FROM pg_constraint con
                    INNER JOIN pg_class cls ON cls.oid = con.conrelid
                    INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                    WHERE con.contype = 'p'
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND con.conname <> cls.relname || '_pkey'
                    ORDER BY ns.nspname, cls.relname
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> QueryOrderedColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = tableKeys.ToDictionary(
                key => key,
                _ => new List<(int Ordinal, string Column)>(),
                StringComparer.OrdinalIgnoreCase);

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

                var ordinal = ReaderHasColumn(reader, "Ordinal")
                    ? Convert.ToInt32(reader["Ordinal"], CultureInfo.InvariantCulture)
                    : result[tableKey].Count + 1;
                result[tableKey].Add((ordinal, columnName));
            }

            return ToOrderedColumnDictionary(result);
        }

        private static async Task<IReadOnlyDictionary<string, string>> QueryPrimaryKeyConstraintNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(
                    NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])),
                    Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var constraintName = NullIfWhiteSpace(Convert.ToString(reader["ConstraintName"]));
                if (constraintName is not null)
                    result[tableKey] = constraintName;
            }

            return result;
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> ToOrderedColumnDictionary(
            Dictionary<string, List<(int Ordinal, string Column)>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyList<string>)pair.Value
                    .OrderBy(item => item.Ordinal)
                    .Select(item => item.Column)
                    .ToArray(),
                StringComparer.OrdinalIgnoreCase);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
