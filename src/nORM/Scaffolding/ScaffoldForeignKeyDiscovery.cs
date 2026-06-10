#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldForeignKeyDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
                return await GetSqliteForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await QueryForeignKeysAsync(connection, SqlServerForeignKeySql).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await QueryForeignKeysAsync(connection, PostgresForeignKeySql).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await QueryForeignKeysAsync(connection, MySqlForeignKeySql).ConfigureAwait(false);

            return Array.Empty<ScaffoldForeignKeyInfo>();
        }

        private static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> GetSqliteForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var foreignKeys = new List<ScaffoldForeignKeyInfo>();
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "foreign_key_list", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                var rows = new List<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn, string OnUpdate, string OnDelete)>();
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    rows.Add((
                        Convert.ToInt64(reader["id"], CultureInfo.InvariantCulture),
                        Convert.ToInt64(reader["seq"], CultureInfo.InvariantCulture),
                        Convert.ToString(reader["table"]) ?? string.Empty,
                        Convert.ToString(reader["from"]) ?? string.Empty,
                        Convert.ToString(reader["to"]) ?? string.Empty,
                        Convert.ToString(reader["on_update"]) ?? string.Empty,
                        Convert.ToString(reader["on_delete"]) ?? string.Empty));
                }

                foreach (var group in rows.GroupBy(static row => row.Id))
                    AddSqliteForeignKeyRows(table, group.OrderBy(static row => row.Seq).ToArray(), foreignKeys);
            }

            return foreignKeys;
        }

        private static void AddSqliteForeignKeyRows(
            ScaffoldTableInfo table,
            IReadOnlyList<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn, string OnUpdate, string OnDelete)> rows,
            ICollection<ScaffoldForeignKeyInfo> foreignKeys)
        {
            foreach (var row in rows)
            {
                if (string.IsNullOrWhiteSpace(row.PrincipalTable)
                    || string.IsNullOrWhiteSpace(row.DependentColumn)
                    || string.IsNullOrWhiteSpace(row.PrincipalColumn))
                {
                    continue;
                }

                foreignKeys.Add(new ScaffoldForeignKeyInfo(
                    DependentSchema: table.Schema,
                    DependentTable: table.Name,
                    DependentColumn: row.DependentColumn,
                    PrincipalSchema: table.Schema,
                    PrincipalTable: row.PrincipalTable,
                    PrincipalColumn: row.PrincipalColumn,
                    ConstraintName: "sqlite_fk_" + row.Id,
                    ColumnCount: rows.Count,
                    OnDelete: NormalizeReferentialAction(row.OnDelete),
                    OnUpdate: NormalizeReferentialAction(row.OnUpdate),
                    IsSyntheticConstraintName: true));
            }
        }

        private static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> QueryForeignKeysAsync(DbConnection connection, string sql)
        {
            var foreignKeys = new List<ScaffoldForeignKeyInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var dependentTable = Convert.ToString(reader["DependentTable"]);
                var dependentColumn = Convert.ToString(reader["DependentColumn"]);
                var principalTable = Convert.ToString(reader["PrincipalTable"]);
                var principalColumn = Convert.ToString(reader["PrincipalColumn"]);
                if (string.IsNullOrWhiteSpace(dependentTable)
                    || string.IsNullOrWhiteSpace(dependentColumn)
                    || string.IsNullOrWhiteSpace(principalTable)
                    || string.IsNullOrWhiteSpace(principalColumn))
                {
                    continue;
                }

                foreignKeys.Add(new ScaffoldForeignKeyInfo(
                    NullIfWhiteSpace(Convert.ToString(reader["DependentSchema"])),
                    dependentTable,
                    dependentColumn,
                    NullIfWhiteSpace(Convert.ToString(reader["PrincipalSchema"])),
                    principalTable,
                    principalColumn,
                    Convert.ToString(reader["ConstraintName"]) ?? string.Empty,
                    Convert.ToInt32(reader["ColumnCount"], CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "OnDelete") ? NormalizeReferentialAction(Convert.ToString(reader["OnDelete"])) : "NO ACTION",
                    ReaderHasColumn(reader, "OnUpdate") ? NormalizeReferentialAction(Convert.ToString(reader["OnUpdate"])) : "NO ACTION",
                    ReaderHasColumn(reader, "IsSyntheticConstraintName")
                        && Convert.ToBoolean(reader["IsSyntheticConstraintName"], CultureInfo.InvariantCulture)));
            }

            return foreignKeys;
        }

        private static string NormalizeReferentialAction(string? action)
        {
            if (string.IsNullOrWhiteSpace(action))
                return "NO ACTION";

            return action.Replace('_', ' ').Trim().ToUpperInvariant();
        }

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

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;

        private const string SqlServerForeignKeySql = """
            SELECT
                SCHEMA_NAME(dep.schema_id) AS DependentSchema,
                dep.name AS DependentTable,
                dep_col.name AS DependentColumn,
                SCHEMA_NAME(principal.schema_id) AS PrincipalSchema,
                principal.name AS PrincipalTable,
                principal_col.name AS PrincipalColumn,
                fk.name AS ConstraintName,
                fk.is_system_named AS IsSyntheticConstraintName,
                COUNT(*) OVER (PARTITION BY fk.object_id) AS ColumnCount,
                fk.delete_referential_action_desc AS OnDelete,
                fk.update_referential_action_desc AS OnUpdate
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            INNER JOIN sys.tables dep ON dep.object_id = fk.parent_object_id
            INNER JOIN sys.columns dep_col ON dep_col.object_id = dep.object_id AND dep_col.column_id = fkc.parent_column_id
            INNER JOIN sys.tables principal ON principal.object_id = fk.referenced_object_id
            INNER JOIN sys.columns principal_col ON principal_col.object_id = principal.object_id AND principal_col.column_id = fkc.referenced_column_id
            WHERE dep.is_ms_shipped = 0 AND principal.is_ms_shipped = 0
            ORDER BY SCHEMA_NAME(dep.schema_id), dep.name, fk.name, fkc.constraint_column_id
            """;

        private const string PostgresForeignKeySql = """
            SELECT
                dep_ns.nspname AS DependentSchema,
                dep.relname AS DependentTable,
                dep_att.attname AS DependentColumn,
                principal_ns.nspname AS PrincipalSchema,
                principal.relname AS PrincipalTable,
                principal_att.attname AS PrincipalColumn,
                con.conname AS ConstraintName,
                array_length(con.conkey, 1) AS ColumnCount,
                CASE con.confdeltype
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                    WHEN 'r' THEN 'RESTRICT'
                    ELSE 'NO ACTION'
                END AS OnDelete,
                CASE con.confupdtype
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                    WHEN 'r' THEN 'RESTRICT'
                    ELSE 'NO ACTION'
                END AS OnUpdate
            FROM pg_constraint con
            INNER JOIN pg_class dep ON dep.oid = con.conrelid
            INNER JOIN pg_namespace dep_ns ON dep_ns.oid = dep.relnamespace
            INNER JOIN pg_class principal ON principal.oid = con.confrelid
            INNER JOIN pg_namespace principal_ns ON principal_ns.oid = principal.relnamespace
            INNER JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS key_pair(dep_attnum, principal_attnum, ord) ON true
            INNER JOIN pg_attribute dep_att ON dep_att.attrelid = dep.oid AND dep_att.attnum = key_pair.dep_attnum
            INNER JOIN pg_attribute principal_att ON principal_att.attrelid = principal.oid AND principal_att.attnum = key_pair.principal_attnum
            WHERE con.contype = 'f'
              AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY dep_ns.nspname, dep.relname, con.conname, key_pair.ord
            """;

        private const string MySqlForeignKeySql = """
            SELECT
                NULL AS DependentSchema,
                kcu.table_name AS DependentTable,
                kcu.column_name AS DependentColumn,
                NULL AS PrincipalSchema,
                kcu.referenced_table_name AS PrincipalTable,
                kcu.referenced_column_name AS PrincipalColumn,
                kcu.constraint_name AS ConstraintName,
                COUNT(*) OVER (PARTITION BY kcu.constraint_schema, kcu.table_name, kcu.constraint_name) AS ColumnCount,
                rc.delete_rule AS OnDelete,
                rc.update_rule AS OnUpdate
            FROM information_schema.key_column_usage kcu
            INNER JOIN information_schema.referential_constraints rc
                ON rc.constraint_schema = kcu.constraint_schema
               AND rc.constraint_name = kcu.constraint_name
               AND rc.table_name = kcu.table_name
            WHERE kcu.table_schema = DATABASE()
              AND kcu.referenced_table_name IS NOT NULL
            ORDER BY kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position
            """;
    }
}
