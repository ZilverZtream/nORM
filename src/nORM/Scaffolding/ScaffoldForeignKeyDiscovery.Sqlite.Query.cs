#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private readonly record struct SqliteForeignKeyRow(
            long Id,
            long Seq,
            string PrincipalTable,
            string DependentColumn,
            string PrincipalColumn,
            string OnUpdate,
            string OnDelete,
            string Match);

        private static async Task<string?> GetSqliteCreateTableSqlAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table)
        {
            await using var command = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(table.Schema) ? "main" : table.Schema!;
            command.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name = @tableName";
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = table.Name;
            command.Parameters.Add(parameter);
            return await command.ExecuteScalarAsync().ConfigureAwait(false) as string;
        }

        /// <summary>
        /// Reads a table's primary-key column names in key order (PRAGMA table_info pk ordinals). Used to
        /// back-fill the principal column of a foreign key declared WITHOUT an explicit parent column list
        /// (<c>REFERENCES parent</c>), which SQLite reports with a NULL "to" — that implicit form references
        /// the parent's primary key. Returns an empty list when the table has no declared primary key.
        /// </summary>
        private static async Task<IReadOnlyList<string>> ReadSqlitePrimaryKeyColumnsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schema,
            string tableName)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(provider, schema, "table_info", tableName);
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            // pk is the 1-based position of the column within the primary key (0 for non-key columns).
            var keyed = new List<(long Ordinal, string Name)>();
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var pk = Convert.ToInt64(reader["pk"], CultureInfo.InvariantCulture);
                if (pk > 0)
                    keyed.Add((pk, Convert.ToString(reader["name"]) ?? string.Empty));
            }
            keyed.Sort(static (a, b) => a.Ordinal.CompareTo(b.Ordinal));
            return keyed.Select(static k => k.Name).ToArray();
        }

        /// <summary>
        /// Fills in the principal columns of a foreign key declared without an explicit parent column list
        /// (<c>REFERENCES parent</c>, which SQLite reports with every "to" NULL) by mapping the FK's columns
        /// positionally onto the parent table's primary key — exactly the semantics SQLite applies at runtime.
        /// Without this the whole group is silently dropped (no reference navigation, no <c>HasForeignKey</c>,
        /// and not even a suppression diagnostic, since the rows never reach discovery). Leaves the group
        /// untouched when it is already fully specified, when the parent has no primary key, or when the key
        /// arity does not match — in the last two cases the rows stay blank and are dropped as genuinely
        /// unresolvable rather than mapped to the wrong column.
        /// </summary>
        private static async Task<SqliteForeignKeyRow[]> BackfillImplicitPrincipalColumnsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schema,
            SqliteForeignKeyRow[] ordered)
        {
            if (ordered.Length == 0 || ordered.Any(static r => !string.IsNullOrWhiteSpace(r.PrincipalColumn)))
                return ordered;
            var principalTable = ordered[0].PrincipalTable;
            if (string.IsNullOrWhiteSpace(principalTable))
                return ordered;

            var pkColumns = await ReadSqlitePrimaryKeyColumnsAsync(connection, provider, schema, principalTable).ConfigureAwait(false);
            if (pkColumns.Count != ordered.Length)
                return ordered;

            var result = new SqliteForeignKeyRow[ordered.Length];
            for (var i = 0; i < ordered.Length; i++)
                result[i] = ordered[i] with { PrincipalColumn = pkColumns[i] };
            return result;
        }

        private static async Task<IReadOnlyList<SqliteForeignKeyRow>> ReadSqliteForeignKeyRowsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(provider, table.Schema, "foreign_key_list", table.Name);
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            var rows = new List<SqliteForeignKeyRow>();
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                rows.Add(new SqliteForeignKeyRow(
                    Convert.ToInt64(reader["id"], CultureInfo.InvariantCulture),
                    Convert.ToInt64(reader["seq"], CultureInfo.InvariantCulture),
                    Convert.ToString(reader["table"]) ?? string.Empty,
                    Convert.ToString(reader["from"]) ?? string.Empty,
                    Convert.ToString(reader["to"]) ?? string.Empty,
                    Convert.ToString(reader["on_update"]) ?? string.Empty,
                    Convert.ToString(reader["on_delete"]) ?? string.Empty,
                    ReaderHasColumn(reader, "match") ? Convert.ToString(reader["match"]) ?? string.Empty : string.Empty));
            }

            return rows;
        }
    }
}
