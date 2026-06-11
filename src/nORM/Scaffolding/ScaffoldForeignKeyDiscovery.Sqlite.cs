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
    }
}
