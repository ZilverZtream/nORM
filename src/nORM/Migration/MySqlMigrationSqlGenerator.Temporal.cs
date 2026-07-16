using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

#nullable enable

namespace nORM.Migration
{
    public partial class MySqlMigrationSqlGenerator
    {
        /// <summary>
        /// Emits the temporal companion DDL for every temporal table the diff touches. MySQL
        /// ALTERs do not drop triggers, but the versioning triggers enumerate the main table's
        /// column list, so any column-set change leaves them stale - and the history table must
        /// mirror the main table in lock-step or the trigger INSERTs fail / silently lose the
        /// new column's values.
        /// </summary>
        private static void EmitTemporalDdl(List<string> up, List<string> down, SchemaDiff diff)
        {
            foreach (var table in diff.AddedTables.Where(static t => t.IsTemporal))
            {
                up.Add(BuildCreateHistoryTableSql(table));
                up.AddRange(BuildTemporalTriggerStatements(table));
                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name + "_History")}");
            }
            foreach (var table in diff.DroppedTables.Where(static t => t.IsTemporal))
            {
                up.Add($"DROP TABLE IF EXISTS {EscTable(table.Name + "_History")}");
                down.Add(BuildCreateHistoryTableSql(table));
                down.AddRange(BuildTemporalTriggerStatements(table));
            }

            foreach (var tableName in TemporalMigrationSchema.GetTemporalChangedTableNames(diff))
            {
                var history = EscTable(tableName + "_History");
                var added = diff.AddedColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();
                var dropped = diff.DroppedColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();
                var altered = diff.AlteredColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).ToList();
                var renames = diff.RenamedColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).ToList();

                foreach (var (_, oldColName, newCol) in renames)
                {
                    up.Add($"ALTER TABLE {history} RENAME COLUMN {Esc(oldColName)} TO {Esc(newCol.Name)}");
                    down.Add($"ALTER TABLE {history} RENAME COLUMN {Esc(newCol.Name)} TO {Esc(oldColName)}");
                }
                foreach (var column in dropped)
                {
                    up.Add($"ALTER TABLE {history} DROP COLUMN {Esc(column.Name)}");
                    down.Add(BuildHistoryAddColumnSql(history, column));
                }
                foreach (var (_, newCol, oldCol) in altered)
                {
                    // Mirrors the main generator's altered-column strategy (DROP + ADD).
                    up.Add($"ALTER TABLE {history} DROP COLUMN {Esc(oldCol.Name)}");
                    up.Add(BuildHistoryAddColumnSql(history, newCol));
                    down.Add($"ALTER TABLE {history} DROP COLUMN {Esc(newCol.Name)}");
                    down.Add(BuildHistoryAddColumnSql(history, oldCol));
                }
                foreach (var column in added)
                {
                    up.Add(BuildHistoryAddColumnSql(history, column));
                    down.Add($"ALTER TABLE {history} DROP COLUMN {Esc(column.Name)}");
                }

                if (added.Count > 0 || dropped.Count > 0 || renames.Count > 0)
                {
                    up.AddRange(BuildTemporalTriggerStatements(TemporalMigrationSchema.BuildNewSchema(diff, tableName)));
                    down.AddRange(BuildTemporalTriggerStatements(TemporalMigrationSchema.BuildOldSchema(diff, tableName)));
                }
            }
        }

        private static IEnumerable<string> BuildTemporalTriggerStatements(TableSchema schema)
            => MySqlTemporalDdl.BuildTriggerStatements(
                Esc,
                schema.Name,
                schema.Columns.Select(static c => c.Name).ToArray(),
                schema.Columns.Where(static c => c.IsPrimaryKey).Select(static c => c.Name).ToArray(),
                schema.TenantColumnName);

        /// <summary>History companion: temporal system columns + plain entity columns.</summary>
        private static string BuildCreateHistoryTableSql(TableSchema main)
        {
            var columns = string.Join(",\n    ", main.Columns.Select(static c =>
                $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)}{(c.IsNullable ? "" : " NOT NULL")}"));
            return $@"CREATE TABLE {EscTable(main.Name + "_History")} (
    `__VersionId` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `__ValidFrom` DATETIME(6) NOT NULL,
    `__ValidTo` DATETIME(6) NOT NULL,
    `__Operation` CHAR(1) NOT NULL,
    {columns}
) ENGINE=InnoDB";
        }

        /// <summary>
        /// ADD COLUMN on the history companion. The same DEFAULT backfills existing history rows
        /// (SQL Server system-versioning parity).
        /// </summary>
        private static string BuildHistoryAddColumnSql(string escapedHistoryTable, ColumnSchema column)
        {
            var defaultPart = !string.IsNullOrEmpty(column.DefaultValue)
                ? $" DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                : "";
            return $"ALTER TABLE {escapedHistoryTable} ADD COLUMN {Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
        }
    }
}
