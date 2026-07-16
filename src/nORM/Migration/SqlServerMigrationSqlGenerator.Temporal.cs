using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

#nullable enable

namespace nORM.Migration
{
    public partial class SqlServerMigrationSqlGenerator
    {
        /// <summary>
        /// Emits the temporal companion DDL for every temporal table the diff touches. SQL Server
        /// ALTERs do not drop triggers, but the versioning triggers enumerate the main table's
        /// column list, so any column-set change leaves them silently excluding (or referencing)
        /// the wrong columns - and the history table must mirror the main table's shape in
        /// lock-step or the trigger INSERTs fail / silently lose the new column's values.
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
                var history = tableName + "_History";
                var added = diff.AddedColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();
                var dropped = diff.DroppedColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();
                var altered = diff.AlteredColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).ToList();
                var renames = diff.RenamedColumns.Where(x => TemporalMigrationSchema.NameEquals(x.Table.Name, tableName)).ToList();

                foreach (var (_, oldColName, newCol) in renames)
                {
                    up.Add($"EXEC sp_rename '{EscLiteral(history)}.{EscLiteral(oldColName)}', '{EscLiteral(newCol.Name)}', 'COLUMN'");
                    down.Add($"EXEC sp_rename '{EscLiteral(history)}.{EscLiteral(newCol.Name)}', '{EscLiteral(oldColName)}', 'COLUMN'");
                }
                foreach (var column in dropped)
                {
                    EmitHistoryDropColumn(up, history, column.Name);
                    down.Add(BuildHistoryAddColumnSql(history, column));
                }
                foreach (var (_, newCol, oldCol) in altered)
                {
                    up.Add($"ALTER TABLE {EscTable(history)} ALTER COLUMN {Esc(newCol.Name)} {GetSqlType(newCol)}{FormatCollation(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}");
                    down.Add($"ALTER TABLE {EscTable(history)} ALTER COLUMN {Esc(oldCol.Name)} {GetSqlType(oldCol)}{FormatCollation(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}");
                }
                foreach (var column in added)
                {
                    up.Add(BuildHistoryAddColumnSql(history, column));
                    EmitHistoryDropColumn(down, history, column.Name);
                }

                if (added.Count > 0 || dropped.Count > 0 || renames.Count > 0)
                {
                    up.AddRange(BuildTemporalTriggerStatements(TemporalMigrationSchema.BuildNewSchema(diff, tableName)));
                    down.AddRange(BuildTemporalTriggerStatements(TemporalMigrationSchema.BuildOldSchema(diff, tableName)));
                }
            }
        }

        private static IEnumerable<string> BuildTemporalTriggerStatements(TableSchema schema)
            => SqlServerTemporalDdl.BuildTriggerStatements(
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
    {Esc("__VersionId")} BIGINT IDENTITY(1,1) PRIMARY KEY,
    {Esc("__ValidFrom")} DATETIME2 NOT NULL,
    {Esc("__ValidTo")} DATETIME2 NOT NULL,
    {Esc("__Operation")} CHAR(1) NOT NULL,
    {columns}
)";
        }

        /// <summary>
        /// ADD on the history companion, mirroring the main table's column. The same DEFAULT
        /// backfills existing history rows (SQL Server system-versioning parity); the default is
        /// unnamed so the drop guard can discover it dynamically.
        /// </summary>
        private static string BuildHistoryAddColumnSql(string historyTable, ColumnSchema column)
        {
            var defaultPart = !string.IsNullOrEmpty(column.DefaultValue)
                ? $" DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                : "";
            var withValues = !string.IsNullOrEmpty(column.DefaultValue) && !column.IsNullable ? " WITH VALUES" : "";
            return $"ALTER TABLE {EscTable(historyTable)} ADD {Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}{withValues}";
        }

        /// <summary>DROP COLUMN on history behind the dynamic default-constraint drop guard.</summary>
        private static void EmitHistoryDropColumn(List<string> stmts, string historyTable, string columnName)
        {
            var dropVar = $"@__drop_{new string(columnName.Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant()}";
            if (dropVar.Length > 128) dropVar = dropVar.Substring(0, 128);
            stmts.Add($"DECLARE {dropVar} NVARCHAR({ConstraintNameVarMaxLength}) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{EscLiteral(historyTable)}') AND COL_NAME(parent_object_id,parent_column_id)='{EscLiteral(columnName)}') IF {dropVar} IS NOT NULL EXEC('ALTER TABLE {EscTable(historyTable)} DROP CONSTRAINT ['+{dropVar}+']')");
            stmts.Add($"ALTER TABLE {EscTable(historyTable)} DROP COLUMN {Esc(columnName)}");
        }
    }
}
