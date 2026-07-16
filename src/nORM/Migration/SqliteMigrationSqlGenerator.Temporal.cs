using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

#nullable enable

namespace nORM.Migration
{
    public partial class SqliteMigrationSqlGenerator
    {
        /// <summary>
        /// Emits the temporal companion DDL for every temporal table the diff touches, in both
        /// directions. Trigger-emulated temporal storage keeps a <c>&lt;Table&gt;_History</c>
        /// companion plus three versioning triggers whose bodies enumerate the main table's
        /// columns; any schema change on the main table therefore has to (a) mirror the change
        /// onto the history table in lock-step (SQL Server system-versioning parity: an added
        /// column backfills history through its default, a dropped column drops its historical
        /// values) and (b) re-emit the triggers from the changed schema — a SQLite recreate drops
        /// them entirely (silently ending versioning), and even a plain ADD COLUMN leaves them
        /// enumerating the old column list (silently excluding the new column from history).
        /// </summary>
        private static void EmitTemporalDdl(
            List<string> up,
            List<string> down,
            SchemaDiff diff,
            HashSet<string> upRecreatedTables,
            HashSet<string> downRecreatedTables)
        {
            foreach (var table in diff.AddedTables.Where(static t => t.IsTemporal))
            {
                up.Add(BuildCreateHistoryTableSql(table));
                EmitTemporalTriggers(up, table);
                // Dropping the main table removes its triggers with it; only history remains.
                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name + "_History")}");
            }

            foreach (var table in diff.DroppedTables.Where(static t => t.IsTemporal))
            {
                up.Add($"DROP TABLE IF EXISTS {EscTable(table.Name + "_History")}");
                down.Add(BuildCreateHistoryTableSql(table));
                EmitTemporalTriggers(down, table);
            }

            foreach (var tableName in GetTemporalChangedTableNames(diff, upRecreatedTables, downRecreatedTables))
            {
                var newSchema = BuildNewSchema(diff, tableName);
                var oldSchema = BuildOldSchema(diff, tableName);

                var added = diff.AddedColumns.Where(x => NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();
                var dropped = diff.DroppedColumns.Where(x => NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();
                var altered = diff.AlteredColumns.Any(x => NameEquals(x.Table.Name, tableName));
                var renames = diff.RenamedColumns.Where(x => NameEquals(x.Table.Name, tableName)).ToList();
                var columnsChanged = added.Count > 0 || dropped.Count > 0 || altered || renames.Count > 0;

                // ── Up: mirror the change onto history ─────────────────────────────────────
                if (dropped.Count > 0 || altered)
                {
                    // Column removal or redefinition needs the recreate workaround on history too.
                    RecreateHistoryTable(
                        up,
                        BuildHistorySchema(newSchema),
                        addedColumnNames: added.Count > 0
                            ? added.Select(static c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase)
                            : null,
                        sourceColumnByTarget: renames.Count > 0
                            ? renames.ToDictionary(static x => x.NewColumn.Name, static x => x.OldColumnName, StringComparer.OrdinalIgnoreCase)
                            : null,
                        restoreFill: false);
                }
                else
                {
                    foreach (var (_, oldColName, newCol) in renames)
                        up.Add($"ALTER TABLE {EscTable(tableName + "_History")} RENAME COLUMN {Esc(oldColName)} TO {Esc(newCol.Name)}");
                    foreach (var column in added)
                        up.Add(BuildHistoryAddColumnSql(tableName, column));
                }

                // ── Down: revert history to the pre-migration shape ────────────────────────
                var downNeedsHistoryRecreate = added.Count > 0 || altered
                    // Restoring a dropped NOT NULL no-default column cannot use ADD COLUMN on a
                    // populated history table; recreate backfills the type-appropriate zero
                    // exactly like the main table's Down recreate.
                    || dropped.Any(static c => !c.IsNullable && string.IsNullOrEmpty(c.DefaultValue) && !IsComputedColumn(c));
                if (downNeedsHistoryRecreate)
                {
                    RecreateHistoryTable(
                        down,
                        BuildHistorySchema(oldSchema),
                        addedColumnNames: dropped.Count > 0
                            ? dropped.Select(static c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase)
                            : null,
                        sourceColumnByTarget: renames.Count > 0
                            ? renames.ToDictionary(static x => x.OldColumnName, static x => x.NewColumn.Name, StringComparer.OrdinalIgnoreCase)
                            : null,
                        restoreFill: true);
                }
                else
                {
                    foreach (var (_, oldColName, newCol) in renames)
                        down.Add($"ALTER TABLE {EscTable(tableName + "_History")} RENAME COLUMN {Esc(newCol.Name)} TO {Esc(oldColName)}");
                    foreach (var column in dropped)
                        down.Add(BuildHistoryAddColumnSql(tableName, column));
                }

                // ── Triggers: re-emit from the direction's schema ──────────────────────────
                // A recreate dropped them outright; a column-set change left them enumerating
                // stale columns. Either way the direction's post-state schema is authoritative.
                if (upRecreatedTables.Contains(tableName) || columnsChanged)
                    EmitTemporalTriggers(up, newSchema);
                if (downRecreatedTables.Contains(tableName) || columnsChanged)
                    EmitTemporalTriggers(down, oldSchema);
            }
        }

        /// <summary>
        /// Temporal tables touched by column-level or recreate-driving changes in either
        /// direction. Added/dropped tables are handled separately by the caller.
        /// </summary>
        private static IReadOnlyList<string> GetTemporalChangedTableNames(
            SchemaDiff diff,
            HashSet<string> upRecreatedTables,
            HashSet<string> downRecreatedTables)
        {
            var temporalNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            void Consider(TableSchema table)
            {
                if (table.IsTemporal)
                    temporalNames.Add(table.Name);
            }

            foreach (var (table, _) in diff.AddedColumns) Consider(table);
            foreach (var (table, _) in diff.DroppedColumns) Consider(table);
            foreach (var (table, _, _) in diff.AlteredColumns) Consider(table);
            foreach (var (table, _, _) in diff.RenamedColumns) Consider(table);
            foreach (var (table, _) in diff.AddedForeignKeys) Consider(table);
            foreach (var (table, _) in diff.DroppedForeignKeys) Consider(table);
            foreach (var (table, _) in diff.AddedCheckConstraints) Consider(table);
            foreach (var (table, _) in diff.DroppedCheckConstraints) Consider(table);

            return temporalNames
                .Where(name => upRecreatedTables.Contains(name)
                    || downRecreatedTables.Contains(name)
                    || diff.AddedColumns.Any(x => NameEquals(x.Table.Name, name))
                    || diff.DroppedColumns.Any(x => NameEquals(x.Table.Name, name))
                    || diff.RenamedColumns.Any(x => NameEquals(x.Table.Name, name)))
                .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        /// <summary>
        /// The history companion's schema for a given main-table schema: the four temporal system
        /// columns followed by the entity columns stripped of key/identity/uniqueness/index/computed
        /// metadata — history rows repeat entity keys across versions and store computed values as
        /// plain data. Matches the runtime bootstrap's history shape.
        /// </summary>
        private static TableSchema BuildHistorySchema(TableSchema main)
        {
            var history = new TableSchema { Name = main.Name + "_History" };
            history.Columns.Add(new ColumnSchema { Name = "__VersionId", ClrType = typeof(long).FullName!, IsPrimaryKey = true, IsIdentity = true });
            history.Columns.Add(new ColumnSchema { Name = "__ValidFrom", ClrType = typeof(string).FullName!, IsNullable = false });
            history.Columns.Add(new ColumnSchema { Name = "__ValidTo", ClrType = typeof(string).FullName!, IsNullable = false });
            history.Columns.Add(new ColumnSchema { Name = "__Operation", ClrType = typeof(string).FullName!, IsNullable = false });
            foreach (var column in main.Columns)
            {
                history.Columns.Add(new ColumnSchema
                {
                    Name = column.Name,
                    ClrType = column.ClrType,
                    MaxLength = column.MaxLength,
                    IsUnicode = column.IsUnicode,
                    IsFixedLength = column.IsFixedLength,
                    Precision = column.Precision,
                    Scale = column.Scale,
                    IsNullable = column.IsNullable,
                    DefaultValue = column.DefaultValue
                });
            }
            return history;
        }

        /// <summary>CREATE TABLE IF NOT EXISTS for a temporal table's history companion.</summary>
        private static string BuildCreateHistoryTableSql(TableSchema main)
        {
            var history = BuildHistorySchema(main);
            var colDefs = history.Columns.Select(static c =>
            {
                if (c.IsIdentity && c.IsPrimaryKey)
                    return $"{Esc(c.Name)} {GetSqlType(c)} NOT NULL PRIMARY KEY AUTOINCREMENT";
                var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                    ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                    : "";
                return $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
            });
            return $"CREATE TABLE IF NOT EXISTS {EscTable(history.Name)} ({string.Join(", ", colDefs)})";
        }

        /// <summary>
        /// ALTER TABLE ADD COLUMN on the history companion, mirroring the main table's added
        /// column. The same DEFAULT backfills existing history rows (SQL Server system-versioning
        /// parity); uniqueness and index metadata deliberately do not carry over.
        /// </summary>
        private static string BuildHistoryAddColumnSql(string tableName, ColumnSchema column)
        {
            var nullPart = column.IsNullable
                ? (!string.IsNullOrEmpty(column.DefaultValue)
                    ? $"NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                    : "NULL")
                : $"NOT NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}";
            return $"ALTER TABLE {EscTable(tableName + "_History")} ADD COLUMN {Esc(column.Name)} {GetSqlType(column)} {nullPart}";
        }

        /// <summary>
        /// Recreates the history table with the given target schema, preserving the temporal
        /// system columns and every surviving entity column's data. No FK/CHECK/index metadata:
        /// history companions carry none.
        /// </summary>
        private static void RecreateHistoryTable(
            List<string> stmts,
            TableSchema historySchema,
            IReadOnlySet<string>? addedColumnNames,
            IReadOnlyDictionary<string, string>? sourceColumnByTarget,
            bool restoreFill)
        {
            RecreateTable(
                stmts,
                historySchema,
                historySchema.Columns.ToList(),
                overrides: null,
                fks: Array.Empty<ForeignKeySchema>(),
                checks: Array.Empty<CheckConstraintSchema>(),
                explicitIndexes: Array.Empty<(string, bool, string[], bool[], Configuration.IndexNullSortOrder[], string[], bool, string?)>(),
                expressionIndexes: Array.Empty<ExpressionIndexSchema>(),
                addedColumnNames: addedColumnNames,
                sourceColumnByTarget: sourceColumnByTarget,
                restoreFill: restoreFill);
        }

        /// <summary>
        /// Drops and re-creates the three versioning triggers from the given main-table schema.
        /// The runtime bootstrap's CREATE TRIGGER IF NOT EXISTS would keep a stale trigger alive,
        /// so the drop is explicit.
        /// </summary>
        private static void EmitTemporalTriggers(List<string> stmts, TableSchema schema)
        {
            foreach (var triggerName in SqliteTemporalDdl.GetTriggerNames(schema.Name))
                stmts.Add($"DROP TRIGGER IF EXISTS {Esc(triggerName)}");
            stmts.AddRange(SqliteTemporalDdl.BuildTriggersSql(
                Esc,
                schema.Name,
                schema.Columns.Select(static c => c.Name).ToArray(),
                schema.Columns.Where(static c => c.IsPrimaryKey).Select(static c => c.Name).ToArray(),
                schema.TenantColumnName));
        }
    }
}
