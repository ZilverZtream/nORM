using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    /// <summary>
    /// Generates SQLite-specific SQL statements to apply and roll back schema changes.
    /// </summary>
    public class SqliteMigrationSqlGenerator : IMigrationSqlGenerator
    {
        private static readonly Dictionary<string, string> TypeMap = new()
        {
            { typeof(int).FullName!, "INTEGER" },
            { typeof(long).FullName!, "INTEGER" },
            { typeof(short).FullName!, "INTEGER" },
            { typeof(byte).FullName!, "INTEGER" },
            { typeof(bool).FullName!, "INTEGER" },
            { typeof(string).FullName!, "TEXT" },
            { typeof(DateTime).FullName!, "TEXT" },
            { typeof(decimal).FullName!, "NUMERIC" },
            { typeof(double).FullName!, "REAL" },
            { typeof(float).FullName!, "REAL" },
            { typeof(Guid).FullName!, "TEXT" }
        };

        // PRV-1: Escape SQLite identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id) => $"\"{id.Replace("\"", "\"\"")}\"";

        /// <summary>
        /// Creates SQLite SQL statements for the operations described by the schema diff.
        /// MG-2: PRAGMA foreign_keys=off/on are returned in PreTransactionUp/Down and PostTransactionUp/Down
        /// segments so callers can execute them outside the migration transaction. The Up/Down lists
        /// contain only transactional DDL statements (no PRAGMA).
        /// </summary>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            var up = new List<string>();
            var down = new List<string>();
            bool needsUpFkPragma = false;
            bool needsDownFkPragma = false;

            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                    $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

                // MIG-1: Emit PRIMARY KEY constraint for PK columns
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");

                // MIG-1: Emit UNIQUE constraint for unique non-PK columns
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");

                up.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");

                // MIG-1: Emit CREATE INDEX for columns with a named index (non-PK, non-unique)
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    up.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");

                down.Add($"DROP TABLE IF EXISTS {Esc(table.Name)}");
            }

            foreach (var group in diff.AddedColumns.GroupBy(x => x.Table))
            {
                var table = group.Key;
                var addedColumnNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var (_, column) in group)
                {
                    // MIG-1: NOT NULL column without a DefaultValue cannot be added to a populated table.
                    if (!column.IsNullable && column.DefaultValue == null)
                        throw new InvalidOperationException(
                            $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                            "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");

                    var nullPart = column.IsNullable ? "NULL" : $"NOT NULL DEFAULT {column.DefaultValue}";
                    var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {nullPart}";
                    up.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
                }

                // Down: undo the ADD COLUMN by recreating the table without those columns.
                // MG-2: RecreateTable no longer emits PRAGMA inline.
                var remainingColumns = table.Columns
                    .Where(c => !addedColumnNames.Contains(c.Name))
                    .ToList();
                RecreateTable(down, table, remainingColumns, null);
                needsDownFkPragma = true;
            }

            // G2: SQLite does not support ALTER COLUMN; use the standard table-recreation workaround.
            foreach (var group in diff.AlteredColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table         = diff.AlteredColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var alteredMap    = group.ToDictionary(x => x.NewColumn.Name, x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
                var oldAlteredMap = group.ToDictionary(x => x.OldColumn.Name, x => x.OldColumn, StringComparer.OrdinalIgnoreCase);

                AddRecreate(up,   table, alteredMap);
                AddRecreate(down, table, oldAlteredMap);
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE IF EXISTS {Esc(table.Name)}");
                // Down: recreate the table with full constraint metadata
                var colDefs = table.Columns.Select(c =>
                    $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");
                down.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    down.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");
            }

            // SD-8: Generate DROP COLUMN for columns removed in the new snapshot.
            // SQLite does not support ALTER TABLE ... DROP COLUMN before version 3.35.0,
            // so we use the table-recreation workaround for broad compatibility.
            foreach (var group in diff.DroppedColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var newTable = diff.DroppedColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var droppedColNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var droppedCols = group.Select(g => g.Column).ToList();

                // The columns remaining in the new table (without the dropped ones)
                var remainingCols = newTable.Columns
                    .Where(c => !droppedColNames.Contains(c.Name))
                    .ToList();

                // Up: recreate table without dropped columns — use RecreateTable for full constraint preservation
                // MG-2: RecreateTable no longer emits PRAGMA inline.
                RecreateTable(up, newTable, remainingCols, null);
                needsUpFkPragma = true;

                // Down: add the dropped columns back (SQLite ADD COLUMN is forward-compatible)
                foreach (var droppedCol in droppedCols)
                {
                    var colDef = $"{Esc(droppedCol.Name)} {GetSqlType(droppedCol)} {(droppedCol.IsNullable ? "NULL" : "NOT NULL")}";
                    down.Add($"ALTER TABLE {Esc(newTable.Name)} ADD COLUMN {colDef}");
                }
            }

            // MG-2: Return PRAGMA statements in pre/post transaction segments so callers can
            // execute them outside the migration transaction (required by SQLite documentation).
            var preUp    = needsUpFkPragma   ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=off" } : null;
            var postUp   = needsUpFkPragma   ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=on" }  : null;
            var preDown  = needsDownFkPragma ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=off" } : null;
            var postDown = needsDownFkPragma ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=on" }  : null;

            return new MigrationSqlStatements(up, down, preUp, postUp, preDown, postDown);
        }

        /// <summary>
        /// G2: Generates the SQLite table-recreation DDL sequence for changed columns.
        /// MIG-1: Now emits full schema including PRIMARY KEY, UNIQUE, and CREATE INDEX constraints.
        /// </summary>
        private static void AddRecreate(List<string> stmts, TableSchema table, Dictionary<string, ColumnSchema> overrides)
        {
            var cols = table.Columns.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();
            RecreateTable(stmts, table, cols, null);
        }

        /// <summary>
        /// Emits the SQLite table-recreation DDL sequence (CREATE temp, INSERT, DROP, RENAME)
        /// WITHOUT PRAGMA statements. The caller is responsible for setting needsUpFkPragma/needsDownFkPragma
        /// which causes PRAGMA foreign_keys=off/on to be emitted in the pre/post-transaction segments.
        /// MG-2: PRAGMA statements removed from this method to allow callers to place them
        /// outside the migration transaction.
        /// </summary>
        private static void RecreateTable(List<string> stmts, TableSchema table, List<ColumnSchema> cols, Dictionary<string, ColumnSchema>? overrides)
        {
            // Apply overrides if supplied
            if (overrides != null)
                cols = cols.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();

            var names = cols.Select(c => Esc(c.Name));

            // MIG-1: Build column definitions with full constraint metadata (same as AddedTables path)
            var colDefs = cols.Select(c =>
                $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

            // MIG-1: Emit PRIMARY KEY constraint for PK columns
            var pkCols = cols.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0)
                colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");

            // MIG-1: Emit UNIQUE constraint for unique non-PK columns
            var uniqueNonPkCols = cols.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
            if (uniqueNonPkCols.Count > 0)
                colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");

            // MG-2: No PRAGMA here — PRAGMA foreign_keys=off/on is returned in the pre/post transaction segments.
            var tempName = $"\"__temp__{table.Name.Replace("\"", "\"\"")}\"";
            stmts.Add($"CREATE TABLE {tempName} ({string.Join(", ", colDefs)})");
            stmts.Add($"INSERT INTO {tempName} SELECT {string.Join(", ", names)} FROM {Esc(table.Name)}");
            stmts.Add($"DROP TABLE {Esc(table.Name)}");
            stmts.Add($"ALTER TABLE {tempName} RENAME TO {Esc(table.Name)}");

            // MIG-1: Emit CREATE INDEX for columns with a named index (non-PK, non-unique)
            foreach (var col in cols.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                stmts.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");
        }

        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "TEXT";
    }
}
