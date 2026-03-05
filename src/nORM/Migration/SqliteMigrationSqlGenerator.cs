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

        /// <summary>
        /// Creates SQLite SQL statements for the operations described by the schema diff.
        /// </summary>
        /// <param name="diff">The set of schema changes to be applied.</param>
        /// <returns>The statements needed to apply and rollback the changes.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            var up = new List<string>();
            var down = new List<string>();

            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                    $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

                // MIG-1: Emit PRIMARY KEY constraint for PK columns
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => $"\"{c.Name}\""))})");

                // MIG-1: Emit UNIQUE constraint for unique non-PK columns
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => $"\"{c.Name}\""))})");

                up.Add($"CREATE TABLE \"{table.Name}\" ({string.Join(", ", colDefs)})");

                // MIG-1: Emit CREATE INDEX for columns with a named index (non-PK, non-unique)
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    up.Add($"CREATE INDEX \"{col.IndexName}\" ON \"{table.Name}\" (\"{col.Name}\")");

                down.Add($"DROP TABLE IF EXISTS \"{table.Name}\"");
            }

            foreach (var group in diff.AddedColumns.GroupBy(x => x.Table))
            {
                var table = group.Key;
                var addedColumnNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var (_, column) in group)
                {
                    var colDef = $"\"{column.Name}\" {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                    up.Add($"ALTER TABLE \"{table.Name}\" ADD COLUMN {colDef}");
                }

                // Down: undo the ADD COLUMN by recreating the table without those columns.
                // Use RecreateTable so that PK/UNIQUE/INDEX constraints are preserved.
                var remainingColumns = table.Columns
                    .Where(c => !addedColumnNames.Contains(c.Name))
                    .ToList();
                RecreateTable(down, table, remainingColumns, null);
            }

            // G2: SQLite does not support ALTER COLUMN; use the standard table-recreation workaround.
            foreach (var group in diff.AlteredColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table         = diff.AlteredColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var alteredMap    = group.ToDictionary(x => x.NewColumn.Name, x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
                var oldAlteredMap = group.ToDictionary(x => x.OldColumn.Name, x => x.OldColumn, StringComparer.OrdinalIgnoreCase);

                AddRecreate(up,   table, alteredMap);
                AddRecreate(down, table, oldAlteredMap);
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE IF EXISTS \"{table.Name}\"");
                // Down: recreate the table with full constraint metadata
                var colDefs = table.Columns.Select(c =>
                    $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => $"\"{c.Name}\""))})");
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => $"\"{c.Name}\""))})");
                down.Add($"CREATE TABLE \"{table.Name}\" ({string.Join(", ", colDefs)})");
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    down.Add($"CREATE INDEX \"{col.IndexName}\" ON \"{table.Name}\" (\"{col.Name}\")");
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
                RecreateTable(up, newTable, remainingCols, null);

                // Down: add the dropped columns back (SQLite ADD COLUMN is forward-compatible)
                foreach (var droppedCol in droppedCols)
                {
                    var colDef = $"\"{droppedCol.Name}\" {GetSqlType(droppedCol)} {(droppedCol.IsNullable ? "NULL" : "NOT NULL")}";
                    down.Add($"ALTER TABLE \"{newTable.Name}\" ADD COLUMN {colDef}");
                }
            }

            return new MigrationSqlStatements(up, down);
        }

        /// <summary>
        /// G2: Generates the SQLite table-recreation sequence (PRAGMA off, CREATE temp, INSERT,
        /// DROP original, RENAME temp, PRAGMA on) required to change an existing column definition.
        /// MIG-1: Now emits full schema including PRIMARY KEY, UNIQUE, and CREATE INDEX constraints,
        /// identical to the AddedTables path, so constraints are preserved through ALTER operations.
        /// </summary>
        private static void AddRecreate(List<string> stmts, TableSchema table, Dictionary<string, ColumnSchema> overrides)
        {
            var cols = table.Columns.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();
            RecreateTable(stmts, table, cols, null);
        }

        /// <summary>
        /// Emits the full SQLite table-recreation sequence: PRAGMA foreign_keys=off,
        /// CREATE TABLE __temp__ (with PRIMARY KEY, UNIQUE, CREATE INDEX),
        /// INSERT INTO __temp__ SELECT, DROP TABLE, RENAME TO, PRAGMA foreign_keys=on,
        /// followed by any CREATE INDEX statements.
        /// </summary>
        /// <param name="stmts">List to append statements to.</param>
        /// <param name="table">The original table (used for name and INSERT SELECT column list).</param>
        /// <param name="cols">The effective column list for the recreated table (may differ from table.Columns for drop/alter paths).</param>
        /// <param name="overrides">Optional per-column overrides (pass null if <paramref name="cols"/> is already the resolved list).</param>
        private static void RecreateTable(List<string> stmts, TableSchema table, List<ColumnSchema> cols, Dictionary<string, ColumnSchema>? overrides)
        {
            // Apply overrides if supplied
            if (overrides != null)
                cols = cols.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();

            var names = cols.Select(c => $"\"{c.Name}\"");

            // MIG-1: Build column definitions with full constraint metadata (same as AddedTables path)
            var colDefs = cols.Select(c =>
                $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

            // MIG-1: Emit PRIMARY KEY constraint for PK columns
            var pkCols = cols.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0)
                colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => $"\"{c.Name}\""))})");

            // MIG-1: Emit UNIQUE constraint for unique non-PK columns
            var uniqueNonPkCols = cols.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
            if (uniqueNonPkCols.Count > 0)
                colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => $"\"{c.Name}\""))})");

            stmts.Add("PRAGMA foreign_keys=off");
            stmts.Add($"CREATE TABLE \"__temp__{table.Name}\" ({string.Join(", ", colDefs)})");
            stmts.Add($"INSERT INTO \"__temp__{table.Name}\" SELECT {string.Join(", ", names)} FROM \"{table.Name}\"");
            stmts.Add($"DROP TABLE \"{table.Name}\"");
            stmts.Add($"ALTER TABLE \"__temp__{table.Name}\" RENAME TO \"{table.Name}\"");
            stmts.Add("PRAGMA foreign_keys=on");

            // MIG-1: Emit CREATE INDEX for columns with a named index (non-PK, non-unique)
            foreach (var col in cols.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                stmts.Add($"CREATE INDEX \"{col.IndexName}\" ON \"{table.Name}\" (\"{col.Name}\")");
        }

        /// <summary>
        /// Converts the supplied <see cref="ColumnSchema"/> into a SQLite column type. When
        /// an exact mapping is not found, the method defaults to <c>TEXT</c>, which can store
        /// arbitrary data.
        /// </summary>
        /// <param name="column">The column definition to map.</param>
        /// <returns>The SQLite data type as a string.</returns>
        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "TEXT";
    }
}
