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
                var cols = table.Columns.Select(c =>
                    $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
                up.Add($"CREATE TABLE \"{table.Name}\" ({string.Join(", ", cols)})");
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

                var remainingColumns = table.Columns
                    .Where(c => !addedColumnNames.Contains(c.Name))
                    .ToArray();

                var remainingDefs = remainingColumns
                    .Select(c => $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
                var remainingNames = remainingColumns.Select(c => $"\"{c.Name}\"").ToArray();

                down.Add("PRAGMA foreign_keys=off");
                down.Add($"CREATE TABLE \"__temp__{table.Name}\" ({string.Join(", ", remainingDefs)})");
                down.Add($"INSERT INTO \"__temp__{table.Name}\" ({string.Join(", ", remainingNames)}) SELECT {string.Join(", ", remainingNames)} FROM \"{table.Name}\"");
                down.Add($"DROP TABLE \"{table.Name}\"");
                down.Add($"ALTER TABLE \"__temp__{table.Name}\" RENAME TO \"{table.Name}\"");
                down.Add("PRAGMA foreign_keys=on");
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
                // Down: recreate the table
                var cols = table.Columns.Select(c =>
                    $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
                down.Add($"CREATE TABLE \"{table.Name}\" ({string.Join(", ", cols)})");
            }

            // SD-8: Generate DROP COLUMN for columns removed in the new snapshot.
            // SQLite does not support ALTER TABLE ... DROP COLUMN before version 3.35.0,
            // so we use the table-recreation workaround for broad compatibility.
            foreach (var group in diff.DroppedColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var newTable = diff.DroppedColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var droppedColNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

                // The columns remaining in the new table (without the dropped ones)
                var remainingCols = newTable.Columns
                    .Where(c => !droppedColNames.Contains(c.Name))
                    .ToArray();

                var remainingDefs = remainingCols
                    .Select(c => $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
                var remainingNames = remainingCols.Select(c => $"\"{c.Name}\"").ToArray();

                up.Add("PRAGMA foreign_keys=off");
                up.Add($"CREATE TABLE \"__temp__{newTable.Name}\" ({string.Join(", ", remainingDefs)})");
                up.Add($"INSERT INTO \"__temp__{newTable.Name}\" ({string.Join(", ", remainingNames)}) SELECT {string.Join(", ", remainingNames)} FROM \"{newTable.Name}\"");
                up.Add($"DROP TABLE \"{newTable.Name}\"");
                up.Add($"ALTER TABLE \"__temp__{newTable.Name}\" RENAME TO \"{newTable.Name}\"");
                up.Add("PRAGMA foreign_keys=on");

                // Down: add the dropped columns back (SQLite ADD COLUMN)
                foreach (var droppedCol in group.Select(g => g.Column))
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
        /// </summary>
        private static void AddRecreate(List<string> stmts, TableSchema table, Dictionary<string, ColumnSchema> overrides)
        {
            var cols  = table.Columns.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();
            var defs  = cols.Select(c => $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
            var names = cols.Select(c => $"\"{c.Name}\"");
            stmts.Add("PRAGMA foreign_keys=off");
            stmts.Add($"CREATE TABLE \"__temp__{table.Name}\" ({string.Join(", ", defs)})");
            stmts.Add($"INSERT INTO \"__temp__{table.Name}\" SELECT {string.Join(", ", names)} FROM \"{table.Name}\"");
            stmts.Add($"DROP TABLE \"{table.Name}\"");
            stmts.Add($"ALTER TABLE \"__temp__{table.Name}\" RENAME TO \"{table.Name}\"");
            stmts.Add("PRAGMA foreign_keys=on");
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
