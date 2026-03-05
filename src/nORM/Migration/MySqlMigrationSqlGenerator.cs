using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    /// <summary>
    /// Generates MySQL-compatible SQL statements for applying schema migrations.
    /// This implementation translates high level <see cref="SchemaDiff"/> objects
    /// into SQL required to upgrade or downgrade a database.
    /// </summary>
    public class MySqlMigrationSqlGenerator : IMigrationSqlGenerator
    {
        private static readonly Dictionary<string, string> TypeMap = new()
        {
            { typeof(int).FullName!, "INT" },
            { typeof(long).FullName!, "BIGINT" },
            { typeof(short).FullName!, "SMALLINT" },
            { typeof(byte).FullName!, "TINYINT" },
            { typeof(bool).FullName!, "TINYINT(1)" },
            { typeof(string).FullName!, "LONGTEXT" },
            { typeof(DateTime).FullName!, "DATETIME" },
            { typeof(decimal).FullName!, "DECIMAL(18,2)" },
            { typeof(double).FullName!, "DOUBLE" },
            { typeof(float).FullName!, "FLOAT" },
            { typeof(Guid).FullName!, "CHAR(36)" }
        };

        /// <summary>
        /// Generates MySQL-specific SQL statements that apply the schema changes described by the provided diff.
        /// </summary>
        /// <param name="diff">The differences between the current and desired database schema.</param>
        /// <returns>A pair of SQL statement lists for migrating up and rolling back.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            var up = new List<string>();
            var down = new List<string>();

            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                    $"`{c.Name}` {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

                // MIG-1: Emit PRIMARY KEY constraint for PK columns
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => $"`{c.Name}`"))})");

                // MIG-1: Emit UNIQUE constraint for unique non-PK columns
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => $"`{c.Name}`"))})");

                up.Add($"CREATE TABLE `{table.Name}` ({string.Join(", ", colDefs)})");

                // MIG-1: Emit CREATE INDEX for columns with a named index (non-PK, non-unique)
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    up.Add($"CREATE INDEX `{col.IndexName}` ON `{table.Name}` (`{col.Name}`)");

                down.Add($"DROP TABLE `{table.Name}`");
            }

            foreach (var (table, column) in diff.AddedColumns)
            {
                var colDef = $"`{column.Name}` {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE `{table.Name}` ADD COLUMN {colDef}");
                down.Add($"ALTER TABLE `{table.Name}` DROP COLUMN `{column.Name}`");
            }

            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                var newDef = $"`{newCol.Name}` {GetSqlType(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE `{table.Name}` MODIFY COLUMN {newDef}");
                var oldDef = $"`{oldCol.Name}` {GetSqlType(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE `{table.Name}` MODIFY COLUMN {oldDef}");
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE `{table.Name}`");
                // Down: recreate the table with full constraint metadata
                var colDefs = table.Columns.Select(c =>
                    $"`{c.Name}` {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => $"`{c.Name}`"))})");
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => $"`{c.Name}`"))})");
                down.Add($"CREATE TABLE `{table.Name}` ({string.Join(", ", colDefs)})");
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    down.Add($"CREATE INDEX `{col.IndexName}` ON `{table.Name}` (`{col.Name}`)");
            }

            // SD-8: Generate DROP COLUMN for columns removed in the new snapshot
            foreach (var (table, column) in diff.DroppedColumns)
            {
                up.Add($"ALTER TABLE `{table.Name}` DROP COLUMN `{column.Name}`");
                var colDef = $"`{column.Name}` {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE `{table.Name}` ADD COLUMN {colDef}");
            }

            return new MigrationSqlStatements(up, down);
        }

        /// <summary>
        /// Resolves the MySQL column definition for the supplied column schema. Unmapped
        /// CLR types default to <c>LONGTEXT</c> to ensure the migration can be executed
        /// even when a specific mapping is unknown.
        /// </summary>
        /// <param name="column">The column metadata describing the CLR type.</param>
        /// <returns>The corresponding MySQL data type.</returns>
        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "LONGTEXT";
    }
}
