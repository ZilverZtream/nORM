using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
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
                var cols = table.Columns.Select(c =>
                    $"`{c.Name}` {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
                up.Add($"CREATE TABLE `{table.Name}` ({string.Join(", ", cols)})");
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

            return new MigrationSqlStatements(up, down);
        }

        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "LONGTEXT";
    }
}
