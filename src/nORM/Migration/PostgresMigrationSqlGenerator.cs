using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    public class PostgresMigrationSqlGenerator : IMigrationSqlGenerator
    {
        private static readonly Dictionary<string, string> TypeMap = new()
        {
            { typeof(int).FullName!, "INTEGER" },
            { typeof(long).FullName!, "BIGINT" },
            { typeof(short).FullName!, "SMALLINT" },
            { typeof(byte).FullName!, "SMALLINT" },
            { typeof(bool).FullName!, "BOOLEAN" },
            { typeof(string).FullName!, "TEXT" },
            { typeof(DateTime).FullName!, "TIMESTAMP" },
            { typeof(decimal).FullName!, "DECIMAL(18,2)" },
            { typeof(double).FullName!, "DOUBLE PRECISION" },
            { typeof(float).FullName!, "REAL" },
            { typeof(Guid).FullName!, "UUID" }
        };

        /// <summary>
        /// Produces PostgreSQL-compatible SQL statements to transition between schema versions.
        /// </summary>
        /// <param name="diff">The description of schema changes to apply.</param>
        /// <returns>A set of statements to apply the changes and to revert them.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            var up = new List<string>();
            var down = new List<string>();

            foreach (var table in diff.AddedTables)
            {
                var cols = table.Columns.Select(c =>
                    $"\"{c.Name}\" {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}");
                up.Add($"CREATE TABLE \"{table.Name}\" ({string.Join(", ", cols)})");
                down.Add($"DROP TABLE \"{table.Name}\"");
            }

            foreach (var (table, column) in diff.AddedColumns)
            {
                var colDef = $"\"{column.Name}\" {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE \"{table.Name}\" ADD COLUMN {colDef}");
                down.Add($"ALTER TABLE \"{table.Name}\" DROP COLUMN \"{column.Name}\"");
            }

            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                var newDef = $"\"{newCol.Name}\" {GetSqlType(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE \"{table.Name}\" ALTER COLUMN {newDef}");
                var oldDef = $"\"{oldCol.Name}\" {GetSqlType(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE \"{table.Name}\" ALTER COLUMN {oldDef}");
            }

            return new MigrationSqlStatements(up, down);
        }

        /// <summary>
        /// Maps a <see cref="ColumnSchema"/> instance to the appropriate PostgreSQL column
        /// type. When a CLR type is not explicitly mapped, <c>TEXT</c> is used as a safe
        /// default.
        /// </summary>
        /// <param name="column">The column metadata describing the desired CLR type.</param>
        /// <returns>The PostgreSQL data type name.</returns>
        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "TEXT";
    }
}
