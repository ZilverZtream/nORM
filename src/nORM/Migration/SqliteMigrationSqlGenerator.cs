using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
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

            foreach (var (table, column) in diff.AddedColumns)
            {
                var colDef = $"\"{column.Name}\" {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE \"{table.Name}\" ADD COLUMN {colDef}");
                // SQLite does not support dropping columns prior to version 3.35
                down.Add($"-- SQLite does not support dropping column '{column.Name}'");
            }

            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                up.Add($"-- SQLite does not support altering column '{newCol.Name}' in table '{table.Name}'");
                down.Add($"-- SQLite does not support altering column '{oldCol.Name}' in table '{table.Name}'");
            }

            return new MigrationSqlStatements(up, down);
        }

        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "TEXT";
    }
}
