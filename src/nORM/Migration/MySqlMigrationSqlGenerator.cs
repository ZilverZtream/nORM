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

        // PRV-1: Escape MySQL identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id) => $"`{id.Replace("`", "``")}`";

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
                    $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

                // MIG-1: Emit PRIMARY KEY constraint for PK columns
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");

                // MIG-1: Emit UNIQUE constraint for unique non-PK columns
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");

                // MG-1: Emit inline FOREIGN KEY constraints
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));

                up.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");

                // MIG-1: Emit CREATE INDEX for columns with a named index (non-PK, non-unique)
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    up.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");

                down.Add($"DROP TABLE {Esc(table.Name)}");
            }

            foreach (var (table, column) in diff.AddedColumns)
            {
                // MIG-1: NOT NULL column without a DefaultValue cannot be added to a populated table.
                if (!column.IsNullable && column.DefaultValue == null)
                    throw new InvalidOperationException(
                        $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                        "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");

                var nullPart = column.IsNullable ? "NULL" : $"NOT NULL DEFAULT {column.DefaultValue}";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {nullPart}";
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
                down.Add($"ALTER TABLE {Esc(table.Name)} DROP COLUMN {Esc(column.Name)}");
            }

            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                // M1: Include DEFAULT in MODIFY COLUMN — MySQL replaces the full column definition.
                var newDefault = newCol.DefaultValue != null ? $" DEFAULT {newCol.DefaultValue}" : "";
                var newDef = $"{Esc(newCol.Name)} {GetSqlType(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}{newDefault}";
                up.Add($"ALTER TABLE {Esc(table.Name)} MODIFY COLUMN {newDef}");
                var oldDefault = oldCol.DefaultValue != null ? $" DEFAULT {oldCol.DefaultValue}" : "";
                var oldDef = $"{Esc(oldCol.Name)} {GetSqlType(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}{oldDefault}";
                down.Add($"ALTER TABLE {Esc(table.Name)} MODIFY COLUMN {oldDef}");
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE {Esc(table.Name)}");
                // Down: recreate the table with full constraint metadata
                var colDefs = table.Columns.Select(c =>
                    $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");
                // MG-1: Restore FK constraints in Down recreation
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                down.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    down.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");
            }

            // SD-8: Generate DROP COLUMN for columns removed in the new snapshot
            foreach (var (table, column) in diff.DroppedColumns)
            {
                up.Add($"ALTER TABLE {Esc(table.Name)} DROP COLUMN {Esc(column.Name)}");
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
            }

            // MG-1: Add FK constraints to existing tables
            foreach (var (table, fk) in diff.AddedForeignKeys)
            {
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");
                // MySQL uses DROP FOREIGN KEY (not DROP CONSTRAINT) for FK removal
                down.Add($"ALTER TABLE {Esc(table.Name)} DROP FOREIGN KEY {Esc(fk.ConstraintName)}");
            }

            // MG-1: Drop FK constraints from existing tables
            foreach (var (table, fk) in diff.DroppedForeignKeys)
            {
                up.Add($"ALTER TABLE {Esc(table.Name)} DROP FOREIGN KEY {Esc(fk.ConstraintName)}");
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");
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

        // M1/X1: Allowlist for FK referential action tokens.
        private static readonly HashSet<string> _validFkActions =
            new(StringComparer.OrdinalIgnoreCase) { "NO ACTION", "CASCADE", "SET NULL", "RESTRICT", "SET DEFAULT" };

        private static string ValidateFkAction(string action, string constraintName)
        {
            if (!_validFkActions.Contains(action))
                throw new ArgumentException(
                    $"Invalid FK referential action '{action}' in constraint '{constraintName}'. " +
                    "Allowed values: NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT.");
            return action;
        }

        /// <summary>
        /// MG-1: Builds the inline FOREIGN KEY constraint SQL fragment for a CREATE TABLE or ALTER TABLE statement.
        /// </summary>
        private static string BuildFkConstraintSql(ForeignKeySchema fk)
        {
            var depCols = string.Join(", ", fk.DependentColumns.Select(Esc));
            var refCols = string.Join(", ", fk.PrincipalColumns.Select(Esc));
            var onDelete = ValidateFkAction(fk.OnDelete, fk.ConstraintName);
            var onUpdate = ValidateFkAction(fk.OnUpdate, fk.ConstraintName);
            var sql = $"CONSTRAINT {Esc(fk.ConstraintName)} FOREIGN KEY ({depCols}) REFERENCES {Esc(fk.PrincipalTable)}({refCols})";
            if (!string.Equals(onDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON DELETE {onDelete}";
            if (!string.Equals(onUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON UPDATE {onUpdate}";
            return sql;
        }
    }
}
