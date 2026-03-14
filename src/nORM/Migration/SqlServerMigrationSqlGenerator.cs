using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    /// <summary>
    /// Generates SQL Server compatible migration scripts based on a schema diff.
    /// </summary>
    public class SqlServerMigrationSqlGenerator : IMigrationSqlGenerator
    {
        private static readonly Dictionary<string, string> TypeMap = new()
        {
            { typeof(int).FullName!, "INT" },
            { typeof(long).FullName!, "BIGINT" },
            { typeof(short).FullName!, "SMALLINT" },
            { typeof(byte).FullName!, "TINYINT" },
            { typeof(bool).FullName!, "BIT" },
            { typeof(string).FullName!, "NVARCHAR(MAX)" },
            { typeof(DateTime).FullName!, "DATETIME2" },
            { typeof(decimal).FullName!, "DECIMAL(18,2)" },
            { typeof(double).FullName!, "FLOAT" },
            { typeof(float).FullName!, "REAL" },
            { typeof(Guid).FullName!, "UNIQUEIDENTIFIER" }
        };

        // PRV-1: Escape SQL Server identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id) => $"[{id.Replace("]", "]]")}]";

        /// <summary>
        /// Generates SQL Server specific statements for applying the provided schema changes.
        /// </summary>
        /// <param name="diff">The computed differences between the current and desired schema.</param>
        /// <returns>The SQL statements to upgrade and downgrade the database schema.</returns>
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
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD {colDef}");
                down.Add($"ALTER TABLE {Esc(table.Name)} DROP COLUMN {Esc(column.Name)}");
            }

            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                var newDef = $"{Esc(newCol.Name)} {GetSqlType(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {newDef}");
                var oldDef = $"{Esc(oldCol.Name)} {GetSqlType(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {oldDef}");

                // M1: Emit DEFAULT constraint changes when DefaultValue differs.
                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                {
                    // Drop the old default constraint (if any) by finding it via system metadata.
                    up.Add($"DECLARE @__df_{table.Name}_{newCol.Name} NVARCHAR(200) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{table.Name}') AND COL_NAME(parent_column_id,column_id)='{newCol.Name}') IF @__df_{table.Name}_{newCol.Name} IS NOT NULL EXEC('ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT ['+@__df_{table.Name}_{newCol.Name}+']')");
                    if (newCol.DefaultValue != null)
                        up.Add($"ALTER TABLE {Esc(table.Name)} ADD CONSTRAINT {Esc($"DF_{table.Name}_{newCol.Name}")} DEFAULT ({newCol.DefaultValue}) FOR {Esc(newCol.Name)}");

                    // Down: restore the old default.
                    down.Add($"DECLARE @__df_{table.Name}_{oldCol.Name} NVARCHAR(200) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{table.Name}') AND COL_NAME(parent_column_id,column_id)='{oldCol.Name}') IF @__df_{table.Name}_{oldCol.Name} IS NOT NULL EXEC('ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT ['+@__df_{table.Name}_{oldCol.Name}+']')");
                    if (oldCol.DefaultValue != null)
                        down.Add($"ALTER TABLE {Esc(table.Name)} ADD CONSTRAINT {Esc($"DF_{table.Name}_{oldCol.Name}")} DEFAULT ({oldCol.DefaultValue}) FOR {Esc(oldCol.Name)}");
                }
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
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD {colDef}");
            }

            // MG-1: Add FK constraints to existing tables
            foreach (var (table, fk) in diff.AddedForeignKeys)
            {
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");
                down.Add($"ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");
            }

            // MG-1: Drop FK constraints from existing tables
            foreach (var (table, fk) in diff.DroppedForeignKeys)
            {
                up.Add($"ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");
            }

            return new MigrationSqlStatements(up, down);
        }

        /// <summary>
        /// Determines the SQL Server column type corresponding to the supplied column schema.
        /// Types not present in the internal mapping fall back to <c>NVARCHAR(MAX)</c>.
        /// </summary>
        /// <param name="column">The column description including the CLR type.</param>
        /// <returns>The SQL Server data type name.</returns>
        private static string GetSqlType(ColumnSchema column)
            => TypeMap.TryGetValue(column.ClrType, out var sql) ? sql : "NVARCHAR(MAX)";

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
