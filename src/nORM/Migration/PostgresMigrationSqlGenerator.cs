using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    /// <summary>
    /// Generates PostgreSQL-specific SQL statements to apply and roll back schema changes.
    /// </summary>
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

        // Escape PostgreSQL identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id) => $"\"{id.Replace("\"", "\"\"")}\"";

        /// <summary>
        /// Produces PostgreSQL-compatible SQL statements to transition between schema versions.
        /// </summary>
        /// <param name="diff">The description of schema changes to apply.</param>
        /// <returns>A set of statements to apply the changes and to revert them.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            var up = new List<string>();
            var down = new List<string>();

            // ─ UP: correct DDL dependency ordering ──────────────────────────────────
            // FK constraints must be dropped BEFORE the columns/tables they reference
            // are removed. Symmetric rule for DOWN: FK constraints added in UP must be
            // dropped BEFORE the columns/tables added in UP are removed.

            // UP-1: Drop FK constraints first (before columns/tables they depend on).
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                up.Add($"ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");

            // UP-2: Drop tables.
            foreach (var table in diff.DroppedTables)
                up.Add($"DROP TABLE {Esc(table.Name)}");

            // UP-3: Drop columns (safe — FKs on those columns are removed in UP-1).
            foreach (var (table, column) in diff.DroppedColumns)
                up.Add($"ALTER TABLE {Esc(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // UP-4: Alter existing columns.
            // PostgreSQL requires separate ALTER COLUMN statements for type and nullability changes.
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (!string.Equals(oldCol.ClrType, newCol.ClrType, StringComparison.Ordinal))
                    up.Add($"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(newCol.Name)} TYPE {GetSqlType(newCol)}");
                if (oldCol.IsNullable != newCol.IsNullable)
                    up.Add(newCol.IsNullable
                        ? $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(newCol.Name)} DROP NOT NULL"
                        : $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(newCol.Name)} SET NOT NULL");

                // M1: Emit DEFAULT changes as separate SET DEFAULT / DROP DEFAULT statements.
                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                    up.Add(newCol.DefaultValue != null
                        ? $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(newCol.Name)} SET DEFAULT {DefaultValueValidator.Validate(newCol.DefaultValue)}"
                        : $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(newCol.Name)} DROP DEFAULT");
            }

            // UP-5: Create new tables (including inline FK constraints).
            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                    $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();

                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");

                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");

                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));

                up.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");

                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    up.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");
            }

            // UP-6: Add columns to existing tables.
            foreach (var (table, column) in diff.AddedColumns)
            {
                if (!column.IsNullable && column.DefaultValue == null)
                    throw new InvalidOperationException(
                        $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                        "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");

                var nullPart = column.IsNullable ? "NULL" : $"NOT NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {nullPart}";
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
            }

            // UP-7: Add FK constraints last (all tables and columns are in place).
            foreach (var (table, fk) in diff.AddedForeignKeys)
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");

            // ─ DOWN: reverse of UP, with symmetric FK ordering ──────────────────────

            // DOWN-1: Drop FK constraints that were added in UP-7 (before touching their columns).
            foreach (var (table, fk) in diff.AddedForeignKeys)
                down.Add($"ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");

            // DOWN-2: Drop columns that were added in UP-6.
            foreach (var (table, column) in diff.AddedColumns)
                down.Add($"ALTER TABLE {Esc(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // DOWN-3: Drop tables that were created in UP-5.
            foreach (var table in diff.AddedTables)
                down.Add($"DROP TABLE {Esc(table.Name)}");

            // DOWN-4: Reverse column alterations from UP-4.
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (!string.Equals(oldCol.ClrType, newCol.ClrType, StringComparison.Ordinal))
                    down.Add($"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} TYPE {GetSqlType(oldCol)}");
                if (oldCol.IsNullable != newCol.IsNullable)
                    down.Add(oldCol.IsNullable
                        ? $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} DROP NOT NULL"
                        : $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} SET NOT NULL");

                // M1: Down — restore old DEFAULT.
                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                    down.Add(oldCol.DefaultValue != null
                        ? $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} SET DEFAULT {DefaultValueValidator.Validate(oldCol.DefaultValue)}"
                        : $"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} DROP DEFAULT");
            }

            // DOWN-5: Restore columns that were dropped in UP-3.
            foreach (var (table, column) in diff.DroppedColumns)
            {
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
            }

            // DOWN-6: Restore tables that were dropped in UP-2.
            foreach (var table in diff.DroppedTables)
            {
                var colDefs = table.Columns.Select(c =>
                    $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}").ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
                var uniqueNonPkCols = table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey).ToList();
                if (uniqueNonPkCols.Count > 0)
                    colDefs.Add($"UNIQUE ({string.Join(", ", uniqueNonPkCols.Select(c => Esc(c.Name)))})");
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                down.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");
                foreach (var col in table.Columns.Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique))
                    down.Add($"CREATE INDEX {Esc(col.IndexName!)} ON {Esc(table.Name)} ({Esc(col.Name)})");
            }

            // DOWN-7: Restore FK constraints that were dropped in UP-1.
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");

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
