using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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
            { typeof(Guid).FullName!, "UNIQUEIDENTIFIER" },
            // X2: expanded type map
            { typeof(byte[]).FullName!, "VARBINARY(MAX)" },
            { typeof(DateOnly).FullName!, "DATE" },
            { typeof(TimeOnly).FullName!, "TIME" },
            { typeof(DateTimeOffset).FullName!, "DATETIMEOFFSET" },
            { typeof(TimeSpan).FullName!, "TIME" },
            { typeof(char).FullName!, "NCHAR(1)" },
            { typeof(sbyte).FullName!, "SMALLINT" },
            { typeof(ushort).FullName!, "INT" },
            { typeof(uint).FullName!, "BIGINT" },
            { typeof(ulong).FullName!, "DECIMAL(20,0)" }
        };

        // Escape SQL Server identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id) => $"[{id.Replace("]", "]]")}]";

        // Escape a value interpolated inside a SQL single-quoted string literal.
        private static string EscLiteral(string s) => s.Replace("'", "''");

        /// <summary>
        /// Generates SQL Server specific statements for applying the provided schema changes.
        /// </summary>
        /// <param name="diff">The computed differences between the current and desired schema.</param>
        /// <returns>The SQL statements to upgrade and downgrade the database schema.</returns>
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
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                var newDef = $"{Esc(newCol.Name)} {GetSqlType(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}";
                up.Add($"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {newDef}");

                // M1: Emit DEFAULT constraint changes when DefaultValue differs.
                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                {
                    up.Add($"DECLARE @__df_{table.Name}_{newCol.Name} NVARCHAR(200) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{EscLiteral(table.Name)}') AND COL_NAME(parent_column_id,column_id)='{EscLiteral(newCol.Name)}') IF @__df_{table.Name}_{newCol.Name} IS NOT NULL EXEC('ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT ['+@__df_{table.Name}_{newCol.Name}+']')");
                    if (newCol.DefaultValue != null)
                        up.Add($"ALTER TABLE {Esc(table.Name)} ADD CONSTRAINT {Esc($"DF_{table.Name}_{newCol.Name}")} DEFAULT ({DefaultValueValidator.Validate(newCol.DefaultValue)}) FOR {Esc(newCol.Name)}");
                }
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
                up.Add($"ALTER TABLE {Esc(table.Name)} ADD {colDef}");
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
                var oldDef = $"{Esc(oldCol.Name)} {GetSqlType(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE {Esc(table.Name)} ALTER COLUMN {oldDef}");

                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                {
                    down.Add($"DECLARE @__df_{table.Name}_{oldCol.Name} NVARCHAR(200) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{EscLiteral(table.Name)}') AND COL_NAME(parent_column_id,column_id)='{EscLiteral(oldCol.Name)}') IF @__df_{table.Name}_{oldCol.Name} IS NOT NULL EXEC('ALTER TABLE {Esc(table.Name)} DROP CONSTRAINT ['+@__df_{table.Name}_{oldCol.Name}+']')");
                    if (oldCol.DefaultValue != null)
                        down.Add($"ALTER TABLE {Esc(table.Name)} ADD CONSTRAINT {Esc($"DF_{table.Name}_{oldCol.Name}")} DEFAULT ({DefaultValueValidator.Validate(oldCol.DefaultValue)}) FOR {Esc(oldCol.Name)}");
                }
            }

            // DOWN-5: Restore columns that were dropped in UP-3.
            foreach (var (table, column) in diff.DroppedColumns)
            {
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD {colDef}");
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
        /// Determines the SQL Server column type corresponding to the supplied column schema.
        /// Types not present in the internal mapping fall back to <c>NVARCHAR(MAX)</c>.
        /// </summary>
        /// <param name="column">The column description including the CLR type.</param>
        /// <returns>The SQL Server data type name.</returns>
        private static string GetSqlType(ColumnSchema column)
        {
            // X2: handle enum types by mapping to their underlying integral type
            if (!TypeMap.TryGetValue(column.ClrType, out var sql))
            {
                var clrType = ResolveType(column.ClrType);
                if (clrType != null && clrType.IsEnum)
                {
                    var underlying = Enum.GetUnderlyingType(clrType);
                    if (TypeMap.TryGetValue(underlying.FullName!, out sql))
                        return sql;
                }
                return "NVARCHAR(MAX)";
            }
            return sql;
        }

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

        // X2: resolve type by name, scanning loaded assemblies when Type.GetType fails
        private static Type? ResolveType(string typeName)
        {
            var t = Type.GetType(typeName);
            if (t != null) return t;
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                t = asm.GetType(typeName);
                if (t != null) return t;
            }
            return null;
        }
    }
}
