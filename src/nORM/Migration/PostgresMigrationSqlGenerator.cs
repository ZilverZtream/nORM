using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    /// <summary>
    /// Generates PostgreSQL-specific SQL statements to apply and roll back schema changes.
    /// </summary>
    /// <remarks>
    /// Provider: PostgreSQL (double-quote-escaped identifiers). Uses GENERATED ALWAYS AS IDENTITY
    /// for auto-increment columns, separate ALTER COLUMN statements for type and nullability changes,
    /// and SET DEFAULT / DROP DEFAULT for default value alterations. FK referential actions are
    /// validated against an allowlist before being interpolated into DDL.
    /// </remarks>
    public class PostgresMigrationSqlGenerator : IMigrationSqlGenerator
    {
        /// <summary>Default PostgreSQL fallback type for unmapped CLR types.</summary>
        private const string FallbackSqlType = "TEXT";

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
            { typeof(Guid).FullName!, "UUID" },
            // X2: expanded type map
            { typeof(byte[]).FullName!, "BYTEA" },
            { typeof(DateOnly).FullName!, "DATE" },
            { typeof(TimeOnly).FullName!, "TIME" },
            { typeof(DateTimeOffset).FullName!, "TIMESTAMPTZ" },
            { typeof(TimeSpan).FullName!, "INTERVAL" },
            { typeof(char).FullName!, "CHAR(1)" },
            { typeof(sbyte).FullName!, "SMALLINT" },
            { typeof(ushort).FullName!, "INTEGER" },
            { typeof(uint).FullName!, "BIGINT" },
            { typeof(ulong).FullName!, "NUMERIC(20,0)" }
        };

        // Escape PostgreSQL identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return $"\"{id.Replace("\"", "\"\"")}\"";
        }

        /// <summary>
        /// Produces PostgreSQL-compatible SQL statements to transition between schema versions.
        /// </summary>
        /// <param name="diff">The description of schema changes to apply.</param>
        /// <returns>A set of statements to apply the changes and to revert them.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);

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
                    // TODO: Add USING clause for explicit type cast when converting between incompatible types
                    // (e.g. TEXT::integer). Without USING, PostgreSQL will reject the ALTER if an implicit
                    // cast from the old type to the new type does not exist. Known limitation: callers must
                    // handle type-coercion migrations manually for incompatible type changes.
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
                {
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    var sqlType = c.IsIdentity ? GetIdentitySqlType(c) : GetSqlType(c);
                    var identityPart = c.IsIdentity ? " GENERATED ALWAYS AS IDENTITY" : "";
                    return $"{Esc(c.Name)} {sqlType} {(c.IsNullable ? "NULL" : "NOT NULL")}{identityPart}{defaultPart}";
                }).ToList();

                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");

                // A: emit a separate UNIQUE constraint for each individual unique non-PK column.
                foreach (var uc in table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey))
                    colDefs.Add($"UNIQUE ({Esc(uc.Name)})");

                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));

                up.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");

                // B: group columns by IndexName and emit ONE CREATE INDEX per unique name.
                foreach (var idxGroup in table.Columns
                    .Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique)
                    .GroupBy(c => c.IndexName!, StringComparer.OrdinalIgnoreCase))
                    up.Add($"CREATE INDEX {Esc(idxGroup.Key)} ON {Esc(table.Name)} ({string.Join(", ", idxGroup.Select(c => Esc(c.Name)))})");
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
                down.Add($"DROP TABLE IF EXISTS {Esc(table.Name)}");

            // DOWN-4: Reverse column alterations from UP-4.
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (!string.Equals(oldCol.ClrType, newCol.ClrType, StringComparison.Ordinal))
                    // TODO: Add USING clause for explicit type cast (see UP-4 comment above for details).
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
            // C: include DefaultValue in the column definition so NOT NULL restore doesn't fail.
            foreach (var (table, column) in diff.DroppedColumns)
            {
                var restoreDefault = !string.IsNullOrEmpty(column.DefaultValue)
                    ? $" DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                    : "";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}{restoreDefault}";
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
            }

            // DOWN-6: Restore tables that were dropped in UP-2.
            foreach (var table in diff.DroppedTables)
            {
                var colDefs = table.Columns.Select(c =>
                {
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    var sqlType = c.IsIdentity ? GetIdentitySqlType(c) : GetSqlType(c);
                    var identityPart = c.IsIdentity ? " GENERATED ALWAYS AS IDENTITY" : "";
                    return $"{Esc(c.Name)} {sqlType} {(c.IsNullable ? "NULL" : "NOT NULL")}{identityPart}{defaultPart}";
                }).ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
                // A: emit a separate UNIQUE constraint for each individual unique non-PK column.
                foreach (var uc in table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey))
                    colDefs.Add($"UNIQUE ({Esc(uc.Name)})");
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                down.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");
                // B: group columns by IndexName and emit ONE CREATE INDEX per unique name.
                foreach (var idxGroup in table.Columns
                    .Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique)
                    .GroupBy(c => c.IndexName!, StringComparer.OrdinalIgnoreCase))
                    down.Add($"CREATE INDEX {Esc(idxGroup.Key)} ON {Esc(table.Name)} ({string.Join(", ", idxGroup.Select(c => Esc(c.Name)))})");
            }

            // DOWN-7: Restore FK constraints that were dropped in UP-1.
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                down.Add($"ALTER TABLE {Esc(table.Name)} ADD {BuildFkConstraintSql(fk)}");

            return new MigrationSqlStatements(up, down);
        }

        // Integer CLR type full names that map validly to PostgreSQL GENERATED ALWAYS AS IDENTITY.
        private static readonly HashSet<string> _integerClrTypes = new(StringComparer.Ordinal)
        {
            typeof(int).FullName!,
            typeof(long).FullName!,
            typeof(short).FullName!,
            typeof(byte).FullName!,
            typeof(sbyte).FullName!,
            typeof(ushort).FullName!,
            typeof(uint).FullName!,
            typeof(ulong).FullName!,
        };

        /// <summary>
        /// Returns the appropriate PostgreSQL integer type for an identity column.
        /// BIGINT for long/ulong, INTEGER for all other integer types.
        /// Non-integer CLR types return INTEGER with a leading SQL warning comment because
        /// GENERATED ALWAYS AS IDENTITY is only valid for integer types in PostgreSQL.
        /// </summary>
        private static string GetIdentitySqlType(ColumnSchema column)
        {
            if (string.Equals(column.ClrType, typeof(long).FullName, StringComparison.Ordinal)
             || string.Equals(column.ClrType, typeof(ulong).FullName, StringComparison.Ordinal))
                return "BIGINT";

            if (!_integerClrTypes.Contains(column.ClrType))
                // WARNING: GENERATED ALWAYS AS IDENTITY is only valid for integer types in PostgreSQL.
                // The column CLR type is not an integer type; INTEGER is used as a fallback — verify the mapping is correct.
                return "/* WARNING: GENERATED ALWAYS AS IDENTITY is only valid for integer types in PostgreSQL */ INTEGER";

            return "INTEGER";
        }

        /// <summary>
        /// Maps a <see cref="ColumnSchema"/> instance to the appropriate PostgreSQL column
        /// type. When a CLR type is not explicitly mapped, <c>TEXT</c> is used as a safe
        /// default.
        /// </summary>
        /// <param name="column">The column metadata describing the desired CLR type.</param>
        /// <returns>The PostgreSQL data type name.</returns>
        private static string GetSqlType(ColumnSchema column)
        {
            ArgumentNullException.ThrowIfNull(column);

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
                return FallbackSqlType;
            }
            return sql;
        }

        // Allowlist for FK referential action tokens (NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT).
        // NOTE: Identical copy exists in the other three migration generators. If a shared base class is introduced, consolidate here.
        private static readonly HashSet<string> _validFkActions =
            new(StringComparer.OrdinalIgnoreCase) { "NO ACTION", "CASCADE", "SET NULL", "RESTRICT", "SET DEFAULT" };

        private static string ValidateFkAction(string action, string constraintName)
        {
            ArgumentNullException.ThrowIfNull(action);
            if (!_validFkActions.Contains(action))
                throw new ArgumentException(
                    $"Invalid FK referential action '{action}' in constraint '{constraintName}'. " +
                    "Allowed values: NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT.");
            return action;
        }

        /// <summary>
        /// Builds the inline FOREIGN KEY constraint SQL fragment for a CREATE TABLE or ALTER TABLE statement.
        /// </summary>
        private static string BuildFkConstraintSql(ForeignKeySchema fk)
        {
            ArgumentNullException.ThrowIfNull(fk);
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

        // NOTE: identical copies of ResolveType and ValidateFkAction exist in the other three generators;
        // consolidate into a shared base class or static helper if one is introduced in the future.

        // Cache for ResolveType to avoid scanning all loaded assemblies on every call.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type?> _resolveTypeCache
            = new(StringComparer.Ordinal);

        // Resolve a CLR type by its full name, scanning loaded assemblies when Type.GetType fails.
        // Results are cached in _resolveTypeCache to avoid repeated AppDomain scans.
        private static Type? ResolveType(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;
            return _resolveTypeCache.GetOrAdd(typeName, static name =>
            {
                var t = Type.GetType(name);
                if (t != null) return t;
                foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
                {
                    t = asm.GetType(name);
                    if (t != null) return t;
                }
                return null;
            });
        }
    }
}
