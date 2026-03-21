using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace nORM.Migration
{
    /// <summary>
    /// Generates SQLite-specific SQL statements to apply and roll back schema changes.
    /// </summary>
    /// <remarks>
    /// Provider: SQLite (double-quote-escaped identifiers). Uses INTEGER PRIMARY KEY AUTOINCREMENT
    /// for identity columns. Because SQLite does not support ALTER COLUMN or DROP COLUMN on older
    /// versions, schema changes use the standard table-recreation workaround (CREATE temp, INSERT,
    /// DROP, RENAME). PRAGMA foreign_keys=off/on are returned in pre/post-transaction segments so
    /// callers can execute them outside the migration transaction. FK referential actions are
    /// validated against an allowlist before being interpolated into DDL.
    /// </remarks>
    public class SqliteMigrationSqlGenerator : IMigrationSqlGenerator
    {
        /// <summary>Default SQLite fallback type for unmapped CLR types.</summary>
        private const string FallbackSqlType = "TEXT";

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
            { typeof(Guid).FullName!, "TEXT" },
            // X2: expanded type map
            { typeof(byte[]).FullName!, "BLOB" },
            { typeof(DateOnly).FullName!, "TEXT" },
            { typeof(TimeOnly).FullName!, "TEXT" },
            { typeof(DateTimeOffset).FullName!, "TEXT" },
            { typeof(TimeSpan).FullName!, "TEXT" },
            { typeof(char).FullName!, "TEXT" },
            { typeof(sbyte).FullName!, "INTEGER" },
            { typeof(ushort).FullName!, "INTEGER" },
            { typeof(uint).FullName!, "INTEGER" },
            { typeof(ulong).FullName!, "INTEGER" }
        };

        // Escape SQLite identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return $"\"{id.Replace("\"", "\"\"")}\"";
        }

        /// <summary>
        /// Creates SQLite SQL statements for the operations described by the schema diff.
        /// PRAGMA foreign_keys=off/on are returned in PreTransactionUp/Down and PostTransactionUp/Down
        /// segments so callers can execute them outside the migration transaction. The Up/Down lists
        /// contain only transactional DDL statements (no PRAGMA).
        /// </summary>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);

            var up = new List<string>();
            var down = new List<string>();
            bool needsUpFkPragma = false;
            bool needsDownFkPragma = false;

            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                {
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    // SQLite AUTOINCREMENT requires inline "INTEGER PRIMARY KEY AUTOINCREMENT" on the column definition
                    if (c.IsIdentity && c.IsPrimaryKey)
                        return $"{Esc(c.Name)} {GetSqlType(c)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";
                    return $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
                }).ToList();

                // Emit PRIMARY KEY constraint for PK columns.
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                {
                    // SQLite AUTOINCREMENT requires "INTEGER PRIMARY KEY AUTOINCREMENT" inline, not table-level constraint
                    if (!pkCols.Any(c => c.IsIdentity))
                        colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
                }

                // A: emit a separate UNIQUE constraint for each individual unique non-PK column.
                foreach (var uc in table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey))
                    colDefs.Add($"UNIQUE ({Esc(uc.Name)})");

                // MG-1: Emit inline FOREIGN KEY constraints
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));

                up.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");

                // B: group columns by IndexName and emit ONE CREATE INDEX per unique name.
                foreach (var idxGroup in table.Columns
                    .Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique)
                    .GroupBy(c => c.IndexName!, StringComparer.OrdinalIgnoreCase))
                    up.Add($"CREATE INDEX {Esc(idxGroup.Key)} ON {Esc(table.Name)} ({string.Join(", ", idxGroup.Select(c => Esc(c.Name)))})");

                down.Add($"DROP TABLE IF EXISTS {Esc(table.Name)}");
            }

            foreach (var group in diff.AddedColumns.GroupBy(x => x.Table))
            {
                var table = group.Key;
                var addedColumnNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var (_, column) in group)
                {
                    // NOT NULL column without a DefaultValue cannot be added to a populated table.
                    if (!column.IsNullable && column.DefaultValue == null)
                        throw new InvalidOperationException(
                            $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                            "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");

                    var nullPart = column.IsNullable ? "NULL" : $"NOT NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}";
                    var colDef = $"{Esc(column.Name)} {GetSqlType(column)} {nullPart}";
                    up.Add($"ALTER TABLE {Esc(table.Name)} ADD COLUMN {colDef}");
                }

                // Down: undo the ADD COLUMN by recreating the table without those columns.
                // MG-2: RecreateTable no longer emits PRAGMA inline.
                var remainingColumns = table.Columns
                    .Where(c => !addedColumnNames.Contains(c.Name))
                    .ToList();
                RecreateTable(down, table, remainingColumns, null);
                needsDownFkPragma = true;
            }

            // G2: SQLite does not support ALTER COLUMN; use the standard table-recreation workaround.
            foreach (var group in diff.AlteredColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table         = diff.AlteredColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var alteredMap    = group.ToDictionary(x => x.NewColumn.Name, x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
                var oldAlteredMap = group.ToDictionary(x => x.OldColumn.Name, x => x.OldColumn, StringComparer.OrdinalIgnoreCase);

                AddRecreate(up,   table, alteredMap);
                AddRecreate(down, table, oldAlteredMap);
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE IF EXISTS {Esc(table.Name)}");
                // Down: recreate the table with full constraint metadata
                var colDefs = table.Columns.Select(c =>
                {
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    if (c.IsIdentity && c.IsPrimaryKey)
                        return $"{Esc(c.Name)} {GetSqlType(c)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";
                    return $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
                }).ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                {
                    if (!pkCols.Any(c => c.IsIdentity))
                        colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
                }
                // A: emit a separate UNIQUE constraint for each individual unique non-PK column.
                foreach (var uc in table.Columns.Where(c => c.IsUnique && !c.IsPrimaryKey))
                    colDefs.Add($"UNIQUE ({Esc(uc.Name)})");
                // MG-1: Restore FK constraints in Down recreation
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                down.Add($"CREATE TABLE {Esc(table.Name)} ({string.Join(", ", colDefs)})");
                // B: group columns by IndexName and emit ONE CREATE INDEX per unique name.
                foreach (var idxGroup in table.Columns
                    .Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique)
                    .GroupBy(c => c.IndexName!, StringComparer.OrdinalIgnoreCase))
                    down.Add($"CREATE INDEX {Esc(idxGroup.Key)} ON {Esc(table.Name)} ({string.Join(", ", idxGroup.Select(c => Esc(c.Name)))})");
            }

            // SD-8: Generate DROP COLUMN for columns removed in the new snapshot.
            // SQLite does not support ALTER TABLE ... DROP COLUMN before version 3.35.0,
            // so we use the table-recreation workaround for broad compatibility.
            foreach (var group in diff.DroppedColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var newTable = diff.DroppedColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var droppedColNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var droppedCols = group.Select(g => g.Column).ToList();

                // The columns remaining in the new table (without the dropped ones)
                var remainingCols = newTable.Columns
                    .Where(c => !droppedColNames.Contains(c.Name))
                    .ToList();

                // Up: recreate table without dropped columns — use RecreateTable for full constraint preservation
                // MG-2: RecreateTable no longer emits PRAGMA inline.
                RecreateTable(up, newTable, remainingCols, null);
                needsUpFkPragma = true;

                // Down: add the dropped columns back (SQLite ADD COLUMN is forward-compatible)
                // C: include DefaultValue so NOT NULL columns can be restored to populated tables.
                foreach (var droppedCol in droppedCols)
                {
                    var restoreDefault = !string.IsNullOrEmpty(droppedCol.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(droppedCol.DefaultValue)}"
                        : "";
                    var colDef = $"{Esc(droppedCol.Name)} {GetSqlType(droppedCol)} {(droppedCol.IsNullable ? "NULL" : "NOT NULL")}{restoreDefault}";
                    down.Add($"ALTER TABLE {Esc(newTable.Name)} ADD COLUMN {colDef}");
                }
            }

            // MG-1: FK constraint changes on existing SQLite tables require table recreation.
            // Group by table name so each table is recreated only once even if multiple FKs change.
            foreach (var tableGroup in diff.AddedForeignKeys
                .GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                // M1/X1: Validate FK actions for each newly added FK before any DDL is emitted.
                foreach (var (_, addedFk) in tableGroup)
                {
                    ValidateFkAction(addedFk.OnDelete, addedFk.ConstraintName);
                    ValidateFkAction(addedFk.OnUpdate, addedFk.ConstraintName);
                }

                var table = diff.AddedForeignKeys
                    .First(x => string.Equals(x.Table.Name, tableGroup.Key, StringComparison.OrdinalIgnoreCase)).Table;
                // Up: recreate with the new FK set (table.ForeignKeys reflects post-diff state)
                RecreateTable(up, table, table.Columns, null, table.ForeignKeys);
                // Down: recreate without the newly added FKs
                var addedNames = tableGroup.Select(x => x.ForeignKey.ConstraintName)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                var oldFks = table.ForeignKeys
                    .Where(fk => !addedNames.Contains(fk.ConstraintName)).ToList();
                RecreateTable(down, table, table.Columns, null, oldFks);
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            foreach (var tableGroup in diff.DroppedForeignKeys
                .GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = diff.DroppedForeignKeys
                    .First(x => string.Equals(x.Table.Name, tableGroup.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var droppedFks = tableGroup.Select(x => x.ForeignKey).ToList();
                // Up: recreate without the dropped FKs (table.ForeignKeys already excludes them)
                RecreateTable(up, table, table.Columns, null, table.ForeignKeys);
                // Down: recreate with the dropped FKs restored
                var restoredFks = table.ForeignKeys.Concat(droppedFks).ToList();
                RecreateTable(down, table, table.Columns, null, restoredFks);
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            // MG-2: Return PRAGMA statements in pre/post transaction segments so callers can
            // execute them outside the migration transaction (required by SQLite documentation).
            var preUp    = needsUpFkPragma   ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=off" } : null;
            var postUp   = needsUpFkPragma   ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=on" }  : null;
            var preDown  = needsDownFkPragma ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=off" } : null;
            var postDown = needsDownFkPragma ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=on" }  : null;

            return new MigrationSqlStatements(up, down, preUp, postUp, preDown, postDown);
        }

        /// <summary>
        /// Generates the SQLite table-recreation DDL sequence for changed columns.
        /// Emits full schema including PRIMARY KEY, UNIQUE, and CREATE INDEX constraints.
        /// </summary>
        private static void AddRecreate(List<string> stmts, TableSchema table, Dictionary<string, ColumnSchema> overrides)
        {
            var cols = table.Columns.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();
            RecreateTable(stmts, table, cols, null);
        }

        /// <summary>
        /// Emits the SQLite table-recreation DDL sequence (CREATE temp, INSERT, DROP, RENAME)
        /// WITHOUT PRAGMA statements. The caller is responsible for setting needsUpFkPragma/needsDownFkPragma
        /// which causes PRAGMA foreign_keys=off/on to be emitted in the pre/post-transaction segments.
        /// MG-2: PRAGMA statements removed from this method to allow callers to place them
        /// outside the migration transaction.
        /// MG-1: Accepts an optional explicit FK list; defaults to table.ForeignKeys when null.
        /// </summary>
        private static void RecreateTable(List<string> stmts, TableSchema table, List<ColumnSchema> cols,
            Dictionary<string, ColumnSchema>? overrides, IReadOnlyList<ForeignKeySchema>? fks = null)
        {
            // Apply overrides if supplied
            if (overrides != null)
                cols = cols.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();

            // F: compute names AFTER overrides have been applied so the SELECT list uses the correct column set.
            var names = cols.Select(c => Esc(c.Name)).ToList();

            // Build column definitions with full constraint metadata (same as AddedTables path).
            var colDefs = cols.Select(c =>
            {
                var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                    ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                    : "";
                if (c.IsIdentity && c.IsPrimaryKey)
                    return $"{Esc(c.Name)} {GetSqlType(c)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";
                return $"{Esc(c.Name)} {GetSqlType(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
            }).ToList();

            // Emit PRIMARY KEY constraint for PK columns.
            var pkCols = cols.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0)
            {
                // SQLite AUTOINCREMENT requires "INTEGER PRIMARY KEY AUTOINCREMENT" inline, not table-level constraint
                if (!pkCols.Any(c => c.IsIdentity))
                    colDefs.Add($"PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})");
            }

            // A: emit a separate UNIQUE constraint for each individual unique non-PK column.
            foreach (var uc in cols.Where(c => c.IsUnique && !c.IsPrimaryKey))
                colDefs.Add($"UNIQUE ({Esc(uc.Name)})");

            // MG-1: Emit inline FOREIGN KEY constraints (explicit list or fall back to table.ForeignKeys)
            foreach (var fk in fks ?? table.ForeignKeys)
                colDefs.Add(BuildFkConstraintSql(fk));

            // MG-2: No PRAGMA here — PRAGMA foreign_keys=off/on is returned in the pre/post transaction segments.
            var tempName = $"\"__temp__{table.Name.Replace("\"", "\"\"")}\"";
            // G: Drop the temp table if it already exists (handles interrupted prior migration).
            stmts.Add($"DROP TABLE IF EXISTS {tempName}");
            stmts.Add($"CREATE TABLE {tempName} ({string.Join(", ", colDefs)})");
            stmts.Add($"INSERT INTO {tempName} SELECT {string.Join(", ", names)} FROM {Esc(table.Name)}");
            stmts.Add($"DROP TABLE {Esc(table.Name)}");
            stmts.Add($"ALTER TABLE {tempName} RENAME TO {Esc(table.Name)}");

            // B: group columns by IndexName and emit ONE CREATE INDEX per unique name.
            foreach (var idxGroup in cols
                .Where(c => c.IndexName != null && !c.IsPrimaryKey && !c.IsUnique)
                .GroupBy(c => c.IndexName!, StringComparer.OrdinalIgnoreCase))
                stmts.Add($"CREATE INDEX {Esc(idxGroup.Key)} ON {Esc(table.Name)} ({string.Join(", ", idxGroup.Select(c => Esc(c.Name)))})");
        }

        // M1/X1: Allowlist for FK referential action tokens. Free-form strings are not safe
        // to interpolate into DDL; an attacker-controlled OnDelete/OnUpdate could inject
        // arbitrary SQL into migration scripts.
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
        /// Builds the inline FOREIGN KEY constraint SQL fragment for a CREATE TABLE statement.
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
