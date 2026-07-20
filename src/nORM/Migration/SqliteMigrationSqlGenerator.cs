using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Configuration;

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
    public partial class SqliteMigrationSqlGenerator : IMigrationSqlGenerator
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
            // TEXT, not NUMERIC: SQLite's NUMERIC affinity converts any well-formed
            // real literal to REAL (a double), silently collapsing decimals beyond
            // 15-16 significant digits on every insert. TEXT stores the canonical
            // decimal string exactly — the same mapping EF Core uses for SQLite.
            { typeof(decimal).FullName!, "TEXT" },
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

        private static string EscTable(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return string.Join(".", id.Split('.').Select(Esc));
        }

        private static (string? Schema, string Table) SplitTableName(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            var dot = id.IndexOf('.');
            return dot <= 0
                ? (null, id)
                : (id[..dot], id[(dot + 1)..]);
        }

        private static string EscTempTable(string id)
        {
            var (schema, table) = SplitTableName(id);
            var tempTable = "__temp__" + table;
            return schema is null ? Esc(tempTable) : $"{Esc(schema)}.{Esc(tempTable)}";
        }

        private static string EscTableNameOnly(string id)
        {
            var (_, table) = SplitTableName(id);
            return Esc(table);
        }

        private static string EscIndexName(string tableName, string indexName)
        {
            var (schema, _) = SplitTableName(tableName);
            return schema is null ? Esc(indexName) : $"{Esc(schema)}.{Esc(indexName)}";
        }

        private static string EscIndexTargetTable(string tableName)
        {
            var (_, table) = SplitTableName(tableName);
            return Esc(table);
        }

        private static (string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql) ResolveIndex(
            TableSchema table,
            string indexName,
            bool isUnique,
            string[] columnNames,
            bool[] descending)
        {
            foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
            {
                if (string.Equals(index.IndexName, indexName, StringComparison.OrdinalIgnoreCase))
                    return index;
            }

            return (indexName, isUnique, columnNames, descending, Array.Empty<IndexNullSortOrder>(), Array.Empty<string>(), false, null);
        }

        private static string BuildIndexSql(TableSchema table, string indexName, bool isUnique, string[] columnNames, bool[] descending)
        {
            var index = ResolveIndex(table, indexName, isUnique, columnNames, descending);
            EnsureNoIncludedColumns(index.IncludedColumnNames, index.IndexName);
            EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
            EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
            var unique = index.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscIndexTargetTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatFilter(index.FilterSql)}";
        }

        private static string BuildImplicitUniqueIndexSql(TableSchema table, ColumnSchema column)
            => BuildIndexSql(table, GetUniqueConstraintName(table, column), true, new[] { column.Name }, new[] { false });

        private static bool IsImplicitUniqueColumn(ColumnSchema column)
            => column.IsUnique
               && !column.IsPrimaryKey
               && string.IsNullOrWhiteSpace(column.IndexName)
               && column.Indexes.Count == 0;

        private static string GetPrimaryKeyConstraintName(TableSchema table, IReadOnlyList<ColumnSchema> pkCols)
            => pkCols.FirstOrDefault(static c => c.IsPrimaryKey && !string.IsNullOrWhiteSpace(c.IndexName))?.IndexName
               ?? $"PK_{table.Name}";

        private static string GetUniqueConstraintName(TableSchema table, ColumnSchema column)
            => $"UQ_{table.Name}_{column.Name}";

        private static string BuildPrimaryKeyConstraintSql(TableSchema table, IReadOnlyList<ColumnSchema> pkCols)
            => $"CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, pkCols))} PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})";

        private static string BuildUniqueConstraintSql(TableSchema table, ColumnSchema column)
            => $"CONSTRAINT {Esc(GetUniqueConstraintName(table, column))} UNIQUE ({Esc(column.Name)})";

        private static string BuildCreateColumnDefinition(ColumnSchema column)
        {
            if (IsComputedColumn(column))
                return BuildComputedColumnDefinition(column);

            var defaultPart = !string.IsNullOrEmpty(column.DefaultValue)
                ? $" DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                : "";

            if (column.IsIdentity && column.IsPrimaryKey)
                return $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";

            return $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
        }

        private static string BuildCreateTableSql(TableSchema table)
        {
            var colDefs = table.Columns.Select(BuildCreateColumnDefinition).ToList();
            var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0 && !pkCols.Any(c => c.IsIdentity))
                colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));

            foreach (var uniqueColumn in table.Columns.Where(IsImplicitUniqueColumn))
                colDefs.Add(BuildUniqueConstraintSql(table, uniqueColumn));
            foreach (var fk in table.ForeignKeys)
                colDefs.Add(BuildFkConstraintSql(fk));
            foreach (var check in table.CheckConstraints)
                colDefs.Add(BuildCheckConstraintSql(check));

            return $"CREATE TABLE {EscTable(table.Name)} ({string.Join(", ", colDefs)})";
        }

        private static void AddCreateTableWithIndexes(List<string> statements, TableSchema table)
        {
            statements.Add(BuildCreateTableSql(table));

            foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
                statements.Add(BuildIndexSql(table, index.IndexName, index.IsUnique, index.ColumnNames, index.Descending));
            foreach (var expressionIndex in table.ExpressionIndexes)
                statements.Add(BuildExpressionIndexSql(table, expressionIndex));
        }

        private static bool RequiresRecreateForAddedColumn(ColumnSchema column)
            => column.IsPrimaryKey;

        /// <summary>
        /// Creates SQLite SQL statements for the operations described by the schema diff.
        /// PRAGMA foreign_keys=off/on are returned in PreTransactionUp/Down and PostTransactionUp/Down
        /// segments so callers can execute them outside the migration transaction. The Up/Down lists
        /// contain only transactional DDL statements (no PRAGMA).
        /// </summary>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);
            ValidateSqliteIdentityMetadata(diff);

            var up = new List<string>();
            var down = new List<string>();
            bool needsUpFkPragma = false;
            bool needsDownFkPragma = false;
            var upRecreatedTables = GetUpRecreatedTableNames(diff);
            var downRecreatedTables = GetDownRecreatedTableNames(diff);

            foreach (var table in diff.AddedTables)
            {
                AddCreateTableWithIndexes(up, table);
                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");
            }

            // FK referential actions must be validated before any DDL is emitted.
            foreach (var (_, addedFk) in diff.AddedForeignKeys)
            {
                ValidateFkAction(addedFk.OnDelete, addedFk.ConstraintName);
                ValidateFkAction(addedFk.OnUpdate, addedFk.ConstraintName);
            }

            // A NOT NULL added column without a DefaultValue cannot be materialized on a populated
            // table by either ADD COLUMN or the recreate INSERT ... SELECT. Fail fast with a clear error.
            foreach (var (table, column) in diff.AddedColumns)
            {
                if (!IsComputedColumn(column) && !column.IsNullable && column.DefaultValue == null)
                    throw new InvalidOperationException(
                        $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                        "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");
            }

            // SQLite cannot ALTER COLUMN, DROP COLUMN, or ALTER a constraint in place, so those changes
            // use the table-recreation workaround (CREATE temp, INSERT ... SELECT, DROP, RENAME). A single
            // migration can touch one table through several such changes at once; recreate each affected
            // table EXACTLY ONCE per direction with the complete target schema. Emitting a separate
            // recreate per change makes the recreations clobber one another - most damagingly on Down,
            // where each rebuild reverted only its own dimension and reset the others, silently corrupting
            // the rollback.
            foreach (var tableName in upRecreatedTables)
            {
                EmitUpRecreate(up, diff, tableName);
                needsUpFkPragma = true;
            }
            foreach (var tableName in downRecreatedTables)
            {
                EmitDownRecreate(down, diff, tableName);
                needsDownFkPragma = true;
            }

            // Added columns on tables NOT recreated in Up use a plain ALTER TABLE ADD COLUMN. When the
            // table is recreated for another reason the added columns are folded into that Up recreate
            // (via addedColumnNames); their Down removal is always handled by the consolidated Down recreate.
            foreach (var group in diff.AddedColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                if (upRecreatedTables.Contains(table.Name))
                    continue;
                foreach (var (_, column) in group)
                {
                    if (IsComputedColumn(column))
                    {
                        up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(column)}");
                        continue;
                    }

                    // A NULLABLE added column must carry its declared DEFAULT too - dropping it
                    // silently diverges the migrated schema from a freshly-created one (the
                    // CREATE TABLE path and the Down-restore path both emit the default), and new
                    // inserts would not honour the model's default.
                    var nullPart = column.IsNullable
                        ? (!string.IsNullOrEmpty(column.DefaultValue)
                            ? $"NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                            : "NULL")
                        : $"NOT NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}";
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {nullPart}");
                }
                foreach (var (_, column) in group.Where(g => IsImplicitUniqueColumn(g.Column)))
                    up.Add(BuildImplicitUniqueIndexSql(table, column));
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");
                AddCreateTableWithIndexes(down, table);
            }

            // Dropped columns on tables NOT recreated in Down are restored with ALTER TABLE ADD COLUMN.
            // Their Up removal is always handled by the consolidated Up recreate; when the table is also
            // recreated in Down (e.g. a dropped PK column, or another altered column on the same table),
            // the restore is folded into that Down recreate instead.
            foreach (var group in diff.DroppedColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                if (downRecreatedTables.Contains(table.Name))
                    continue;
                foreach (var (_, droppedCol) in group)
                {
                    if (IsComputedColumn(droppedCol))
                    {
                        down.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(droppedCol)}");
                        continue;
                    }

                    var restoreDefault = !string.IsNullOrEmpty(droppedCol.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(droppedCol.DefaultValue)}"
                        : "";
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {Esc(droppedCol.Name)} {GetSqlType(droppedCol)}{FormatCollation(droppedCol)} {(droppedCol.IsNullable ? "NULL" : "NOT NULL")}{restoreDefault}");
                }
                foreach (var (_, droppedCol) in group.Where(g => IsImplicitUniqueColumn(g.Column)))
                    down.Add(BuildImplicitUniqueIndexSql(table, droppedCol));
            }

            // Rename columns — SQLite 3.25+ supports ALTER TABLE t RENAME COLUMN old TO new. When the
            // table is recreated in a given direction the rename is folded into that recreate's
            // INSERT ... SELECT (old->new on Up, new->old on Down); emitting a standalone RENAME then
            // would target a column the recreate already renamed and fail. Only rename here for the
            // direction(s) where the table is not recreated. Renames MUST precede the index blocks
            // below: a rebuilt index for a renamed column references the NEW name on Up (and the OLD
            // name on Down), which only exists after the rename in that direction has run.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
            {
                if (!upRecreatedTables.Contains(table.Name))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(oldColName)} TO {Esc(newCol.Name)}");
                if (!downRecreatedTables.Contains(table.Name))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(newCol.Name)} TO {Esc(oldColName)}");
            }

            foreach (var (table, expressionIndex) in diff.DroppedExpressionIndexes)
            {
                if (!upRecreatedTables.Contains(table.Name))
                    up.Add($"DROP INDEX IF EXISTS {EscIndexName(table.Name, expressionIndex.Name)}");
            }
            foreach (var (table, indexName) in diff.DroppedIndexes)
            {
                if (!upRecreatedTables.Contains(table.Name))
                    up.Add($"DROP INDEX IF EXISTS {EscIndexName(table.Name, indexName)}");
            }
            foreach (var (table, expressionIndex) in diff.AddedExpressionIndexes)
            {
                if (!upRecreatedTables.Contains(table.Name))
                    up.Add(BuildExpressionIndexSql(table, expressionIndex));
            }
            foreach (var (table, indexName, isUnique, columnNames, descending) in diff.AddedIndexes)
            {
                if (!upRecreatedTables.Contains(table.Name))
                    up.Add(BuildIndexSql(table, indexName, isUnique, columnNames, descending));
            }

            var droppedExpressionIndexNamesByTable = diff.DroppedExpressionIndexes
                .GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    static group => group.Key,
                    static group => group.Select(static item => item.ExpressionIndex.Name).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);
            var droppedIndexNamesByTable = diff.DroppedIndexes
                .GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    static group => group.Key,
                    static group => group.Select(static item => item.IndexName).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            foreach (var (table, expressionIndex) in diff.AddedExpressionIndexes)
            {
                var restoredSameName = droppedExpressionIndexNamesByTable.TryGetValue(table.Name, out var droppedNames)
                    && droppedNames.Contains(expressionIndex.Name);
                if (!downRecreatedTables.Contains(table.Name) || !restoredSameName)
                    down.Add($"DROP INDEX IF EXISTS {EscIndexName(table.Name, expressionIndex.Name)}");
            }
            foreach (var (table, indexName, _, _, _) in diff.AddedIndexes)
            {
                var restoredSameName = droppedIndexNamesByTable.TryGetValue(table.Name, out var droppedNames)
                    && droppedNames.Contains(indexName);
                if (!downRecreatedTables.Contains(table.Name) || !restoredSameName)
                    down.Add($"DROP INDEX IF EXISTS {EscIndexName(table.Name, indexName)}");
            }
            foreach (var (table, expressionIndex) in diff.DroppedExpressionIndexes)
            {
                if (!downRecreatedTables.Contains(table.Name))
                    down.Add(BuildExpressionIndexSql(table, expressionIndex));
            }
            foreach (var (table, indexName, isUnique, columnNames, descending) in diff.DroppedIndexes
                .Select(droppedIndex =>
                {
                    var resolved = ResolveIndex(droppedIndex.Table, droppedIndex.IndexName, false, Array.Empty<string>(), Array.Empty<bool>());
                    return (droppedIndex.Table, resolved.IndexName, resolved.IsUnique, resolved.ColumnNames, resolved.Descending);
                }))
            {
                if (!downRecreatedTables.Contains(table.Name))
                    down.Add(BuildIndexSql(table, indexName, isUnique, columnNames, descending));
            }

            // Temporal companions LAST: history mirrors and trigger re-emission reference the
            // main table's post-change shape in each direction, which the statements above
            // establish. Trigger-emulated versioning would otherwise silently stop (recreate
            // drops the triggers) or silently exclude changed columns from history.
            EmitTemporalDdl(up, down, diff, upRecreatedTables, downRecreatedTables);

            // MG-2: Return PRAGMA statements in pre/post transaction segments so callers can
            // execute them outside the migration transaction (required by SQLite documentation).
            var preUp    = needsUpFkPragma   ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=off" } : null;
            var postUp   = needsUpFkPragma   ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=on" }  : null;
            var preDown  = needsDownFkPragma ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=off" } : null;
            var postDown = needsDownFkPragma ? (IReadOnlyList<string>)new[] { "PRAGMA foreign_keys=on" }  : null;

            return new MigrationSqlStatements(up, down, preUp, postUp, preDown, postDown);
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

        private static string FormatIndexColumns(string[] columnNames, bool[] descending, IndexNullSortOrder[] _)
            => string.Join(", ", columnNames.Select((name, index) =>
                Esc(name) + (index < descending.Length && descending[index] ? " DESC" : string.Empty)));

        private static void EnsureNoIncludedColumns(string[] includedColumnNames, string indexName)
        {
            if (includedColumnNames.Length > 0)
                throw new NotSupportedException($"SQLite does not support INCLUDE columns for index '{indexName}'. Use key columns only or keep the covering-index tuning in provider-specific migration code.");
        }

        private static void EnsureNoNullsNotDistinct(bool nullsNotDistinct, string indexName)
        {
            if (nullsNotDistinct)
                throw new NotSupportedException($"SQLite does not support PostgreSQL NULLS NOT DISTINCT semantics for index '{indexName}'. Keep that unique-index behavior in PostgreSQL-specific migration code.");
        }

        private static void EnsureNoNullSortOrders(IndexNullSortOrder[] nullSortOrders, string indexName)
        {
            if (nullSortOrders.Any(static order => order != IndexNullSortOrder.Default))
                throw new NotSupportedException($"SQLite does not support provider-neutral NULLS FIRST/LAST index ordering for index '{indexName}'. Keep that ordering in provider-specific migration code.");
        }

        private static string FormatFilter(string? filterSql)
            => string.IsNullOrWhiteSpace(filterSql) ? string.Empty : " WHERE " + filterSql.Trim();

        private static string BuildExpressionIndexSql(TableSchema table, ExpressionIndexSchema expressionIndex)
        {
            EnsureNoIncludedColumns(expressionIndex.IncludedColumnNames ?? Array.Empty<string>(), expressionIndex.Name);
            EnsureNoNullsNotDistinct(expressionIndex.NullsNotDistinct, expressionIndex.Name);
            EnsureNoNullSortOrders(new[] { expressionIndex.NullSortOrder }, expressionIndex.Name);
            var unique = expressionIndex.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {EscIndexName(table.Name, expressionIndex.Name)} ON {EscIndexTargetTable(table.Name)} ({expressionIndex.ExpressionSql.Trim()}){FormatFilter(expressionIndex.FilterSql)}";
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
            var sql = $"CONSTRAINT {Esc(fk.ConstraintName)} FOREIGN KEY ({depCols}) REFERENCES {EscTableNameOnly(fk.PrincipalTable)}({refCols})";
            if (!string.Equals(onDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON DELETE {onDelete}";
            if (!string.Equals(onUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON UPDATE {onUpdate}";
            return sql;
        }

        private static bool IsComputedColumn(ColumnSchema column) => column.ComputedColumnSql is not null;

        private static string BuildComputedColumnDefinition(ColumnSchema column)
        {
            if (string.IsNullOrWhiteSpace(column.ComputedColumnSql))
                throw new NotSupportedException($"Computed column '{column.Name}' requires ComputedColumnSql for SQLite migration generation.");
            var storage = column.IsStoredComputedColumn ? " STORED" : " VIRTUAL";
            return $"{Esc(column.Name)} {GetSqlType(column)} GENERATED ALWAYS AS ({FormatCheckPredicate(column.ComputedColumnSql)}){storage}";
        }

        private static string BuildCheckConstraintSql(CheckConstraintSchema check)
        {
            ArgumentNullException.ThrowIfNull(check);
            return $"CONSTRAINT {Esc(check.ConstraintName)} CHECK ({FormatCheckPredicate(check.Sql)})";
        }

        private static string FormatCheckPredicate(string sql)
        {
            ArgumentNullException.ThrowIfNull(sql);
            var trimmed = sql.Trim();
            if (trimmed.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase))
            {
                var open = trimmed.IndexOf('(');
                var close = trimmed.LastIndexOf(')');
                if (open >= 0 && close > open)
                    trimmed = trimmed.Substring(open + 1, close - open - 1).Trim();
            }
            return trimmed;
        }

        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026:RequiresUnreferencedCode",
            Justification = "ResolveType is only used for design-time enum-to-underlying-type mapping; the enum type should be present in the loaded assembly set.")]
        private static string GetSqlType(ColumnSchema column)
        {
            ArgumentNullException.ThrowIfNull(column);

            // An explicit HasColumnType (StoreType) overrides the CLR-derived type entirely.
            if (!string.IsNullOrEmpty(column.StoreType))
                return column.StoreType;

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

        private static string FormatCollation(ColumnSchema column)
            => string.IsNullOrWhiteSpace(column.Collation)
                ? string.Empty
                : $" COLLATE {ValidateCollationIdentifier(column.Collation)}";

        private static string ValidateCollationIdentifier(string collation)
        {
            var value = collation.Trim();
            if (value.Length == 0)
                throw new ArgumentException("Collation cannot be empty.", nameof(collation));

            foreach (var ch in value)
            {
                if (!char.IsLetterOrDigit(ch) && ch != '_' && ch != '-')
                    throw new ArgumentException($"Collation '{collation}' contains unsupported characters.");
            }

            return value;
        }

        // NOTE: identical copies of ResolveType and ValidateFkAction exist in the other three generators;
        // consolidate into a shared base class or static helper if one is introduced in the future.

        // Cache for ResolveType to avoid scanning all loaded assemblies on every call.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type?> _resolveTypeCache
            = new(StringComparer.Ordinal);

        // Resolve a CLR type by its full name, scanning loaded assemblies when Type.GetType fails.
        // Results are cached in _resolveTypeCache to avoid repeated AppDomain scans.
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Type resolution by assembly-qualified name is not trim-safe; the target type may be removed by the linker.")]
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
