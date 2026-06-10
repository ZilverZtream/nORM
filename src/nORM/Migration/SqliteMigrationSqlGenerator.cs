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

        private static bool RequiresRecreateForAddedColumn(ColumnSchema column)
            => column.IsPrimaryKey;

        private static void ValidateSqliteIdentityMetadata(SchemaDiff diff)
        {
            foreach (var table in EnumerateTablesForIdentityValidation(diff))
                ValidateSqliteIdentityColumns(table.Name, table.Columns);

            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                ValidateSqliteIdentityColumns(table.Name, MergeColumnsForIdentityValidation(table.Columns, group.Select(static item => item.Column)));
            }

            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                ValidateSqliteIdentityColumns(table.Name, MergeColumnsForIdentityValidation(table.Columns, group.Select(static item => item.Column)));
            }

            foreach (var group in diff.AlteredColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var oldColumnsByName = group.ToDictionary(static item => item.NewColumn.Name, static item => item.OldColumn, StringComparer.OrdinalIgnoreCase);
                var oldColumns = table.Columns
                    .Select(column => oldColumnsByName.TryGetValue(column.Name, out var oldColumn) ? oldColumn : column)
                    .ToArray();
                ValidateSqliteIdentityColumns(table.Name, oldColumns);
            }
        }

        private static IEnumerable<TableSchema> EnumerateTablesForIdentityValidation(SchemaDiff diff)
        {
            foreach (var table in diff.AddedTables)
                yield return table;
            foreach (var table in diff.DroppedTables)
                yield return table;
            foreach (var (table, _) in diff.AddedColumns)
                yield return table;
            foreach (var (table, _) in diff.DroppedColumns)
                yield return table;
            foreach (var (table, _, _) in diff.AlteredColumns)
                yield return table;
            foreach (var (table, _) in diff.AddedForeignKeys)
                yield return table;
            foreach (var (table, _) in diff.DroppedForeignKeys)
                yield return table;
            foreach (var (table, _) in diff.AddedCheckConstraints)
                yield return table;
            foreach (var (table, _) in diff.DroppedCheckConstraints)
                yield return table;
            foreach (var (table, _) in diff.AddedExpressionIndexes)
                yield return table;
            foreach (var (table, _) in diff.DroppedExpressionIndexes)
                yield return table;
            foreach (var (table, _, _, _, _) in diff.AddedIndexes)
                yield return table;
            foreach (var (table, _) in diff.DroppedIndexes)
                yield return table;
        }

        private static IReadOnlyList<ColumnSchema> MergeColumnsForIdentityValidation(
            IEnumerable<ColumnSchema> tableColumns,
            IEnumerable<ColumnSchema> operationColumns)
        {
            var columns = tableColumns.ToDictionary(static column => column.Name, StringComparer.OrdinalIgnoreCase);
            foreach (var column in operationColumns)
                columns[column.Name] = column;
            return columns.Values.ToArray();
        }

        private static void ValidateSqliteIdentityColumns(string tableName, IReadOnlyList<ColumnSchema> columns)
        {
            var identityColumns = columns.Where(static column => column.IsIdentity).ToArray();
            if (identityColumns.Length == 0)
                return;

            if (identityColumns.Length > 1)
                throw new NotSupportedException($"SQLite supports at most one identity column per table. Table '{tableName}' has identity metadata on: {string.Join(", ", identityColumns.Select(static column => column.Name))}.");

            var identityColumn = identityColumns[0];
            var primaryKeyColumns = columns.Where(static column => column.IsPrimaryKey).ToArray();
            if (!identityColumn.IsPrimaryKey)
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' must also be the single primary key column; otherwise AUTOINCREMENT would be ignored.");

            if (primaryKeyColumns.Length != 1)
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' cannot be part of a composite primary key because AUTOINCREMENT requires a single INTEGER PRIMARY KEY column.");

            if (!IsSqliteIntegerIdentityType(identityColumn))
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' must map to INTEGER PRIMARY KEY. CLR type '{identityColumn.ClrType}' maps to '{GetSqlType(identityColumn)}'.");

            if (identityColumn.IdentitySeed is not null || identityColumn.IdentityIncrement is not null)
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' cannot use identity seed or increment options.");

            if (!string.IsNullOrWhiteSpace(identityColumn.DefaultValue))
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' cannot also define a default value.");
        }

        private static bool IsSqliteIntegerIdentityType(ColumnSchema column)
            => !string.Equals(column.ClrType, typeof(bool).FullName, StringComparison.Ordinal)
               && string.Equals(GetSqlType(column), "INTEGER", StringComparison.OrdinalIgnoreCase);

        private static HashSet<string> GetUpRecreatedTableNames(SchemaDiff diff)
            => diff.AlteredColumns.Select(static item => item.Table.Name)
                .Concat(diff.AddedColumns
                    .Where(static item => RequiresRecreateForAddedColumn(item.Column))
                    .Select(static item => item.Table.Name))
                .Concat(diff.DroppedColumns.Select(static item => item.Table.Name))
                .Concat(diff.AddedForeignKeys.Select(static item => item.Table.Name))
                .Concat(diff.DroppedForeignKeys.Select(static item => item.Table.Name))
                .Concat(diff.AddedCheckConstraints.Select(static item => item.Table.Name))
                .Concat(diff.DroppedCheckConstraints.Select(static item => item.Table.Name))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static HashSet<string> GetDownRecreatedTableNames(SchemaDiff diff)
            => diff.AlteredColumns.Select(static item => item.Table.Name)
                .Concat(diff.AddedColumns.Select(static item => item.Table.Name))
                .Concat(diff.DroppedColumns
                    .Where(static item => RequiresRecreateForAddedColumn(item.Column))
                    .Select(static item => item.Table.Name))
                .Concat(diff.AddedForeignKeys.Select(static item => item.Table.Name))
                .Concat(diff.DroppedForeignKeys.Select(static item => item.Table.Name))
                .Concat(diff.AddedCheckConstraints.Select(static item => item.Table.Name))
                .Concat(diff.DroppedCheckConstraints.Select(static item => item.Table.Name))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<ExpressionIndexSchema> GetExpressionIndexesForUp(TableSchema table, SchemaDiff diff)
            => ResolveExpressionIndexesForRecreate(
                table,
                diff.DroppedExpressionIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => item.ExpressionIndex),
                diff.AddedExpressionIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => item.ExpressionIndex));

        private static IReadOnlyList<ExpressionIndexSchema> GetExpressionIndexesForDown(TableSchema table, SchemaDiff diff)
            => ResolveExpressionIndexesForRecreate(
                table,
                diff.AddedExpressionIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => item.ExpressionIndex),
                diff.DroppedExpressionIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => item.ExpressionIndex));

        private static IReadOnlyList<ExpressionIndexSchema> ResolveExpressionIndexesForRecreate(
            TableSchema table,
            IEnumerable<ExpressionIndexSchema> removed,
            IEnumerable<ExpressionIndexSchema> added)
        {
            var indexes = table.ExpressionIndexes.ToDictionary(static index => index.Name, StringComparer.OrdinalIgnoreCase);
            foreach (var index in removed)
                indexes.Remove(index.Name);
            foreach (var index in added)
                indexes[index.Name] = index;
            return indexes.Values.ToArray();
        }

        private static IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> GetExplicitIndexesForUp(TableSchema table, SchemaDiff diff)
            => ResolveExplicitIndexesForRecreate(
                table,
                diff.DroppedIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => item.IndexName),
                diff.AddedIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => ResolveIndex(item.Table, item.IndexName, item.IsUnique, item.ColumnNames, item.Descending)));

        private static IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> GetExplicitIndexesForDown(TableSchema table, SchemaDiff diff)
            => ResolveExplicitIndexesForRecreate(
                table,
                diff.AddedIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => item.IndexName),
                diff.DroppedIndexes
                    .Where(item => string.Equals(item.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(static item => ResolveIndex(item.Table, item.IndexName, false, Array.Empty<string>(), Array.Empty<bool>())));

        private static IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> ResolveExplicitIndexesForRecreate(
            TableSchema table,
            IEnumerable<string> removed,
            IEnumerable<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> added)
        {
            var indexes = SchemaDiffer.GetExplicitIndexes(table).ToDictionary(static index => index.IndexName, StringComparer.OrdinalIgnoreCase);
            foreach (var indexName in removed)
                indexes.Remove(indexName);
            foreach (var index in added)
                indexes[index.IndexName] = index;
            return indexes.Values.ToArray();
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
            ValidateSqliteIdentityMetadata(diff);

            var up = new List<string>();
            var down = new List<string>();
            bool needsUpFkPragma = false;
            bool needsDownFkPragma = false;
            var upRecreatedTables = GetUpRecreatedTableNames(diff);
            var downRecreatedTables = GetDownRecreatedTableNames(diff);

            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                {
                    if (IsComputedColumn(c))
                        return BuildComputedColumnDefinition(c);
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    // SQLite AUTOINCREMENT requires inline "INTEGER PRIMARY KEY AUTOINCREMENT" on the column definition
                    if (c.IsIdentity && c.IsPrimaryKey)
                        return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
                }).ToList();

                // Emit PRIMARY KEY constraint for PK columns.
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                {
                    // SQLite AUTOINCREMENT requires "INTEGER PRIMARY KEY AUTOINCREMENT" inline, not table-level constraint
                    if (!pkCols.Any(c => c.IsIdentity))
                        colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));
                }

                foreach (var uc in table.Columns.Where(IsImplicitUniqueColumn))
                    colDefs.Add(BuildUniqueConstraintSql(table, uc));

                // MG-1: Emit inline FOREIGN KEY constraints
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                foreach (var check in table.CheckConstraints)
                    colDefs.Add(BuildCheckConstraintSql(check));

                up.Add($"CREATE TABLE {EscTable(table.Name)} ({string.Join(", ", colDefs)})");

                foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
                {
                    var unique = index.IsUnique ? "UNIQUE " : string.Empty;
                    EnsureNoIncludedColumns(index.IncludedColumnNames, index.IndexName);
                    EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
                    EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
                    up.Add($"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscIndexTargetTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatFilter(index.FilterSql)}");
                }
                foreach (var expressionIndex in table.ExpressionIndexes)
                    up.Add(BuildExpressionIndexSql(table, expressionIndex));

                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");
            }

            foreach (var group in diff.AddedColumns.GroupBy(x => x.Table))
            {
                var table = group.Key;
                var addedColumnNames = group.Select(g => g.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var addedColumns = group.Select(static g => g.Column).ToArray();
                var recreateForAdd = addedColumns.Any(RequiresRecreateForAddedColumn);

                foreach (var column in addedColumns)
                {
                    // NOT NULL column without a DefaultValue cannot be added to a populated table.
                    if (!IsComputedColumn(column) && !column.IsNullable && column.DefaultValue == null)
                        throw new InvalidOperationException(
                            $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                            "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");
                }

                if (recreateForAdd)
                {
                    RecreateTable(
                        up,
                        table,
                        table.Columns.ToList(),
                        null,
                        explicitIndexes: GetExplicitIndexesForUp(table, diff),
                        expressionIndexes: GetExpressionIndexesForUp(table, diff),
                        addedColumnNames: addedColumnNames);
                    needsUpFkPragma = true;
                }
                else
                {
                    foreach (var column in addedColumns)
                    {
                        if (IsComputedColumn(column))
                        {
                            up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(column)}");
                            continue;
                        }

                        var nullPart = column.IsNullable ? "NULL" : $"NOT NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}";
                        var colDef = $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {nullPart}";
                        up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {colDef}");
                    }
                    foreach (var column in addedColumns.Where(IsImplicitUniqueColumn))
                        up.Add(BuildImplicitUniqueIndexSql(table, column));
                }

                // Down: undo the ADD COLUMN by recreating the table without those columns.
                // MG-2: RecreateTable no longer emits PRAGMA inline.
                var remainingColumns = table.Columns
                    .Where(c => !addedColumnNames.Contains(c.Name))
                    .ToList();
                if (remainingColumns.Count > 0)
                {
                    RecreateTable(
                        down,
                        table,
                        remainingColumns,
                        null,
                        explicitIndexes: GetExplicitIndexesForDown(table, diff),
                        expressionIndexes: GetExpressionIndexesForDown(table, diff));
                    needsDownFkPragma = true;
                }
            }

            // G2: SQLite does not support ALTER COLUMN; use the standard table-recreation workaround.
            foreach (var group in diff.AlteredColumns.GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table         = diff.AlteredColumns.First(x => string.Equals(x.Table.Name, group.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var alteredMap    = group.ToDictionary(x => x.NewColumn.Name, x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
                var oldAlteredMap = group.ToDictionary(x => x.OldColumn.Name, x => x.OldColumn, StringComparer.OrdinalIgnoreCase);

                AddRecreate(up,   table, alteredMap, GetExplicitIndexesForUp(table, diff), GetExpressionIndexesForUp(table, diff));
                AddRecreate(down, table, oldAlteredMap, GetExplicitIndexesForDown(table, diff), GetExpressionIndexesForDown(table, diff));
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            // SD-8: Generate DROP TABLE for tables removed in the new snapshot
            foreach (var table in diff.DroppedTables)
            {
                up.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");
                // Down: recreate the table with full constraint metadata
                var colDefs = table.Columns.Select(c =>
                {
                    if (IsComputedColumn(c))
                        return BuildComputedColumnDefinition(c);
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    if (c.IsIdentity && c.IsPrimaryKey)
                        return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
                }).ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                {
                    if (!pkCols.Any(c => c.IsIdentity))
                        colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));
                }
                foreach (var uc in table.Columns.Where(IsImplicitUniqueColumn))
                    colDefs.Add(BuildUniqueConstraintSql(table, uc));
                // MG-1: Restore FK constraints in Down recreation
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                foreach (var check in table.CheckConstraints)
                    colDefs.Add(BuildCheckConstraintSql(check));
                down.Add($"CREATE TABLE {EscTable(table.Name)} ({string.Join(", ", colDefs)})");
                foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
                {
                    var unique = index.IsUnique ? "UNIQUE " : string.Empty;
                    EnsureNoIncludedColumns(index.IncludedColumnNames, index.IndexName);
                    EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
                    EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
                    down.Add($"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscIndexTargetTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatFilter(index.FilterSql)}");
                }
                foreach (var expressionIndex in table.ExpressionIndexes)
                    down.Add(BuildExpressionIndexSql(table, expressionIndex));
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
                RecreateTable(
                    up,
                    newTable,
                    remainingCols,
                    null,
                    explicitIndexes: GetExplicitIndexesForUp(newTable, diff),
                    expressionIndexes: GetExpressionIndexesForUp(newTable, diff));
                needsUpFkPragma = true;

                // Down: add the dropped columns back (SQLite ADD COLUMN is forward-compatible)
                // C: include DefaultValue so NOT NULL columns can be restored to populated tables.
                if (droppedCols.Any(RequiresRecreateForAddedColumn))
                {
                    RecreateTable(
                        down,
                        newTable,
                        newTable.Columns.ToList(),
                        null,
                        explicitIndexes: GetExplicitIndexesForDown(newTable, diff),
                        expressionIndexes: GetExpressionIndexesForDown(newTable, diff),
                        addedColumnNames: droppedColNames);
                    needsDownFkPragma = true;
                    continue;
                }

                foreach (var droppedCol in droppedCols)
                {
                    if (IsComputedColumn(droppedCol))
                    {
                        down.Add($"ALTER TABLE {EscTable(newTable.Name)} ADD COLUMN {BuildComputedColumnDefinition(droppedCol)}");
                        continue;
                    }

                    var restoreDefault = !string.IsNullOrEmpty(droppedCol.DefaultValue)
                        ? $" DEFAULT {DefaultValueValidator.Validate(droppedCol.DefaultValue)}"
                        : "";
                    var colDef = $"{Esc(droppedCol.Name)} {GetSqlType(droppedCol)}{FormatCollation(droppedCol)} {(droppedCol.IsNullable ? "NULL" : "NOT NULL")}{restoreDefault}";
                    down.Add($"ALTER TABLE {EscTable(newTable.Name)} ADD COLUMN {colDef}");
                }
                foreach (var droppedCol in droppedCols.Where(IsImplicitUniqueColumn))
                    down.Add(BuildImplicitUniqueIndexSql(newTable, droppedCol));
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
                RecreateTable(up, table, table.Columns, null, table.ForeignKeys, explicitIndexes: GetExplicitIndexesForUp(table, diff), expressionIndexes: GetExpressionIndexesForUp(table, diff));
                // Down: recreate without the newly added FKs
                var addedNames = tableGroup.Select(x => x.ForeignKey.ConstraintName)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                var oldFks = table.ForeignKeys
                    .Where(fk => !addedNames.Contains(fk.ConstraintName)).ToList();
                RecreateTable(down, table, table.Columns, null, oldFks, explicitIndexes: GetExplicitIndexesForDown(table, diff), expressionIndexes: GetExpressionIndexesForDown(table, diff));
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
                RecreateTable(up, table, table.Columns, null, table.ForeignKeys, explicitIndexes: GetExplicitIndexesForUp(table, diff), expressionIndexes: GetExpressionIndexesForUp(table, diff));
                // Down: recreate with the dropped FKs restored
                var restoredFks = table.ForeignKeys.Concat(droppedFks).ToList();
                RecreateTable(down, table, table.Columns, null, restoredFks, explicitIndexes: GetExplicitIndexesForDown(table, diff), expressionIndexes: GetExpressionIndexesForDown(table, diff));
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            // SQLite cannot ALTER CHECK constraints directly. Recreate the table with
            // the post-diff or pre-diff constraint set, preserving data and indexes.
            foreach (var tableGroup in diff.AddedCheckConstraints
                .GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = diff.AddedCheckConstraints
                    .First(x => string.Equals(x.Table.Name, tableGroup.Key, StringComparison.OrdinalIgnoreCase)).Table;
                RecreateTable(up, table, table.Columns, null, table.ForeignKeys, table.CheckConstraints, GetExplicitIndexesForUp(table, diff), GetExpressionIndexesForUp(table, diff));
                var addedNames = tableGroup.Select(x => x.CheckConstraint.ConstraintName)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                var oldChecks = table.CheckConstraints
                    .Where(check => !addedNames.Contains(check.ConstraintName)).ToList();
                RecreateTable(down, table, table.Columns, null, table.ForeignKeys, oldChecks, GetExplicitIndexesForDown(table, diff), GetExpressionIndexesForDown(table, diff));
                needsUpFkPragma = true;
                needsDownFkPragma = true;
            }

            foreach (var tableGroup in diff.DroppedCheckConstraints
                .GroupBy(x => x.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = diff.DroppedCheckConstraints
                    .First(x => string.Equals(x.Table.Name, tableGroup.Key, StringComparison.OrdinalIgnoreCase)).Table;
                var droppedChecks = tableGroup.Select(x => x.CheckConstraint).ToList();
                RecreateTable(up, table, table.Columns, null, table.ForeignKeys, table.CheckConstraints, GetExplicitIndexesForUp(table, diff), GetExpressionIndexesForUp(table, diff));
                var restoredChecks = table.CheckConstraints.Concat(droppedChecks).ToList();
                RecreateTable(down, table, table.Columns, null, table.ForeignKeys, restoredChecks, GetExplicitIndexesForDown(table, diff), GetExpressionIndexesForDown(table, diff));
                needsUpFkPragma = true;
                needsDownFkPragma = true;
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

            // Rename columns — SQLite 3.25+ supports ALTER TABLE t RENAME COLUMN old TO new.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
            {
                up.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(oldColName)} TO {Esc(newCol.Name)}");
                // Down: rename back
                down.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(newCol.Name)} TO {Esc(oldColName)}");
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
        private static void AddRecreate(
            List<string> stmts,
            TableSchema table,
            Dictionary<string, ColumnSchema> overrides,
            IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> explicitIndexes,
            IReadOnlyList<ExpressionIndexSchema> expressionIndexes)
        {
            var cols = table.Columns.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();
            RecreateTable(stmts, table, cols, null, explicitIndexes: explicitIndexes, expressionIndexes: expressionIndexes);
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
            Dictionary<string, ColumnSchema>? overrides, IReadOnlyList<ForeignKeySchema>? fks = null,
            IReadOnlyList<CheckConstraintSchema>? checks = null,
            IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)>? explicitIndexes = null,
            IReadOnlyList<ExpressionIndexSchema>? expressionIndexes = null,
            IReadOnlySet<string>? addedColumnNames = null)
        {
            // Apply overrides if supplied
            if (overrides != null)
                cols = cols.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();

            // F: compute names AFTER overrides have been applied so the SELECT list uses the correct column set.
            var insertColumns = new List<string>(cols.Count);
            var selectExpressions = new List<string>(cols.Count);
            foreach (var col in cols)
            {
                if (IsComputedColumn(col))
                    continue;

                insertColumns.Add(Esc(col.Name));
                selectExpressions.Add(addedColumnNames?.Contains(col.Name) == true
                    ? GetAddedColumnInsertExpression(table, col)
                    : Esc(col.Name));
            }
            if (cols.Count > 0 && insertColumns.Count == 0)
                throw new NotSupportedException($"Cannot recreate table '{table.Name}' because all retained columns are computed/generated columns.");
            var recreatedTable = new TableSchema { Name = table.Name };
            foreach (var col in cols)
                recreatedTable.Columns.Add(col);

            // Build column definitions with full constraint metadata (same as AddedTables path).
            var colDefs = cols.Select(c =>
            {
                if (IsComputedColumn(c))
                    return BuildComputedColumnDefinition(c);
                var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                    ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                    : "";
                if (c.IsIdentity && c.IsPrimaryKey)
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} NOT NULL PRIMARY KEY AUTOINCREMENT{defaultPart}";
                return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{defaultPart}";
            }).ToList();

            // Emit PRIMARY KEY constraint for PK columns.
            var pkCols = cols.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0)
            {
                // SQLite AUTOINCREMENT requires "INTEGER PRIMARY KEY AUTOINCREMENT" inline, not table-level constraint
                if (!pkCols.Any(c => c.IsIdentity))
                    colDefs.Add(BuildPrimaryKeyConstraintSql(recreatedTable, pkCols));
            }

            foreach (var uc in cols.Where(IsImplicitUniqueColumn))
                colDefs.Add(BuildUniqueConstraintSql(recreatedTable, uc));

            // MG-1: Emit inline FOREIGN KEY constraints (explicit list or fall back to table.ForeignKeys)
            foreach (var fk in fks ?? table.ForeignKeys)
                colDefs.Add(BuildFkConstraintSql(fk));
            foreach (var check in checks ?? table.CheckConstraints)
                colDefs.Add(BuildCheckConstraintSql(check));

            // MG-2: No PRAGMA here — PRAGMA foreign_keys=off/on is returned in the pre/post transaction segments.
            var tempName = EscTempTable(table.Name);
            var renameTarget = EscTableNameOnly(table.Name);
            // G: Drop the temp table if it already exists (handles interrupted prior migration).
            stmts.Add($"DROP TABLE IF EXISTS {tempName}");
            stmts.Add($"CREATE TABLE {tempName} ({string.Join(", ", colDefs)})");
            stmts.Add($"INSERT INTO {tempName} ({string.Join(", ", insertColumns)}) SELECT {string.Join(", ", selectExpressions)} FROM {EscTable(table.Name)}");
            stmts.Add($"DROP TABLE {EscTable(table.Name)}");
            stmts.Add($"ALTER TABLE {tempName} RENAME TO {renameTarget}");

            foreach (var index in explicitIndexes ?? SchemaDiffer.GetExplicitIndexes(recreatedTable))
            {
                var unique = index.IsUnique ? "UNIQUE " : string.Empty;
                EnsureNoIncludedColumns(index.IncludedColumnNames, index.IndexName);
                EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
                EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
                stmts.Add($"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscIndexTargetTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatFilter(index.FilterSql)}");
            }
            foreach (var expressionIndex in expressionIndexes ?? table.ExpressionIndexes)
                stmts.Add(BuildExpressionIndexSql(table, expressionIndex));
        }

        private static string GetAddedColumnInsertExpression(TableSchema table, ColumnSchema column)
        {
            var defaultValue = column.DefaultValue;
            if (!string.IsNullOrEmpty(defaultValue))
                return DefaultValueValidator.Validate(defaultValue)!;
            if (column.IsNullable)
                return "NULL";

            throw new InvalidOperationException(
                $"Cannot recreate table '{table.Name}' with added column '{column.Name}' because it is NOT NULL and has no DefaultValue.");
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
