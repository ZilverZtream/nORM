using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Configuration;

namespace nORM.Migration
{
    /// <summary>
    /// Generates MySQL-compatible SQL statements for applying schema migrations.
    /// This implementation translates high level <see cref="SchemaDiff"/> objects
    /// into SQL required to upgrade or downgrade a database.
    /// </summary>
    /// <remarks>
    /// Provider: MySQL (backtick-escaped identifiers). Uses AUTO_INCREMENT for identity columns,
    /// MODIFY COLUMN for alterations, and DROP FOREIGN KEY (not DROP CONSTRAINT) for FK removal.
    /// FK referential actions are validated against an allowlist before being interpolated into DDL.
    /// </remarks>
    public partial class MySqlMigrationSqlGenerator : IMigrationSqlGenerator
    {
        /// <summary>Default MySQL fallback type for unmapped CLR types.</summary>
        private const string FallbackSqlType = "LONGTEXT";

        /// <summary>Conservative utf8mb4-safe MySQL VARCHAR bound before LONGTEXT fallback.</summary>
        private const int MaxBoundedVarCharLength = 16383;

        /// <summary>Conservative MySQL VARBINARY bound before BLOB fallback.</summary>
        private const int MaxBoundedVarBinaryLength = 16383;

        private static readonly Dictionary<string, string> TypeMap = new()
        {
            { typeof(int).FullName!, "INT" },
            { typeof(long).FullName!, "BIGINT" },
            { typeof(short).FullName!, "SMALLINT" },
            // byte is unsigned in .NET (0..255); signed TINYINT overflows above 127.
            { typeof(byte).FullName!, "TINYINT UNSIGNED" },
            { typeof(bool).FullName!, "TINYINT(1)" },
            { typeof(string).FullName!, "LONGTEXT" },
            // Temporal types carry (6): bare DATETIME/TIME hold whole seconds only and
            // MySQL ROUNDS fractional seconds on write, silently changing the stored
            // instant. The query layer also assumes microsecond DATETIME(6) storage.
            { typeof(DateTime).FullName!, "DATETIME(6)" },
            { typeof(decimal).FullName!, "DECIMAL(18,2)" },
            { typeof(double).FullName!, "DOUBLE" },
            { typeof(float).FullName!, "FLOAT" },
            { typeof(Guid).FullName!, "CHAR(36)" },
            { typeof(byte[]).FullName!, "BLOB" },
            { typeof(DateOnly).FullName!, "DATE" },
            { typeof(TimeOnly).FullName!, "TIME(6)" },
            { typeof(DateTimeOffset).FullName!, "DATETIME(6)" },
            { typeof(TimeSpan).FullName!, "TIME(6)" },
            { typeof(char).FullName!, "CHAR(1)" },
            { typeof(sbyte).FullName!, "TINYINT" },
            { typeof(ushort).FullName!, "SMALLINT UNSIGNED" },
            { typeof(uint).FullName!, "INT UNSIGNED" },
            { typeof(ulong).FullName!, "BIGINT UNSIGNED" }
        };

        // Escape MySQL identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return $"`{id.Replace("`", "``")}`";
        }

        private static string EscTable(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return string.Join(".", id.Split('.').Select(Esc));
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
            EnsureNoFilter(index.FilterSql, index.IndexName);
            var unique = index.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {Esc(index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)})";
        }

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

        private static void ValidateUnsupportedIdentityOperations(SchemaDiff diff)
        {
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (oldCol.IsIdentity != newCol.IsIdentity
                    || oldCol.IdentitySeed != newCol.IdentitySeed
                    || oldCol.IdentityIncrement != newCol.IdentityIncrement)
                    throw new NotSupportedException(
                        $"Changing identity metadata for column '{newCol.Name}' on table '{table.Name}' is not supported by MySQL migration generation. " +
                        "Drop and recreate the table or use a provider-specific custom migration.");
            }

            foreach (var (table, column) in diff.AddedColumns)
            {
                if (column.IsIdentity)
                    throw new NotSupportedException(
                        $"Adding identity column '{column.Name}' to existing table '{table.Name}' is not supported by MySQL migration generation. " +
                        "Create a new table or use a provider-specific custom migration.");
            }

            foreach (var (table, column) in diff.DroppedColumns)
            {
                if (column.IsIdentity)
                    throw new NotSupportedException(
                        $"Dropping identity column '{column.Name}' from table '{table.Name}' is not supported by MySQL migration generation because the Down migration cannot restore identity metadata safely. " +
                        "Drop and recreate the table or use a provider-specific custom migration.");
            }
        }

        /// <summary>
        /// Generates MySQL-specific SQL statements that apply the schema changes described by the provided diff.
        /// </summary>
        /// <param name="diff">The differences between the current and desired database schema.</param>
        /// <returns>A pair of SQL statement lists for migrating up and rolling back.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);
            ValidateUnsupportedIdentityOperations(diff);

            var up = new List<string>();
            var down = new List<string>();
            var primaryKeyChanges = SchemaDiffer.GetPrimaryKeyChanges(diff);

            // â”€ UP: correct DDL dependency ordering â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            // FK constraints must be dropped BEFORE the columns/tables they reference
            // are removed. Symmetric rule for DOWN: FK constraints added in UP must be
            // dropped BEFORE the columns/tables added in UP are removed.

            // UP-1: Drop FK constraints first (before columns/tables they depend on).
            // MySQL uses DROP FOREIGN KEY (not DROP CONSTRAINT) for FK removal.
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP FOREIGN KEY {Esc(fk.ConstraintName)}");
            foreach (var (table, check) in diff.DroppedCheckConstraints)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CHECK {Esc(check.ConstraintName)}");
            foreach (var (table, indexName) in diff.DroppedIndexes)
                up.Add($"DROP INDEX {Esc(indexName)} ON {EscTable(table.Name)}");

            // UP-2: Drop tables.
            foreach (var table in diff.DroppedTables)
                up.Add($"DROP TABLE {EscTable(table.Name)}");

            // UP-3: Drop columns (safe â€” FKs on those columns are removed in UP-1).
            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                if (group.Any(static item => item.Column.IsPrimaryKey))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP PRIMARY KEY");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    up.Add($"DROP INDEX {Esc(GetUniqueConstraintName(table, column))} ON {EscTable(table.Name)}");
            }
            foreach (var (table, column) in diff.DroppedColumns)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // UP-3b: Rename columns BEFORE anything that references the new names â€”
            // altered-column statements and rebuilt indexes for a renamed column
            // reference the post-rename name, which must exist by then.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
                up.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(oldColName)} TO {Esc(newCol.Name)}");

            // UP-4: Alter existing columns.
            foreach (var (table, oldPkCols, _) in primaryKeyChanges.Where(static change => change.OldPrimaryKeyColumns.Length > 0))
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP PRIMARY KEY");
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (IsComputedColumn(newCol) || IsComputedColumn(oldCol))
                {
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(oldCol.Name)}");
                    if (IsComputedColumn(newCol))
                        up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(newCol)}");
                    else
                        up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {Esc(newCol.Name)} {GetSqlType(newCol)}{FormatCollation(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}");
                    continue;
                }
                // M1: Include DEFAULT in MODIFY COLUMN â€” MySQL replaces the full column definition.
                if (IsImplicitUniqueColumn(oldCol) && !IsImplicitUniqueColumn(newCol))
                    up.Add($"DROP INDEX {Esc(GetUniqueConstraintName(table, oldCol))} ON {EscTable(table.Name)}");
                var newDefault = newCol.DefaultValue != null ? $" DEFAULT {FormatDefaultValue(newCol)}" : "";
                var newDef = $"{Esc(newCol.Name)} {GetSqlType(newCol)}{FormatCollation(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}{newDefault}";
                up.Add($"ALTER TABLE {EscTable(table.Name)} MODIFY COLUMN {newDef}");
                if (!IsImplicitUniqueColumn(oldCol) && IsImplicitUniqueColumn(newCol))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, newCol)}");
            }
            foreach (var (table, _, newPkCols) in primaryKeyChanges.Where(static change => change.NewPrimaryKeyColumns.Length > 0))
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, newPkCols)}");

            // UP-5: Create new tables (including inline FK constraints).
            foreach (var table in diff.AddedTables)
            {
                var colDefs = table.Columns.Select(c =>
                {
                    if (IsComputedColumn(c))
                        return BuildComputedColumnDefinition(c);
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {FormatDefaultValue(c)}"
                        : "";
                    var identityPart = c.IsIdentity ? " AUTO_INCREMENT" : "";
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{identityPart}{defaultPart}{FormatColumnComment(c.Comment)}";
                }).ToList();

                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));

                foreach (var uc in table.Columns.Where(IsImplicitUniqueColumn))
                    colDefs.Add(BuildUniqueConstraintSql(table, uc));

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
                    EnsureNoFilter(index.FilterSql, index.IndexName);
                    up.Add($"CREATE {unique}INDEX {Esc(index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)})");
                }
                foreach (var expressionIndex in table.ExpressionIndexes)
                    up.Add(BuildExpressionIndexSql(table, expressionIndex));
            }

            // UP-6: Add columns to existing tables.
            foreach (var (table, column) in diff.AddedColumns)
            {
                if (IsComputedColumn(column))
                {
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(column)}");
                    continue;
                }

                if (!column.IsNullable && column.DefaultValue == null)
                    throw new InvalidOperationException(
                        $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                        "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");

                // A NULLABLE added column must carry its declared DEFAULT too - dropping it silently
                // diverges the migrated schema from a freshly-created one and new inserts would not
                // honour the model's default. (Sister fix in the SQLite/Postgres generators; the
                // SqlServer generator already applied its inline default clause unconditionally.)
                var nullPart = column.IsNullable
                    ? (!string.IsNullOrEmpty(column.DefaultValue) ? $"NULL DEFAULT {FormatDefaultValue(column)}" : "NULL")
                    : $"NOT NULL DEFAULT {FormatDefaultValue(column)}";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {nullPart}{FormatColumnComment(column.Comment)}";
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {colDef}");
            }
            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, pkCols)}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, column)}");
            }

            // UP-7: Add FK constraints last (all tables and columns are in place).
            foreach (var (table, check) in diff.AddedCheckConstraints)
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildCheckConstraintSql(check)}");
            foreach (var (table, indexName, isUnique, columnNames, descending) in diff.AddedIndexes)
                up.Add(BuildIndexSql(table, indexName, isUnique, columnNames, descending));
            foreach (var (table, fk) in diff.AddedForeignKeys)
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildFkConstraintSql(fk)}");
            foreach (var (table, expressionIndex) in diff.AddedExpressionIndexes)
                up.Add(BuildExpressionIndexSql(table, expressionIndex));

            // â”€ DOWN: reverse of UP, with symmetric FK ordering â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            // DOWN-1: Drop FK constraints that were added in UP-7 (before touching their columns).
            foreach (var (table, fk) in diff.AddedForeignKeys)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP FOREIGN KEY {Esc(fk.ConstraintName)}");
            foreach (var (table, check) in diff.AddedCheckConstraints)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CHECK {Esc(check.ConstraintName)}");
            foreach (var (table, indexName, _, _, _) in diff.AddedIndexes)
                down.Add($"DROP INDEX {Esc(indexName)} ON {EscTable(table.Name)}");
            foreach (var (table, expressionIndex) in diff.AddedExpressionIndexes)
                down.Add($"DROP INDEX {Esc(expressionIndex.Name)} ON {EscTable(table.Name)}");

            // DOWN-2: Drop columns that were added in UP-6.
            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                if (group.Any(static item => item.Column.IsPrimaryKey))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP PRIMARY KEY");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    down.Add($"DROP INDEX {Esc(GetUniqueConstraintName(table, column))} ON {EscTable(table.Name)}");
            }
            foreach (var (table, column) in diff.AddedColumns)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // DOWN-3: Drop tables that were created in UP-5.
            foreach (var table in diff.AddedTables)
                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");

            // DOWN-3b: Rename columns back BEFORE anything that references the old
            // names â€” alteration reverts and restored indexes use pre-rename names.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
                down.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(newCol.Name)} TO {Esc(oldColName)}");

            // DOWN-4: Reverse column alterations from UP-4.
            foreach (var (table, _, newPkCols) in primaryKeyChanges.Where(static change => change.NewPrimaryKeyColumns.Length > 0))
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP PRIMARY KEY");
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (IsComputedColumn(newCol) || IsComputedColumn(oldCol))
                {
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(newCol.Name)}");
                    if (IsComputedColumn(oldCol))
                        down.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(oldCol)}");
                    else
                        down.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {Esc(oldCol.Name)} {GetSqlType(oldCol)}{FormatCollation(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}");
                    continue;
                }

                if (IsImplicitUniqueColumn(newCol) && !IsImplicitUniqueColumn(oldCol))
                    down.Add($"DROP INDEX {Esc(GetUniqueConstraintName(table, newCol))} ON {EscTable(table.Name)}");
                var oldDefault = oldCol.DefaultValue != null ? $" DEFAULT {FormatDefaultValue(oldCol)}" : "";
                var oldDef = $"{Esc(oldCol.Name)} {GetSqlType(oldCol)}{FormatCollation(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}{oldDefault}";
                down.Add($"ALTER TABLE {EscTable(table.Name)} MODIFY COLUMN {oldDef}");
                if (!IsImplicitUniqueColumn(newCol) && IsImplicitUniqueColumn(oldCol))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, oldCol)}");
            }
            foreach (var (table, oldPkCols, _) in primaryKeyChanges.Where(static change => change.OldPrimaryKeyColumns.Length > 0))
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, oldPkCols)}");

            // DOWN-5: Restore columns that were dropped in UP-3.
            // C: include DefaultValue in the column definition so NOT NULL restore doesn't fail.
            foreach (var (table, column) in diff.DroppedColumns)
            {
                if (IsComputedColumn(column))
                {
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {BuildComputedColumnDefinition(column)}");
                    continue;
                }

                var restoreDefault = column.DefaultValue != null
                    ? $" DEFAULT {FormatDefaultValue(column)}"
                    : "";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}{restoreDefault}";
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {colDef}");
            }
            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, pkCols)}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, column)}");
            }

            // DOWN-6: Restore tables that were dropped in UP-2.
            foreach (var table in diff.DroppedTables)
            {
                var colDefs = table.Columns.Select(c =>
                {
                    if (IsComputedColumn(c))
                        return BuildComputedColumnDefinition(c);
                    var defaultPart = !string.IsNullOrEmpty(c.DefaultValue)
                        ? $" DEFAULT {FormatDefaultValue(c)}"
                        : "";
                    var identityPart = c.IsIdentity ? " AUTO_INCREMENT" : "";
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{identityPart}{defaultPart}{FormatColumnComment(c.Comment)}";
                }).ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));
                foreach (var uc in table.Columns.Where(IsImplicitUniqueColumn))
                    colDefs.Add(BuildUniqueConstraintSql(table, uc));
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
                    EnsureNoFilter(index.FilterSql, index.IndexName);
                    down.Add($"CREATE {unique}INDEX {Esc(index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)})");
                }
                foreach (var expressionIndex in table.ExpressionIndexes)
                    down.Add(BuildExpressionIndexSql(table, expressionIndex));
            }

            // DOWN-7: Restore FK constraints that were dropped in UP-1.
            foreach (var (table, check) in diff.DroppedCheckConstraints)
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildCheckConstraintSql(check)}");
            foreach (var (table, indexName, isUnique, columnNames, descending) in diff.DroppedIndexes
                .Select(droppedIndex =>
                {
                    var resolved = ResolveIndex(droppedIndex.Table, droppedIndex.IndexName, false, Array.Empty<string>(), Array.Empty<bool>());
                    return (droppedIndex.Table, resolved.IndexName, resolved.IsUnique, resolved.ColumnNames, resolved.Descending);
                }))
                down.Add(BuildIndexSql(table, indexName, isUnique, columnNames, descending));
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildFkConstraintSql(fk)}");
            foreach (var (table, expressionIndex) in diff.DroppedExpressionIndexes)
                down.Add(BuildExpressionIndexSql(table, expressionIndex));

            // Temporal companions LAST: history mirrors and trigger re-emission reference the main

            // table's post-change shape in each direction, established by the statements above.

            EmitTemporalDdl(up, down, diff);


            return new MigrationSqlStatements(up, down);
        }

        /// <summary>
        /// Resolves the MySQL column definition for the supplied column schema. Unmapped
        /// CLR types default to <c>LONGTEXT</c> to ensure the migration can be executed
        /// even when a specific mapping is unknown.
        /// </summary>
        /// <param name="column">The column metadata describing the CLR type.</param>
        /// <returns>The corresponding MySQL data type.</returns>
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026:RequiresUnreferencedCode",
            Justification = "ResolveType is only used for design-time enum-to-underlying-type mapping; the enum type should be present in the loaded assembly set.")]
        private static string GetSqlType(ColumnSchema column)
        {
            ArgumentNullException.ThrowIfNull(column);

            // An explicit HasColumnType (StoreType) overrides the CLR-derived type entirely.
            if (!string.IsNullOrEmpty(column.StoreType))
                return column.StoreType;

            if (TryGetDecimalWithPrecisionType(column, out var decimalSql))
                return decimalSql;

            if (TryGetBoundedStringOrBinaryType(column, out var boundedSql))
                return boundedSql;

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

        private static bool TryGetBoundedStringOrBinaryType(ColumnSchema column, out string sql)
        {
            sql = string.Empty;
            if (column.MaxLength is not > 0)
                return false;

            if (string.Equals(column.ClrType, typeof(string).FullName, StringComparison.Ordinal))
            {
                if (column.MaxLength.Value <= MaxBoundedVarCharLength)
                {
                    var typeName = column.IsFixedLength ? "CHAR" : "VARCHAR";
                    sql = $"{typeName}({column.MaxLength.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
                    return true;
                }

                return false;
            }

            if (string.Equals(column.ClrType, typeof(byte[]).FullName, StringComparison.Ordinal))
            {
                if (column.MaxLength.Value <= MaxBoundedVarBinaryLength)
                {
                    var typeName = column.IsFixedLength ? "BINARY" : "VARBINARY";
                    sql = $"{typeName}({column.MaxLength.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
                    return true;
                }

                return false;
            }

            return false;
        }

        private static bool TryGetDecimalWithPrecisionType(ColumnSchema column, out string sql)
        {
            sql = string.Empty;
            if (!string.Equals(column.ClrType, typeof(decimal).FullName, StringComparison.Ordinal)
                || column.Precision is not > 0
                || (column.Scale.HasValue && (column.Scale.Value < 0 || column.Scale.Value > column.Precision.Value)))
            {
                return false;
            }

            var precision = column.Precision.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            sql = column.Scale.HasValue
                ? $"DECIMAL({precision},{column.Scale.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})"
                : $"DECIMAL({precision})";
            return true;
        }

        /// <summary>
        /// Validates a column default and aligns zero-argument temporal function
        /// defaults with the column's fractional-seconds precision. MySQL requires a
        /// temporal default's precision to MATCH the column's exactly, so a bare
        /// <c>CURRENT_TIMESTAMP</c> (or <c>NOW()</c>/<c>LOCALTIME</c>/<c>LOCALTIMESTAMP</c>)
        /// default is rejected on the DATETIME(6)/TIME(6) columns this generator emits
        /// ("Invalid default value") and must become <c>CURRENT_TIMESTAMP(6)</c>.
        /// </summary>
        private static string FormatDefaultValue(ColumnSchema column)
        {
            var validated = DefaultValueValidator.Validate(column.DefaultValue!) ?? string.Empty;
            if (!GetSqlType(column).EndsWith("(6)", StringComparison.Ordinal))
                return validated;

            var trimmed = validated.Trim();
            var name = trimmed.EndsWith("()", StringComparison.Ordinal) ? trimmed[..^2] : trimmed;
            return name.ToUpperInvariant() switch
            {
                "CURRENT_TIMESTAMP" or "NOW" or "LOCALTIME" or "LOCALTIMESTAMP" => $"{name}(6)",
                _ => validated,
            };
        }

        /// <summary>
        /// Formats an inline MySQL <c>COMMENT</c> clause for a column. The comment text is emitted as a
        /// single-quoted string literal with quotes doubled and backslashes escaped so it cannot terminate
        /// the literal or inject DDL.
        /// </summary>
        private static string FormatColumnComment(string? comment)
        {
            if (string.IsNullOrWhiteSpace(comment))
                return "";
            var escaped = comment
                .Replace("\\", "\\\\", StringComparison.Ordinal)
                .Replace("'", "''", StringComparison.Ordinal);
            return $" COMMENT '{escaped}'";
        }

        private static bool IsComputedColumn(ColumnSchema column) => column.ComputedColumnSql is not null;

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
                if (!char.IsLetterOrDigit(ch) && ch != '_')
                    throw new ArgumentException($"Collation '{collation}' contains unsupported characters.");
            }

            return value;
        }

        private static string BuildComputedColumnDefinition(ColumnSchema column)
        {
            if (string.IsNullOrWhiteSpace(column.ComputedColumnSql))
                throw new NotSupportedException($"Computed column '{column.Name}' requires ComputedColumnSql for MySQL migration generation.");
            var storage = column.IsStoredComputedColumn ? " STORED" : " VIRTUAL";
            return $"{Esc(column.Name)} {GetSqlType(column)} GENERATED ALWAYS AS ({FormatCheckPredicate(column.ComputedColumnSql)}){storage}";
        }

        // M1/X1: Allowlist for FK referential action tokens.
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
                throw new NotSupportedException($"MySQL does not support INCLUDE columns for index '{indexName}'. Use key columns only or keep the covering-index tuning in provider-specific migration code.");
        }

        private static void EnsureNoNullsNotDistinct(bool nullsNotDistinct, string indexName)
        {
            if (nullsNotDistinct)
                throw new NotSupportedException($"MySQL does not support PostgreSQL NULLS NOT DISTINCT semantics for index '{indexName}'. Keep that unique-index behavior in PostgreSQL-specific migration code.");
        }

        private static void EnsureNoNullSortOrders(IndexNullSortOrder[] nullSortOrders, string indexName)
        {
            if (nullSortOrders.Any(static order => order != IndexNullSortOrder.Default))
                throw new NotSupportedException($"MySQL does not support provider-neutral NULLS FIRST/LAST index ordering for index '{indexName}'. Keep that ordering in provider-specific migration code.");
        }

        private static void EnsureNoFilter(string? filterSql, string indexName)
        {
            if (!string.IsNullOrWhiteSpace(filterSql))
                throw new NotSupportedException($"MySQL does not support filtered indexes for index '{indexName}'. Keep the predicate in provider-specific migration code or remodel it as a generated column plus ordinary index.");
        }

        private static string BuildExpressionIndexSql(TableSchema table, ExpressionIndexSchema expressionIndex)
        {
            EnsureNoIncludedColumns(expressionIndex.IncludedColumnNames ?? Array.Empty<string>(), expressionIndex.Name);
            EnsureNoNullsNotDistinct(expressionIndex.NullsNotDistinct, expressionIndex.Name);
            EnsureNoNullSortOrders(new[] { expressionIndex.NullSortOrder }, expressionIndex.Name);
            EnsureNoFilter(expressionIndex.FilterSql, expressionIndex.Name);
            var unique = expressionIndex.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {Esc(expressionIndex.Name)} ON {EscTable(table.Name)} ({expressionIndex.ExpressionSql.Trim()})";
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
            var sql = $"CONSTRAINT {Esc(fk.ConstraintName)} FOREIGN KEY ({depCols}) REFERENCES {EscTable(fk.PrincipalTable)}({refCols})";
            if (!string.Equals(onDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON DELETE {onDelete}";
            if (!string.Equals(onUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON UPDATE {onUpdate}";
            return sql;
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
