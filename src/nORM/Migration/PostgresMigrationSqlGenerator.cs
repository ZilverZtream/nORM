using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;

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
    public partial class PostgresMigrationSqlGenerator : IMigrationSqlGenerator
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

        private static string EscTable(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return string.Join(".", id.Split('.').Select(Esc));
        }

        private static string? GetSchemaName(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            var dot = tableName.IndexOf('.');
            return dot <= 0 ? null : tableName[..dot];
        }

        private static IReadOnlyList<string> GetAddedTableSchemas(IEnumerable<TableSchema> tables)
            => tables.Select(table => GetSchemaName(table.Name))
                .Where(static schema => !string.IsNullOrWhiteSpace(schema))
                .Cast<string>()
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static schema => schema, StringComparer.OrdinalIgnoreCase)
                .ToArray();

        private static string EscIndexName(string tableName, string indexName)
        {
            var schema = GetSchemaName(tableName);
            return schema is null ? Esc(indexName) : $"{Esc(schema)}.{Esc(indexName)}";
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
            var unique = index.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatIncludedColumns(index.IncludedColumnNames)}{FormatNullsNotDistinct(index.NullsNotDistinct, index.IsUnique, index.IndexName)}{FormatFilter(index.FilterSql)}";
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

        /// <summary>
        /// Builds a PostgreSQL <c>COMMENT ON COLUMN</c> statement. The comment text is a single-quoted string
        /// literal with embedded quotes doubled; PostgreSQL's default standard_conforming_strings makes the
        /// backslash non-special, so doubling the quote is sufficient to prevent literal termination.
        /// </summary>
        private static string BuildColumnCommentSql(TableSchema table, ColumnSchema column)
            => BuildColumnCommentSql(table, column, column.Comment);

        /// <summary>Builds a <c>COMMENT ON COLUMN</c> statement; a null <paramref name="comment"/> clears it (<c>IS NULL</c>).</summary>
        private static string BuildColumnCommentSql(TableSchema table, ColumnSchema column, string? comment)
            => comment is null
                ? $"COMMENT ON COLUMN {EscTable(table.Name)}.{Esc(column.Name)} IS NULL"
                : $"COMMENT ON COLUMN {EscTable(table.Name)}.{Esc(column.Name)} IS '{comment.Replace("'", "''", StringComparison.Ordinal)}'";

        private static void ValidateUnsupportedIdentityOperations(SchemaDiff diff)
        {
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (oldCol.IsIdentity != newCol.IsIdentity
                    || oldCol.IdentitySeed != newCol.IdentitySeed
                    || oldCol.IdentityIncrement != newCol.IdentityIncrement)
                    throw new NotSupportedException(
                        $"Changing identity metadata for column '{newCol.Name}' on table '{table.Name}' is not supported by PostgreSQL migration generation. " +
                        "Drop and recreate the table or use a provider-specific custom migration.");
            }

            foreach (var (table, column) in diff.AddedColumns)
            {
                if (column.IsIdentity)
                    throw new NotSupportedException(
                        $"Adding identity column '{column.Name}' to existing table '{table.Name}' is not supported by PostgreSQL migration generation. " +
                        "Create a new table or use a provider-specific custom migration.");
            }

            foreach (var (table, column) in diff.DroppedColumns)
            {
                if (column.IsIdentity)
                    throw new NotSupportedException(
                        $"Dropping identity column '{column.Name}' from table '{table.Name}' is not supported by PostgreSQL migration generation because the Down migration cannot restore identity metadata safely. " +
                        "Drop and recreate the table or use a provider-specific custom migration.");
            }
        }

        /// <summary>
        /// Produces PostgreSQL-compatible SQL statements to transition between schema versions.
        /// </summary>
        /// <param name="diff">The description of schema changes to apply.</param>
        /// <returns>A set of statements to apply the changes and to revert them.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);
            ValidateUnsupportedIdentityOperations(diff);

            var up = new List<string>();
            var down = new List<string>();
            var primaryKeyChanges = SchemaDiffer.GetPrimaryKeyChanges(diff);

            // ─ UP: correct DDL dependency ordering ──────────────────────────────────
            // FK constraints must be dropped BEFORE the columns/tables they reference
            // are removed. Symmetric rule for DOWN: FK constraints added in UP must be
            // dropped BEFORE the columns/tables added in UP are removed.

            foreach (var schema in GetAddedTableSchemas(diff.AddedTables))
                up.Add($"CREATE SCHEMA IF NOT EXISTS {Esc(schema)}");

            // UP-1: Drop FK constraints first (before columns/tables they depend on).
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");
            foreach (var (table, check) in diff.DroppedCheckConstraints)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(check.ConstraintName)}");
            foreach (var (table, expressionIndex) in diff.DroppedExpressionIndexes)
                up.Add($"DROP INDEX {EscIndexName(table.Name, expressionIndex.Name)}");
            foreach (var (table, indexName) in diff.DroppedIndexes)
                up.Add($"DROP INDEX {EscIndexName(table.Name, indexName)}");

            // UP-2: Drop tables.
            foreach (var table in diff.DroppedTables)
                up.Add($"DROP TABLE {EscTable(table.Name)}");

            // UP-3: Drop columns (safe — FKs on those columns are removed in UP-1).
            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, pkCols))}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, column))}");
            }
            foreach (var (table, column) in diff.DroppedColumns)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // UP-3b: Rename columns BEFORE anything that references the new names —
            // altered-column statements and rebuilt indexes for a renamed column
            // reference the post-rename name, which must exist by then.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
                up.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(oldColName)} TO {Esc(newCol.Name)}");

            // UP-4: Alter existing columns.
            // PostgreSQL requires separate ALTER COLUMN statements for type and nullability changes.
            foreach (var (table, oldPkCols, _) in primaryKeyChanges.Where(static change => change.OldPrimaryKeyColumns.Length > 0))
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, oldPkCols))}");
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

                if (IsImplicitUniqueColumn(oldCol) && !IsImplicitUniqueColumn(newCol))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, oldCol))}");
                if (ColumnTypeChanged(oldCol, newCol))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(newCol.Name)} TYPE {GetSqlType(newCol)}{FormatCollation(newCol)} USING {Esc(newCol.Name)}::{GetCastType(newCol)}");
                if (oldCol.IsNullable != newCol.IsNullable)
                    up.Add(newCol.IsNullable
                        ? $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(newCol.Name)} DROP NOT NULL"
                        : $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(newCol.Name)} SET NOT NULL");

                // M1: Emit DEFAULT changes as separate SET DEFAULT / DROP DEFAULT statements.
                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                    up.Add(newCol.DefaultValue != null
                        ? $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(newCol.Name)} SET DEFAULT {DefaultValueValidator.Validate(newCol.DefaultValue)}"
                        : $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(newCol.Name)} DROP DEFAULT");
                if (!IsImplicitUniqueColumn(oldCol) && IsImplicitUniqueColumn(newCol))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, newCol)}");
                if (!string.Equals(oldCol.Comment, newCol.Comment, StringComparison.Ordinal))
                    up.Add(BuildColumnCommentSql(table, newCol, newCol.Comment));
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
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    var sqlType = c.IsIdentity ? GetIdentitySqlType(c) : GetSqlType(c);
                    var identityPart = c.IsIdentity ? " GENERATED ALWAYS AS IDENTITY" : "";
                    return $"{Esc(c.Name)} {sqlType}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{identityPart}{defaultPart}";
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
                    up.Add($"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatIncludedColumns(index.IncludedColumnNames)}{FormatNullsNotDistinct(index.NullsNotDistinct, index.IsUnique, index.IndexName)}{FormatFilter(index.FilterSql)}");
                }
                foreach (var expressionIndex in table.ExpressionIndexes)
                    up.Add(BuildExpressionIndexSql(table, expressionIndex));

                foreach (var c in table.Columns.Where(static c => !string.IsNullOrWhiteSpace(c.Comment)))
                    up.Add(BuildColumnCommentSql(table, c));
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
                // honour the model's default. (Sister fix in the SQLite/MySQL generators; the
                // SqlServer generator already applied its inline default clause unconditionally.)
                var nullPart = column.IsNullable
                    ? (!string.IsNullOrEmpty(column.DefaultValue)
                        ? $"NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
                        : "NULL")
                    : $"NOT NULL DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {nullPart}";
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD COLUMN {colDef}");
                if (!string.IsNullOrWhiteSpace(column.Comment))
                    up.Add(BuildColumnCommentSql(table, column));
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

            // ─ DOWN: reverse of UP, with symmetric FK ordering ──────────────────────

            // DOWN-1: Drop FK constraints that were added in UP-7 (before touching their columns).
            foreach (var (table, fk) in diff.AddedForeignKeys)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");
            foreach (var (table, check) in diff.AddedCheckConstraints)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(check.ConstraintName)}");
            foreach (var (table, expressionIndex) in diff.AddedExpressionIndexes)
                down.Add($"DROP INDEX {EscIndexName(table.Name, expressionIndex.Name)}");
            foreach (var (table, indexName, _, _, _) in diff.AddedIndexes)
                down.Add($"DROP INDEX {EscIndexName(table.Name, indexName)}");

            // DOWN-2: Drop columns that were added in UP-6.
            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, pkCols))}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, column))}");
            }
            foreach (var (table, column) in diff.AddedColumns)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // DOWN-3: Drop tables that were created in UP-5.
            foreach (var table in diff.AddedTables)
                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");

            // DOWN-3b: Rename columns back BEFORE anything that references the old
            // names — alteration reverts and restored indexes use pre-rename names.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
                down.Add($"ALTER TABLE {EscTable(table.Name)} RENAME COLUMN {Esc(newCol.Name)} TO {Esc(oldColName)}");

            // DOWN-4: Reverse column alterations from UP-4.
            foreach (var (table, _, newPkCols) in primaryKeyChanges.Where(static change => change.NewPrimaryKeyColumns.Length > 0))
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, newPkCols))}");
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
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, newCol))}");
                if (ColumnTypeChanged(oldCol, newCol))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} TYPE {GetSqlType(oldCol)}{FormatCollation(oldCol)} USING {Esc(oldCol.Name)}::{GetCastType(oldCol)}");
                if (oldCol.IsNullable != newCol.IsNullable)
                    down.Add(oldCol.IsNullable
                        ? $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} DROP NOT NULL"
                        : $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} SET NOT NULL");

                // M1: Down — restore old DEFAULT.
                if (!string.Equals(oldCol.DefaultValue, newCol.DefaultValue, StringComparison.Ordinal))
                    down.Add(oldCol.DefaultValue != null
                        ? $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} SET DEFAULT {DefaultValueValidator.Validate(oldCol.DefaultValue)}"
                        : $"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {Esc(oldCol.Name)} DROP DEFAULT");
                if (!IsImplicitUniqueColumn(newCol) && IsImplicitUniqueColumn(oldCol))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, oldCol)}");
                if (!string.Equals(oldCol.Comment, newCol.Comment, StringComparison.Ordinal))
                    down.Add(BuildColumnCommentSql(table, oldCol, oldCol.Comment));
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
                var restoreDefault = !string.IsNullOrEmpty(column.DefaultValue)
                    ? $" DEFAULT {DefaultValueValidator.Validate(column.DefaultValue)}"
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
                        ? $" DEFAULT {DefaultValueValidator.Validate(c.DefaultValue)}"
                        : "";
                    var sqlType = c.IsIdentity ? GetIdentitySqlType(c) : GetSqlType(c);
                    var identityPart = c.IsIdentity ? " GENERATED ALWAYS AS IDENTITY" : "";
                    return $"{Esc(c.Name)} {sqlType}{FormatCollation(c)} {(c.IsNullable ? "NULL" : "NOT NULL")}{identityPart}{defaultPart}";
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
                    down.Add($"CREATE {unique}INDEX {EscIndexName(table.Name, index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatIncludedColumns(index.IncludedColumnNames)}{FormatNullsNotDistinct(index.NullsNotDistinct, index.IsUnique, index.IndexName)}{FormatFilter(index.FilterSql)}");
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
        /// Returns the PostgreSQL base type name suitable for USING cast expressions.
        /// Strips precision/scale/size parameters from the type name, e.g. "DECIMAL(18,2)" → "DECIMAL",
        /// "CHAR(1)" → "CHAR", "NUMERIC(20,0)" → "NUMERIC". Types without parentheses are returned as-is.
        /// </summary>
        private static string GetCastType(ColumnSchema column)
        {
            var sqlType = GetSqlType(column);
            var idx = sqlType.IndexOf('(');
            return idx >= 0 ? sqlType[..idx].TrimEnd() : sqlType;
        }

        /// <summary>
        /// Maps a <see cref="ColumnSchema"/> instance to the appropriate PostgreSQL column
        /// type. When a CLR type is not explicitly mapped, <c>TEXT</c> is used as a safe
        /// default.
        /// </summary>
        /// <param name="column">The column metadata describing the desired CLR type.</param>
        /// <returns>The PostgreSQL data type name.</returns>
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

            if (TryGetBoundedStringType(column, out var boundedSql))
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

        private static bool TryGetBoundedStringType(ColumnSchema column, out string sql)
        {
            sql = string.Empty;
            if (column.MaxLength is > 0
                && string.Equals(column.ClrType, typeof(string).FullName, StringComparison.Ordinal))
            {
                var typeName = column.IsFixedLength ? "CHAR" : "VARCHAR";
                sql = $"{typeName}({column.MaxLength.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
                return true;
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

        private static bool ColumnTypeChanged(ColumnSchema oldCol, ColumnSchema newCol)
            => !string.Equals(oldCol.ClrType, newCol.ClrType, StringComparison.Ordinal)
            || !string.Equals(oldCol.StoreType, newCol.StoreType, StringComparison.OrdinalIgnoreCase)
            || oldCol.MaxLength != newCol.MaxLength
            || oldCol.IsUnicode != newCol.IsUnicode
            || oldCol.IsFixedLength != newCol.IsFixedLength
            || oldCol.Precision != newCol.Precision
            || oldCol.Scale != newCol.Scale
            || !string.Equals(oldCol.Collation, newCol.Collation, StringComparison.OrdinalIgnoreCase)
            || !string.Equals(oldCol.ComputedColumnSql, newCol.ComputedColumnSql, StringComparison.OrdinalIgnoreCase)
            || oldCol.IsStoredComputedColumn != newCol.IsStoredComputedColumn;

        private static bool IsComputedColumn(ColumnSchema column) => column.ComputedColumnSql is not null;

        private static string FormatCollation(ColumnSchema column)
            => string.IsNullOrWhiteSpace(column.Collation)
                ? string.Empty
                : $" COLLATE {FormatCollationIdentifier(column.Collation)}";

        private static string FormatCollationIdentifier(string collation)
        {
            var value = collation.Trim();
            if (value.Length == 0)
                throw new ArgumentException("Collation cannot be empty.", nameof(collation));

            var parts = value.Split('.');
            foreach (var part in parts)
            {
                if (part.Length == 0)
                    throw new ArgumentException($"Collation '{collation}' contains an empty identifier segment.");
                foreach (var ch in part)
                {
                    if (!char.IsLetterOrDigit(ch) && ch != '_' && ch != '-')
                        throw new ArgumentException($"Collation '{collation}' contains unsupported characters.");
                }
            }

            return string.Join(".", parts.Select(Esc));
        }

        private static string BuildComputedColumnDefinition(ColumnSchema column)
        {
            if (string.IsNullOrWhiteSpace(column.ComputedColumnSql))
                throw new NotSupportedException($"Computed column '{column.Name}' requires ComputedColumnSql for PostgreSQL migration generation.");
            return $"{Esc(column.Name)} {GetSqlType(column)} GENERATED ALWAYS AS ({FormatCheckPredicate(column.ComputedColumnSql)}) STORED";
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

        private static string FormatIndexColumns(string[] columnNames, bool[] descending, IndexNullSortOrder[] nullSortOrders)
            => string.Join(", ", columnNames.Select((name, index) =>
                Esc(name)
                + (index < descending.Length && descending[index] ? " DESC" : string.Empty)
                + FormatNullSortOrder(nullSortOrders, index)));

        private static string FormatNullSortOrder(IndexNullSortOrder[] nullSortOrders, int index)
        {
            if (index >= nullSortOrders.Length)
                return string.Empty;

            return nullSortOrders[index] switch
            {
                IndexNullSortOrder.Default => string.Empty,
                IndexNullSortOrder.First => " NULLS FIRST",
                IndexNullSortOrder.Last => " NULLS LAST",
                _ => throw new ArgumentOutOfRangeException(nameof(nullSortOrders), $"Unsupported null sort order '{nullSortOrders[index]}'.")
            };
        }

        private static string FormatNullSortOrder(IndexNullSortOrder nullSortOrder, string indexName)
            => nullSortOrder switch
            {
                IndexNullSortOrder.Default => string.Empty,
                IndexNullSortOrder.First => " NULLS FIRST",
                IndexNullSortOrder.Last => " NULLS LAST",
                _ => throw new ArgumentOutOfRangeException(nameof(nullSortOrder), $"Unsupported null sort order '{nullSortOrder}' in expression index '{indexName}'.")
            };

        private static string FormatIncludedColumns(string[] includedColumnNames)
            => includedColumnNames.Length == 0
                ? string.Empty
                : " INCLUDE (" + string.Join(", ", includedColumnNames.Select(Esc)) + ")";

        private static string FormatNullsNotDistinct(bool nullsNotDistinct, bool isUnique, string indexName)
        {
            if (!nullsNotDistinct)
                return string.Empty;
            if (!isUnique)
                throw new InvalidOperationException($"PostgreSQL index '{indexName}' cannot use NULLS NOT DISTINCT without being unique.");
            return " NULLS NOT DISTINCT";
        }

        private static string FormatFilter(string? filterSql)
            => string.IsNullOrWhiteSpace(filterSql) ? string.Empty : " WHERE " + filterSql.Trim();

        private static string BuildExpressionIndexSql(TableSchema table, ExpressionIndexSchema expressionIndex)
        {
            var unique = expressionIndex.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {EscIndexName(table.Name, expressionIndex.Name)} ON {EscTable(table.Name)} ({expressionIndex.ExpressionSql.Trim()}{FormatNullSortOrder(expressionIndex.NullSortOrder, expressionIndex.Name)}){FormatIncludedColumns(expressionIndex.IncludedColumnNames ?? Array.Empty<string>())}{FormatNullsNotDistinct(expressionIndex.NullsNotDistinct, expressionIndex.IsUnique, expressionIndex.Name)}{FormatFilter(expressionIndex.FilterSql)}";
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
