using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using RenameColumnAttr = nORM.Mapping.RenameColumnAttribute;

namespace nORM.Migration
{
    /// <summary>
    /// Represents the differences between two <see cref="SchemaSnapshot"/> instances.
    /// </summary>
    /// <remarks>
    /// All list properties on this class are initialized to non-null empty lists and must
    /// remain non-null throughout their lifetime. Individual entries within each list must
    /// also be non-null; adding a null entry will cause downstream migration SQL generators
    /// to throw a <see cref="NullReferenceException"/>. The lists themselves must never be
    /// set to null - <see cref="HasChanges"/> and all four SQL generators assume non-null lists.
    /// </remarks>
    public class SchemaDiff
    {
        /// <summary>Tables that exist in the new snapshot but not in the old.</summary>
        public List<TableSchema> AddedTables { get; } = new();
        /// <summary>Columns that exist in the new snapshot but not in the old.</summary>
        public List<(TableSchema Table, ColumnSchema Column)> AddedColumns { get; } = new();
        /// <summary>Columns whose definition has changed between snapshots.</summary>
        public List<(TableSchema Table, ColumnSchema NewColumn, ColumnSchema OldColumn)> AlteredColumns { get; } = new();
        /// <summary>Indexes that appear in the new snapshot but not in the old.</summary>
        public List<(TableSchema Table, string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending)> AddedIndexes { get; } = new();
        /// <summary>Indexes that appear in the old snapshot but not in the new.</summary>
        public List<(TableSchema Table, string IndexName)> DroppedIndexes { get; } = new();
        /// <summary>Tables that exist in the old snapshot but not in the new (dropped tables).</summary>
        public List<TableSchema> DroppedTables { get; } = new();
        /// <summary>Columns that exist in the old snapshot but not in the new for a given table (dropped columns).</summary>
        public List<(TableSchema Table, ColumnSchema Column)> DroppedColumns { get; } = new();
        /// <summary>FK constraints present in the new snapshot but not the old for an existing table.</summary>
        public List<(TableSchema Table, ForeignKeySchema ForeignKey)> AddedForeignKeys { get; } = new();
        /// <summary>FK constraints present in the old snapshot but not the new for an existing table.</summary>
        public List<(TableSchema Table, ForeignKeySchema ForeignKey)> DroppedForeignKeys { get; } = new();
        /// <summary>CHECK constraints present in the new snapshot but not the old for an existing table.</summary>
        public List<(TableSchema Table, CheckConstraintSchema CheckConstraint)> AddedCheckConstraints { get; } = new();
        /// <summary>CHECK constraints present in the old snapshot but not the new for an existing table.</summary>
        public List<(TableSchema Table, CheckConstraintSchema CheckConstraint)> DroppedCheckConstraints { get; } = new();
        /// <summary>Expression indexes present in the new snapshot but not the old for an existing table.</summary>
        public List<(TableSchema Table, ExpressionIndexSchema ExpressionIndex)> AddedExpressionIndexes { get; } = new();
        /// <summary>Expression indexes present in the old snapshot but not the new for an existing table.</summary>
        public List<(TableSchema Table, ExpressionIndexSchema ExpressionIndex)> DroppedExpressionIndexes { get; } = new();
        /// <summary>
        /// Column renames detected via <c>[RenameColumn("oldName")]</c>. Each entry carries the table,
        /// the old column name, and the new <see cref="ColumnSchema"/> (with its new name). The differ
        /// populates this list instead of adding to <see cref="DroppedColumns"/> / <see cref="AddedColumns"/>.
        /// </summary>
        public List<(TableSchema Table, string OldColumnName, ColumnSchema NewColumn)> RenamedColumns { get; } = new();

        /// <summary>Indicates whether the diff contains any schema changes.</summary>
        public bool HasChanges => AddedTables.Count > 0 || AddedColumns.Count > 0 || AlteredColumns.Count > 0
            || AddedIndexes.Count > 0 || DroppedIndexes.Count > 0
            || DroppedTables.Count > 0 || DroppedColumns.Count > 0
            || AddedForeignKeys.Count > 0 || DroppedForeignKeys.Count > 0
            || AddedCheckConstraints.Count > 0 || DroppedCheckConstraints.Count > 0
            || AddedExpressionIndexes.Count > 0 || DroppedExpressionIndexes.Count > 0
            || RenamedColumns.Count > 0;

        /// <summary>
        /// Indicates whether the diff contains operations that can remove data or weaken
        /// schema-enforced integrity.
        /// </summary>
        public bool HasDestructiveChanges => GetDestructiveChangeWarnings().Count > 0;

        /// <summary>
        /// Returns human-readable warnings for destructive operations in this diff.
        /// </summary>
        public IReadOnlyList<string> GetDestructiveChangeWarnings()
        {
            var warnings = new List<string>(
                DroppedTables.Count
                + DroppedColumns.Count
                + DroppedForeignKeys.Count
                + DroppedCheckConstraints.Count
                + DroppedIndexes.Count
                + DroppedExpressionIndexes.Count
                + AlteredColumns.Count);
            foreach (var table in DroppedTables)
            {
                warnings.Add($"Drop table '{table.Name}' will remove all data in that table. If this is a rename, replace the generated drop/create with a provider-specific rename operation.");
            }

            foreach (var (table, column) in DroppedColumns)
            {
                var candidate = AddedColumns
                    .Where(x => string.Equals(x.Table.Name, table.Name, StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Column)
                    .FirstOrDefault(x =>
                        string.Equals(x.ClrType, column.ClrType, StringComparison.OrdinalIgnoreCase) &&
                        x.MaxLength == column.MaxLength &&
                        x.IsUnicode == column.IsUnicode &&
                        x.IsFixedLength == column.IsFixedLength &&
                        x.Precision == column.Precision &&
                        x.Scale == column.Scale &&
                        x.IsNullable == column.IsNullable);

                var message = $"Drop column '{table.Name}.{column.Name}' will remove data in that column.";
                if (candidate != null)
                    message += $" Possible rename candidate: '{table.Name}.{candidate.Name}'.";
                message += " If this is a rename, replace the generated drop/add with a provider-specific rename operation.";
                warnings.Add(message);
            }

            foreach (var (table, fk) in DroppedForeignKeys)
            {
                warnings.Add($"Drop foreign key '{fk.ConstraintName}' on '{table.Name}' will stop enforcing referential integrity to '{fk.PrincipalTable}'.");
            }

            foreach (var (table, check) in DroppedCheckConstraints)
            {
                warnings.Add($"Drop check constraint '{check.ConstraintName}' on '{table.Name}' will stop enforcing its predicate.");
            }

            foreach (var (table, indexName) in DroppedIndexes)
            {
                var index = SchemaDiffer.GetExplicitIndexes(table)
                    .FirstOrDefault(index => string.Equals(index.IndexName, indexName, StringComparison.OrdinalIgnoreCase));
                if (index.IsUnique)
                {
                    var columns = index.ColumnNames.Length == 0
                        ? "<unknown>"
                        : string.Join(", ", index.ColumnNames);
                    warnings.Add($"Drop unique index '{indexName}' on '{table.Name}' will stop enforcing uniqueness for '{columns}'.");
                }
            }

            foreach (var (table, expressionIndex) in DroppedExpressionIndexes)
            {
                if (expressionIndex.IsUnique)
                    warnings.Add($"Drop unique expression index '{expressionIndex.Name}' on '{table.Name}' will stop enforcing uniqueness for expression '{expressionIndex.ExpressionSql}'.");
            }

            foreach (var (table, newColumn, oldColumn) in AlteredColumns)
            {
                AddDestructiveColumnAlterWarnings(warnings, table, oldColumn, newColumn);
            }

            if (warnings.Count == 0)
                return Array.Empty<string>();

            return warnings;
        }

        private static void AddDestructiveColumnAlterWarnings(
            List<string> warnings,
            TableSchema table,
            ColumnSchema oldColumn,
            ColumnSchema newColumn)
        {
            var column = $"{table.Name}.{newColumn.Name}";
            if (!string.Equals(oldColumn.ClrType, newColumn.ClrType, StringComparison.OrdinalIgnoreCase))
            {
                warnings.Add($"Alter column '{column}' changes type from '{oldColumn.ClrType}' to '{newColumn.ClrType}' and may truncate or fail to convert existing data.");
            }

            if (oldColumn.IsNullable && !newColumn.IsNullable)
            {
                warnings.Add($"Alter column '{column}' changes nullability from nullable to required and may fail if existing rows contain NULL.");
            }

            if (NarrowsPrecisionOrScale(oldColumn, newColumn))
            {
                warnings.Add($"Alter column '{column}' narrows precision/scale from '{FormatPrecisionScale(oldColumn)}' to '{FormatPrecisionScale(newColumn)}' and may truncate existing values.");
            }

            if (NarrowsMaxLength(oldColumn, newColumn))
            {
                warnings.Add($"Alter column '{column}' narrows max length from '{FormatMaxLength(oldColumn)}' to '{FormatMaxLength(newColumn)}' and may truncate existing values.");
            }

            if (oldColumn.IsPrimaryKey && !newColumn.IsPrimaryKey)
            {
                warnings.Add($"Alter column '{column}' drops primary key membership and will stop enforcing entity identity.");
            }

            if (oldColumn.IsUnique && !newColumn.IsUnique)
            {
                warnings.Add($"Alter column '{column}' drops uniqueness and will allow duplicate values.");
            }

            if (oldColumn.IsIdentity != newColumn.IsIdentity
                || oldColumn.IdentitySeed != newColumn.IdentitySeed
                || oldColumn.IdentityIncrement != newColumn.IdentityIncrement)
            {
                warnings.Add($"Alter column '{column}' changes identity metadata; review generated DDL before applying it.");
            }

            if ((oldColumn.ComputedColumnSql is not null || newColumn.ComputedColumnSql is not null)
                && (!string.Equals(oldColumn.ComputedColumnSql, newColumn.ComputedColumnSql, StringComparison.OrdinalIgnoreCase)
                    || oldColumn.IsStoredComputedColumn != newColumn.IsStoredComputedColumn))
            {
                warnings.Add($"Alter column '{column}' changes computed column SQL; stored computed values may be rebuilt.");
            }

            // A Unicode -> non-Unicode change (e.g. NVARCHAR -> VARCHAR) is a lossy code-page conversion:
            // on SQL Server, characters not representable in the target code page are silently replaced
            // with '?', with no length overflow and no truncation error to signal it. The prior classifier
            // ignored this, so such an alter shipped without the --force gate.
            if (newColumn.IsUnicode == false && oldColumn.IsUnicode != false)
            {
                warnings.Add($"Alter column '{column}' changes from Unicode to non-Unicode storage and may silently replace characters that are not representable in the target code page.");
            }

            if (oldColumn.IsFixedLength != newColumn.IsFixedLength)
            {
                warnings.Add($"Alter column '{column}' changes fixed-length storage from '{oldColumn.IsFixedLength}' to '{newColumn.IsFixedLength}' and may pad or truncate existing values.");
            }

            // An explicit store-type (HasColumnType) change is an opaque raw override the schema model
            // cannot prove is widening; a narrowing such as nvarchar(200) -> varchar(50) or decimal(18,4)
            // -> decimal(5,2) truncates, clamps, or converts existing data lossily. Warn so it requires
            // an explicit review/--force rather than shipping silently.
            if (!string.Equals(oldColumn.StoreType, newColumn.StoreType, StringComparison.OrdinalIgnoreCase))
            {
                warnings.Add($"Alter column '{column}' changes the explicit store type from '{oldColumn.StoreType ?? "(default)"}' to '{newColumn.StoreType ?? "(default)"}' and may truncate or convert existing data lossily.");
            }
        }

        private static bool NarrowsPrecisionOrScale(ColumnSchema oldColumn, ColumnSchema newColumn)
        {
            if (oldColumn.Precision == newColumn.Precision && oldColumn.Scale == newColumn.Scale)
                return false;

            if (newColumn.Precision is null)
                return false;

            if (oldColumn.Precision is null)
                return true;

            var oldScale = oldColumn.Scale ?? 0;
            var newScale = newColumn.Scale ?? 0;
            var oldIntegralDigits = oldColumn.Precision.Value - oldScale;
            var newIntegralDigits = newColumn.Precision.Value - newScale;
            return newColumn.Precision.Value < oldColumn.Precision.Value
                || newScale < oldScale
                || newIntegralDigits < oldIntegralDigits;
        }

        private static string FormatPrecisionScale(ColumnSchema column)
            => column.Precision is null
                ? "unbounded"
                : column.Scale is null
                    ? column.Precision.Value.ToString(CultureInfo.InvariantCulture)
                    : string.Create(
                        CultureInfo.InvariantCulture,
                        $"{column.Precision.Value},{column.Scale.Value}");

        private static bool NarrowsMaxLength(ColumnSchema oldColumn, ColumnSchema newColumn)
        {
            if (oldColumn.MaxLength == newColumn.MaxLength || newColumn.MaxLength is null)
                return false;

            return oldColumn.MaxLength is null || newColumn.MaxLength.Value < oldColumn.MaxLength.Value;
        }

        private static string FormatMaxLength(ColumnSchema column)
            => column.MaxLength?.ToString(CultureInfo.InvariantCulture) ?? "unbounded";
    }
}
