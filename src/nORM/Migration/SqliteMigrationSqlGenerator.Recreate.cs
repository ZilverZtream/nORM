using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;

namespace nORM.Migration
{
    public partial class SqliteMigrationSqlGenerator
    {
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
            if (overrides != null)
                cols = cols.Select(c => overrides.TryGetValue(c.Name, out var ov) ? ov : c).ToList();

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

            var pkCols = cols.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0 && !pkCols.Any(c => c.IsIdentity))
                colDefs.Add(BuildPrimaryKeyConstraintSql(recreatedTable, pkCols));

            foreach (var uc in cols.Where(IsImplicitUniqueColumn))
                colDefs.Add(BuildUniqueConstraintSql(recreatedTable, uc));
            foreach (var fk in fks ?? table.ForeignKeys)
                colDefs.Add(BuildFkConstraintSql(fk));
            foreach (var check in checks ?? table.CheckConstraints)
                colDefs.Add(BuildCheckConstraintSql(check));

            var tempName = EscTempTable(table.Name);
            var renameTarget = EscTableNameOnly(table.Name);
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
    }
}
