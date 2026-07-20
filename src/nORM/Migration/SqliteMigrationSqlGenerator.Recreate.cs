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
                    .Where(static item => RequiresRecreateForAddedColumn(item.Column)
                        // Restoring a NOT NULL column with no default via ADD COLUMN fails on
                        // populated tables ("Cannot add a NOT NULL column with default value
                        // NULL"), and adding a DEFAULT clause would drift the restored schema.
                        // Recreating instead backfills the type-appropriate zero in the
                        // INSERT ... SELECT while the column definition stays exact.
                        || (!item.Column.IsNullable
                            && string.IsNullOrEmpty(item.Column.DefaultValue)
                            && !IsComputedColumn(item.Column)))
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
            IReadOnlySet<string>? addedColumnNames = null,
            IReadOnlyDictionary<string, string>? sourceColumnByTarget = null,
            bool restoreFill = false)
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
                // A renamed column reads from its source name in the old table; addedColumnNames
                // reads a literal default because the column does not exist in the source at all.
                var sourceName = sourceColumnByTarget != null && sourceColumnByTarget.TryGetValue(col.Name, out var src)
                    ? src
                    : col.Name;
                selectExpressions.Add(addedColumnNames?.Contains(col.Name) == true
                    ? (restoreFill ? GetRestoredColumnInsertExpression(col) : GetAddedColumnInsertExpression(table, col))
                    : Esc(sourceName));
            }
            if (cols.Count > 0 && insertColumns.Count == 0)
                throw new NotSupportedException($"Cannot recreate table '{table.Name}' because all retained columns are computed/generated columns.");
            var recreatedTable = new TableSchema { Name = table.Name };
            foreach (var col in cols)
                recreatedTable.Columns.Add(col);

            // Render each column through the single BuildCreateColumnDefinition source of truth (same as
            // BuildCreateTableSql) so the recreate path never drifts from the create path — e.g. it must also
            // carry the inline column comment.
            var colDefs = cols.Select(BuildCreateColumnDefinition).ToList();

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

        private static bool NameEquals(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// The new-snapshot <see cref="TableSchema"/> for a table, carried by every diff category
        /// except <see cref="SchemaDiff.DroppedColumns"/> (which, per <see cref="SchemaDiffer"/>,
        /// references the OLD table). Returns <c>null</c> when a table is recreated ONLY because of
        /// dropped columns, in which case the new schema is reconstructed from the old table.
        /// </summary>
        private static TableSchema? TryResolveNewTable(SchemaDiff diff, string tableName)
        {
            foreach (var (t, _, _) in diff.AlteredColumns) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.AddedColumns) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.AddedForeignKeys) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.DroppedForeignKeys) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.AddedCheckConstraints) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.DroppedCheckConstraints) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _, _) in diff.RenamedColumns) if (NameEquals(t.Name, tableName)) return t;
            return null;
        }

        /// <summary>The old-snapshot <see cref="TableSchema"/>, available when the table has dropped columns.</summary>
        private static TableSchema? TryResolveOldTable(SchemaDiff diff, string tableName)
        {
            foreach (var (t, _) in diff.DroppedColumns) if (NameEquals(t.Name, tableName)) return t;
            return null;
        }

        private static TableSchema CopyTable(TableSchema baseTable, IEnumerable<ColumnSchema> cols,
            IEnumerable<ForeignKeySchema> fks, IEnumerable<CheckConstraintSchema> checks,
            IEnumerable<ExpressionIndexSchema> expressionIndexes)
        {
            // Carry the temporal metadata: the reconstructed schema feeds the versioning-trigger
            // re-emission, and a lost TenantColumnName would silently drop the tenant predicate
            // from the regenerated triggers' history-close condition.
            var t = new TableSchema
            {
                Name = baseTable.Name,
                IsTemporal = baseTable.IsTemporal,
                TenantColumnName = baseTable.TenantColumnName
            };
            t.Columns.AddRange(cols);
            t.ForeignKeys.AddRange(fks);
            t.CheckConstraints.AddRange(checks);
            t.ExpressionIndexes.AddRange(expressionIndexes);
            return t;
        }

        /// <summary>
        /// The base table instance for schema reconstruction: the new-snapshot table when available,
        /// otherwise the old-snapshot table (dropped-column-only recreation). All deltas are then
        /// applied explicitly so the result is correct regardless of which snapshot the base reflects
        /// (real diffs and manually-constructed test diffs both work).
        /// </summary>
        private static TableSchema ResolveBaseTable(SchemaDiff diff, string tableName)
            => TryResolveNewTable(diff, tableName)
               ?? TryResolveOldTable(diff, tableName)
               ?? throw new InvalidOperationException($"No schema source for recreated table '{tableName}'.");

        private static List<ForeignKeySchema> MergeForeignKeys(IEnumerable<ForeignKeySchema> baseFks,
            IEnumerable<string> removeNames, IEnumerable<ForeignKeySchema> add)
        {
            var remove = removeNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new List<ForeignKeySchema>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var fk in baseFks)
                if (!remove.Contains(fk.ConstraintName) && seen.Add(fk.ConstraintName))
                    result.Add(fk);
            foreach (var fk in add)
                if (!remove.Contains(fk.ConstraintName) && seen.Add(fk.ConstraintName))
                    result.Add(fk);
            return result;
        }

        private static List<CheckConstraintSchema> MergeChecks(IEnumerable<CheckConstraintSchema> baseChecks,
            IEnumerable<string> removeNames, IEnumerable<CheckConstraintSchema> add)
        {
            var remove = removeNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new List<CheckConstraintSchema>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var ck in baseChecks)
                if (!remove.Contains(ck.ConstraintName) && seen.Add(ck.ConstraintName))
                    result.Add(ck);
            foreach (var ck in add)
                if (!remove.Contains(ck.ConstraintName) && seen.Add(ck.ConstraintName))
                    result.Add(ck);
            return result;
        }

        /// <summary>
        /// Builds the complete post-migration (new) schema for a recreated table by applying every
        /// change to the base table: drop the dropped columns/FKs/checks, add the added ones, and force
        /// altered columns to their new definition. Deltas are applied idempotently so the result is
        /// correct whether the base already reflects them (real diffs) or not (manual test diffs).
        /// </summary>
        private static TableSchema BuildNewSchema(SchemaDiff diff, string tableName)
        {
            var baseTable = ResolveBaseTable(diff, tableName);
            var droppedNames = diff.DroppedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .Select(x => x.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var alteredNew = diff.AlteredColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(x => x.NewColumn.Name, x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
            var added = diff.AddedColumns.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.Column).ToList();

            var cols = new List<ColumnSchema>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in baseTable.Columns)
            {
                if (droppedNames.Contains(c.Name) || !seen.Add(c.Name))
                    continue;
                cols.Add(alteredNew.TryGetValue(c.Name, out var nc) ? nc : c);
            }
            foreach (var a in added)
                if (seen.Add(a.Name))
                    cols.Add(a);

            var fks = MergeForeignKeys(baseTable.ForeignKeys,
                diff.DroppedForeignKeys.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.ForeignKey.ConstraintName),
                diff.AddedForeignKeys.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.ForeignKey));
            var checks = MergeChecks(baseTable.CheckConstraints,
                diff.DroppedCheckConstraints.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.CheckConstraint.ConstraintName),
                diff.AddedCheckConstraints.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.CheckConstraint));

            return CopyTable(baseTable, cols, fks, checks, baseTable.ExpressionIndexes);
        }

        /// <summary>
        /// Builds the complete pre-migration (old) schema for a recreated table by reverting every
        /// change: altered columns to their old definition, added columns removed, dropped columns
        /// restored, renamed columns back to their old name, and the pre-migration FK/CHECK sets.
        /// Applied idempotently so it is correct for both real and manually-constructed diffs.
        /// </summary>
        private static TableSchema BuildOldSchema(SchemaDiff diff, string tableName)
        {
            var baseTable = ResolveBaseTable(diff, tableName);
            var alteredOld = diff.AlteredColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(x => x.NewColumn.Name, x => x.OldColumn, StringComparer.OrdinalIgnoreCase);
            var addedNames = diff.AddedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .Select(x => x.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var renamedOldByNew = diff.RenamedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(x => x.NewColumn.Name, x => x.OldColumnName, StringComparer.OrdinalIgnoreCase);
            var dropped = diff.DroppedColumns.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.Column).ToList();

            var cols = new List<ColumnSchema>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in baseTable.Columns)
            {
                if (addedNames.Contains(c.Name))
                    continue; // added by the migration; did not exist before
                ColumnSchema col;
                if (alteredOld.TryGetValue(c.Name, out var oldCol))
                    col = oldCol; // revert to the pre-alter definition
                else if (renamedOldByNew.TryGetValue(c.Name, out var oldName))
                {
                    col = c.Clone();
                    col.Name = oldName; // revert to the pre-rename name
                }
                else
                    col = c;
                if (seen.Add(col.Name))
                    cols.Add(col);
            }
            foreach (var d in dropped)
                if (seen.Add(d.Name))
                    cols.Add(d); // restore columns the migration dropped

            var fks = MergeForeignKeys(baseTable.ForeignKeys,
                diff.AddedForeignKeys.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.ForeignKey.ConstraintName),
                diff.DroppedForeignKeys.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.ForeignKey));
            var checks = MergeChecks(baseTable.CheckConstraints,
                diff.AddedCheckConstraints.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.CheckConstraint.ConstraintName),
                diff.DroppedCheckConstraints.Where(x => NameEquals(x.Table.Name, tableName)).Select(x => x.CheckConstraint));

            return CopyTable(baseTable, cols, fks, checks, Array.Empty<ExpressionIndexSchema>());
        }

        /// <summary>
        /// Emits a single Up table-recreation that applies every recreate-requiring change to a table
        /// at once (altered columns, added/dropped columns, added/dropped FKs and CHECK constraints).
        /// Doing it once â€” rather than once per change â€” prevents the per-change recreations from
        /// clobbering one another (most damagingly on the Down path).
        /// </summary>
        private static void EmitUpRecreate(List<string> up, SchemaDiff diff, string tableName)
        {
            var newSchema = BuildNewSchema(diff, tableName);

            // Columns the migration adds do not exist in the source table, so the INSERT ... SELECT
            // fills them with a literal default rather than reading a non-existent column.
            var addedColumnNames = diff.AddedColumns
                .Where(x => NameEquals(x.Table.Name, tableName))
                .Select(x => x.Column.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            // A renamed column is stored under its NEW name in the new table but read from its OLD name
            // in the pre-recreate source table.
            var sourceByTarget = diff.RenamedColumns
                .Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(x => x.NewColumn.Name, x => x.OldColumnName, StringComparer.OrdinalIgnoreCase);

            RecreateTable(
                up,
                newSchema,
                newSchema.Columns.ToList(),
                overrides: null,
                fks: newSchema.ForeignKeys,
                checks: newSchema.CheckConstraints,
                explicitIndexes: GetExplicitIndexesForUp(newSchema, diff),
                expressionIndexes: GetExpressionIndexesForUp(newSchema, diff),
                addedColumnNames: addedColumnNames.Count > 0 ? addedColumnNames : null,
                sourceColumnByTarget: sourceByTarget.Count > 0 ? sourceByTarget : null);
        }

        /// <summary>
        /// Emits a single Down table-recreation that reverts every recreate-requiring change to a
        /// table at once, restoring the complete pre-migration schema.
        /// </summary>
        private static void EmitDownRecreate(List<string> down, SchemaDiff diff, string tableName)
        {
            // The index resolvers derive the Down index set from the NEW schema baseline (they revert
            // added/dropped index changes), so both directions pass the new schema to them.
            var newSchema = BuildNewSchema(diff, tableName);
            var oldSchema = BuildOldSchema(diff, tableName);

            // Columns the migration dropped are absent from the current (post-Up) table, so fill them.
            var fillNames = diff.DroppedColumns
                .Where(x => NameEquals(x.Table.Name, tableName))
                .Select(x => x.Column.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            // The old-named target reads from the column's NEW name in the current table.
            var sourceByTarget = diff.RenamedColumns
                .Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(x => x.OldColumnName, x => x.NewColumn.Name, StringComparer.OrdinalIgnoreCase);

            RecreateTable(
                down,
                oldSchema,
                oldSchema.Columns.ToList(),
                overrides: null,
                fks: oldSchema.ForeignKeys,
                checks: oldSchema.CheckConstraints,
                explicitIndexes: GetExplicitIndexesForDown(newSchema, diff),
                expressionIndexes: GetExpressionIndexesForDown(newSchema, diff),
                addedColumnNames: fillNames.Count > 0 ? fillNames : null,
                sourceColumnByTarget: sourceByTarget.Count > 0 ? sourceByTarget : null,
                restoreFill: true);
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

        /// <summary>
        /// Insert expression for a column being RESTORED on the Down path after it was dropped. The
        /// original data is gone, so a NOT NULL column with no default falls back to a type-appropriate
        /// zero rather than throwing - otherwise the rollback would be unrunnable. Prefers the column's
        /// own default and uses NULL for nullable columns.
        /// </summary>
        private static string GetRestoredColumnInsertExpression(ColumnSchema column)
        {
            if (!string.IsNullOrEmpty(column.DefaultValue))
                return DefaultValueValidator.Validate(column.DefaultValue)!;
            if (column.IsNullable)
                return "NULL";
            return GetSqlType(column) switch
            {
                "TEXT" => "''",
                "BLOB" => "x''",
                _ => "0" // INTEGER, NUMERIC, REAL
            };
        }
    }
}
