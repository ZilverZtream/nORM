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
    /// Provides methods for computing differences between two schema snapshots.
    /// </summary>
    public static class SchemaDiffer
    {
        /// <summary>
        /// Computes the difference between two schema snapshots.
        /// </summary>
        /// <param name="oldSnapshot">The snapshot representing the current database schema.</param>
        /// <param name="newSnapshot">The snapshot representing the desired schema.</param>
        /// <returns>A <see cref="SchemaDiff"/> describing the operations required to transform the schema.</returns>
        public static SchemaDiff Diff(SchemaSnapshot oldSnapshot, SchemaSnapshot newSnapshot)
        {
            ArgumentNullException.ThrowIfNull(oldSnapshot);
            ArgumentNullException.ThrowIfNull(newSnapshot);

            if (oldSnapshot.Tables is null)
                throw new ArgumentException("oldSnapshot.Tables must not be null.", nameof(oldSnapshot));
            if (newSnapshot.Tables is null)
                throw new ArgumentException("newSnapshot.Tables must not be null.", nameof(newSnapshot));

            // Validate constraint metadata on both snapshots before starting the diff.
            foreach (var t in oldSnapshot.Tables)
            {
                foreach (var fk in t.ForeignKeys)
                    ValidateFkSchema(fk, t.Name);
                foreach (var check in t.CheckConstraints)
                    ValidateCheckConstraintSchema(check, t.Name);
                foreach (var expressionIndex in t.ExpressionIndexes)
                    ValidateExpressionIndexSchema(expressionIndex, t.Name);
            }
            foreach (var t in newSnapshot.Tables)
            {
                foreach (var fk in t.ForeignKeys)
                    ValidateFkSchema(fk, t.Name);
                foreach (var check in t.CheckConstraints)
                    ValidateCheckConstraintSchema(check, t.Name);
                foreach (var expressionIndex in t.ExpressionIndexes)
                    ValidateExpressionIndexSchema(expressionIndex, t.Name);
            }

            // Build O(1) lookup dictionaries to avoid O(n^2) scans inside the loops.
            // Use last-wins for duplicate table names to match the pre-existing FirstOrDefault behaviour.
            var oldByName = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in oldSnapshot.Tables) oldByName[t.Name] = t;
            var newByName = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in newSnapshot.Tables) newByName[t.Name] = t;

            var diff = new SchemaDiff();
            foreach (var newTable in newSnapshot.Tables)
            {
                if (!oldByName.TryGetValue(newTable.Name, out var oldTable))
                {
                    diff.AddedTables.Add(newTable);
                    continue;
                }

                // Build O(1) column lookup for the old table.
                var oldColByName = oldTable.Columns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

                // Collect the set of old column names that are consumed by a [RenameColumn] declaration
                // so that the dropped-column loop skips them (they are not truly dropped).
                var renamedOldNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var col in newTable.Columns)
                {
                    // Check for [RenameColumn("OldName")] - prefer rename over drop+add when the
                    // referenced old column exists in the old snapshot.
                    if (col.PreviousName != null && oldColByName.ContainsKey(col.PreviousName))
                    {
                        diff.RenamedColumns.Add((newTable, col.PreviousName, col));
                        renamedOldNames.Add(col.PreviousName);
                        // The new column name is now "in use"; skip add/alter detection for it.
                        continue;
                    }

                    if (!oldColByName.TryGetValue(col.Name, out var oldCol))
                        diff.AddedColumns.Add((newTable, col));
                    else if (!string.Equals(oldCol.ClrType, col.ClrType, StringComparison.OrdinalIgnoreCase)
                        || !string.Equals(oldCol.StoreType, col.StoreType, StringComparison.OrdinalIgnoreCase)  // HasColumnType: an explicit store-type change must re-emit the column type (ALTER/MODIFY already renders GetSqlType, which honors StoreType)
                        || oldCol.MaxLength != col.MaxLength
                        || oldCol.IsUnicode != col.IsUnicode
                        || oldCol.IsFixedLength != col.IsFixedLength
                        || oldCol.Precision != col.Precision
                        || oldCol.Scale != col.Scale
                        || oldCol.IsNullable != col.IsNullable
                        || oldCol.IsPrimaryKey != col.IsPrimaryKey
                        || oldCol.IsUnique != col.IsUnique
                        || !string.Equals(oldCol.IndexName, col.IndexName, StringComparison.OrdinalIgnoreCase)
                        || !string.Equals(oldCol.DefaultValue, col.DefaultValue, StringComparison.OrdinalIgnoreCase)  // OrdinalIgnoreCase: SQL keyword case differences like CURRENT_TIMESTAMP vs current_timestamp must not trigger spurious migrations
                        || !string.Equals(oldCol.DefaultConstraintName, col.DefaultConstraintName, StringComparison.OrdinalIgnoreCase)
                        || !string.Equals(oldCol.Collation, col.Collation, StringComparison.OrdinalIgnoreCase)
                        || oldCol.IsIdentity != col.IsIdentity
                        || oldCol.IdentitySeed != col.IdentitySeed
                        || oldCol.IdentityIncrement != col.IdentityIncrement
                        || !string.Equals(oldCol.ComputedColumnSql, col.ComputedColumnSql, StringComparison.OrdinalIgnoreCase)
                        || oldCol.IsStoredComputedColumn != col.IsStoredComputedColumn)
                        diff.AlteredColumns.Add((newTable, col, oldCol));
                }

                // Build O(1) new-column lookup for dropped-column detection.
                var newColByName = newTable.Columns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

                // Detect dropped columns - columns present in old but not in new.
                // Use oldTable (not newTable) because the column existed in the OLD schema.
                // Skip old columns that were renamed via [RenameColumn] (they are not truly dropped).
                foreach (var oldCol in oldTable.Columns)
                {
                    if (!newColByName.ContainsKey(oldCol.Name) && !renamedOldNames.Contains(oldCol.Name))
                        diff.DroppedColumns.Add((oldTable, oldCol));
                }

                // Detect index changes - compare named indexes between old and new
                var oldIndexes = BuildIndexMap(oldTable);
                var newIndexes = BuildIndexMap(newTable);

                foreach (var (name, (isUnique, cols, descending, nullSortOrders, included, nullsNotDistinct, filterSql)) in newIndexes)
                {
                    if (!oldIndexes.ContainsKey(name))
                    {
                        if (!IsSyntheticIndexKey(name))
                            diff.AddedIndexes.Add((newTable, name, isUnique, cols, descending));
                    }
                    else
                    {
                        // Index exists in both - check for definition changes
                        var (oldIsUnique, oldCols, oldDescending, oldNullSortOrders, oldIncluded, oldNullsNotDistinct, oldFilterSql) = oldIndexes[name];
                        // Compare column lists in declared order; (A,B) and (B,A) are semantically distinct indexes.
                        var colsChanged = !oldCols.SequenceEqual(cols, StringComparer.OrdinalIgnoreCase);
                        var directionChanged = !oldDescending.SequenceEqual(descending);
                        var nullSortOrderChanged = !oldNullSortOrders.SequenceEqual(nullSortOrders);
                        var includedChanged = !oldIncluded.SequenceEqual(included, StringComparer.OrdinalIgnoreCase);
                        var filterChanged = !string.Equals(oldFilterSql, filterSql, StringComparison.OrdinalIgnoreCase);
                        if (oldIsUnique != isUnique || colsChanged || directionChanged || nullSortOrderChanged || includedChanged || oldNullsNotDistinct != nullsNotDistinct || filterChanged)
                        {
                            if (!IsSyntheticIndexKey(name))
                            {
                                // Use oldTable for DroppedIndexes (the index existed on the OLD table).
                                diff.DroppedIndexes.Add((oldTable, name));
                                diff.AddedIndexes.Add((newTable, name, isUnique, cols, descending));
                            }
                        }
                    }
                }

                foreach (var (name, _) in oldIndexes)
                {
                    if (!newIndexes.ContainsKey(name) && !IsSyntheticIndexKey(name))
                        // Use oldTable for DroppedIndexes (the index existed on the OLD table).
                        diff.DroppedIndexes.Add((oldTable, name));
                }

                // Detect FK constraint changes - add/drop/alter foreign keys
                var oldFks = BuildFkMap(oldTable);
                var newFks = BuildFkMap(newTable);

                foreach (var (name, newFk) in newFks)
                {
                    if (!oldFks.TryGetValue(name, out var oldFk))
                    {
                        diff.AddedForeignKeys.Add((newTable, newFk));
                    }
                    else if (!FkEqual(oldFk, newFk))
                    {
                        // Definition changed: drop old, add new
                        diff.DroppedForeignKeys.Add((newTable, oldFk));
                        diff.AddedForeignKeys.Add((newTable, newFk));
                    }
                }
                foreach (var (name, oldFk) in oldFks)
                {
                    if (!newFks.ContainsKey(name))
                        diff.DroppedForeignKeys.Add((newTable, oldFk));
                }

                // Detect CHECK constraint changes by name and normalized SQL predicate.
                var oldChecks = BuildCheckMap(oldTable);
                var newChecks = BuildCheckMap(newTable);

                foreach (var (name, newCheck) in newChecks)
                {
                    if (!oldChecks.TryGetValue(name, out var oldCheck))
                    {
                        diff.AddedCheckConstraints.Add((newTable, newCheck));
                    }
                    else if (!CheckEqual(oldCheck, newCheck))
                    {
                        diff.DroppedCheckConstraints.Add((newTable, oldCheck));
                        diff.AddedCheckConstraints.Add((newTable, newCheck));
                    }
                }

                foreach (var (name, oldCheck) in oldChecks)
                {
                    if (!newChecks.ContainsKey(name))
                        diff.DroppedCheckConstraints.Add((newTable, oldCheck));
                }

                var oldExpressionIndexes = BuildExpressionIndexMap(oldTable);
                var newExpressionIndexes = BuildExpressionIndexMap(newTable);
                foreach (var (name, newExpressionIndex) in newExpressionIndexes)
                {
                    if (!oldExpressionIndexes.TryGetValue(name, out var oldExpressionIndex))
                    {
                        diff.AddedExpressionIndexes.Add((newTable, newExpressionIndex));
                    }
                    else if (!ExpressionIndexEqual(oldExpressionIndex, newExpressionIndex))
                    {
                        diff.DroppedExpressionIndexes.Add((newTable, oldExpressionIndex));
                        diff.AddedExpressionIndexes.Add((newTable, newExpressionIndex));
                    }
                }
                foreach (var (name, oldExpressionIndex) in oldExpressionIndexes)
                {
                    if (!newExpressionIndexes.ContainsKey(name))
                        diff.DroppedExpressionIndexes.Add((oldTable, oldExpressionIndex));
                }
            }

            // Detect dropped tables - tables present in old but not in new.
            // Use the pre-built newByName dictionary for O(1) lookup.
            foreach (var oldTable in oldSnapshot.Tables)
            {
                if (!newByName.ContainsKey(oldTable.Name))
                    diff.DroppedTables.Add(oldTable);
            }

            return diff;
        }

        /// <summary>
        /// Validates that a <see cref="ForeignKeySchema"/>'s column arrays are non-empty and
        /// that the principal table name is non-blank. Throws <see cref="ArgumentException"/> on violation.
        /// </summary>
        private static void ValidateFkSchema(ForeignKeySchema fk, string owningTableName)
        {
            if (fk.DependentColumns == null || fk.DependentColumns.Length == 0)
                throw new ArgumentException(
                    $"FK '{fk.ConstraintName}' on table '{owningTableName}' has no DependentColumns.");
            if (fk.PrincipalColumns == null || fk.PrincipalColumns.Length == 0)
                throw new ArgumentException(
                    $"FK '{fk.ConstraintName}' on table '{owningTableName}' has no PrincipalColumns.");
            if (string.IsNullOrWhiteSpace(fk.PrincipalTable))
                throw new ArgumentException(
                    $"FK '{fk.ConstraintName}' on table '{owningTableName}' has no PrincipalTable.");
        }

        private static void ValidateCheckConstraintSchema(CheckConstraintSchema check, string owningTableName)
        {
            if (string.IsNullOrWhiteSpace(check.ConstraintName))
                throw new ArgumentException($"CHECK constraint on table '{owningTableName}' has no ConstraintName.");
            if (string.IsNullOrWhiteSpace(check.Sql))
                throw new ArgumentException($"CHECK constraint '{check.ConstraintName}' on table '{owningTableName}' has no SQL predicate.");
        }

        private static void ValidateExpressionIndexSchema(ExpressionIndexSchema expressionIndex, string owningTableName)
        {
            if (string.IsNullOrWhiteSpace(expressionIndex.Name))
                throw new ArgumentException($"Expression index on table '{owningTableName}' has no Name.");
            if (string.IsNullOrWhiteSpace(expressionIndex.ExpressionSql))
                throw new ArgumentException($"Expression index '{expressionIndex.Name}' on table '{owningTableName}' has no SQL expression.");
            if (expressionIndex.IncludedColumnNames is null)
                throw new ArgumentException($"Expression index '{expressionIndex.Name}' on table '{owningTableName}' has null IncludedColumnNames.");
            if (expressionIndex.IncludedColumnNames.Any(string.IsNullOrWhiteSpace))
                throw new ArgumentException($"Expression index '{expressionIndex.Name}' on table '{owningTableName}' has a null or whitespace included column name.");
            if (expressionIndex.IncludedColumnNames.Distinct(StringComparer.OrdinalIgnoreCase).Count() != expressionIndex.IncludedColumnNames.Length)
                throw new ArgumentException($"Expression index '{expressionIndex.Name}' on table '{owningTableName}' has duplicate included column names.");
            if (!Enum.IsDefined(expressionIndex.NullSortOrder))
                throw new ArgumentOutOfRangeException(nameof(expressionIndex.NullSortOrder), $"Unsupported null sort order '{expressionIndex.NullSortOrder}' in expression index '{expressionIndex.Name}'.");
        }

        /// <summary>
        /// Builds a name-keyed map of FK constraints for a table.
        /// Duplicate constraint names are detected and cause an <see cref="InvalidOperationException"/>.
        /// </summary>
        private static Dictionary<string, ForeignKeySchema> BuildFkMap(TableSchema table)
        {
            var map = new Dictionary<string, ForeignKeySchema>(table.ForeignKeys.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var fk in table.ForeignKeys)
            {
                if (!map.TryAdd(fk.ConstraintName, fk))
                    throw new InvalidOperationException(
                        $"Duplicate FK constraint name '{fk.ConstraintName}' on table '{table.Name}'.");
            }
            return map;
        }

        /// <summary>
        /// Returns true when two FK schemas represent the same constraint definition.
        /// Column order within each array is significant.
        /// </summary>
        /// <remarks>
        /// <see cref="ForeignKeySchema.ConstraintName"/> is intentionally NOT compared inside this method.
        /// FK rename detection is handled by the key-based lookup in <see cref="BuildFkMap"/>:
        /// that method indexes FKs by <c>ConstraintName</c>, so a pure FK rename (same
        /// columns/tables/actions, different name) produces two distinct dictionary entries -
        /// the old name is "not in new" (DroppedForeignKeys) and the new name is "not in old"
        /// (AddedForeignKeys). <c>FkEqual</c> is only called when the <em>same</em>
        /// <c>ConstraintName</c> key is found in both maps, so the names are equal by definition.
        /// </remarks>
        private static bool FkEqual(ForeignKeySchema a, ForeignKeySchema b) =>
            string.Equals(a.PrincipalTable, b.PrincipalTable, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(a.OnDelete, b.OnDelete, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(a.OnUpdate, b.OnUpdate, StringComparison.OrdinalIgnoreCase) &&
            a.DependentColumns.Length == b.DependentColumns.Length &&
            a.DependentColumns.Zip(b.DependentColumns, (x, y) =>
                string.Equals(x, y, StringComparison.OrdinalIgnoreCase)).All(eq => eq) &&
            a.PrincipalColumns.Length == b.PrincipalColumns.Length &&
            a.PrincipalColumns.Zip(b.PrincipalColumns, (x, y) =>
                string.Equals(x, y, StringComparison.OrdinalIgnoreCase)).All(eq => eq);

        private static bool CheckEqual(CheckConstraintSchema a, CheckConstraintSchema b) =>
            string.Equals(NormalizeCheckSql(a.Sql), NormalizeCheckSql(b.Sql), StringComparison.OrdinalIgnoreCase);

        private static string NormalizeCheckSql(string sql)
            => string.Join(" ", sql.Trim().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        private static bool ExpressionIndexEqual(ExpressionIndexSchema a, ExpressionIndexSchema b) =>
            a.IsUnique == b.IsUnique
            && a.NullsNotDistinct == b.NullsNotDistinct
            && a.NullSortOrder == b.NullSortOrder
            && a.IncludedColumnNames.SequenceEqual(b.IncludedColumnNames, StringComparer.OrdinalIgnoreCase)
            && string.Equals(NormalizeCheckSql(a.ExpressionSql), NormalizeCheckSql(b.ExpressionSql), StringComparison.OrdinalIgnoreCase)
            && string.Equals(NormalizeCheckSql(a.FilterSql ?? string.Empty), NormalizeCheckSql(b.FilterSql ?? string.Empty), StringComparison.OrdinalIgnoreCase);

        internal static IReadOnlyList<(string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> GetExplicitIndexes(TableSchema table)
            => BuildIndexMap(table)
                .Where(static index => !IsSyntheticIndexKey(index.Key))
                .Select(static index => (index.Key, index.Value.IsUnique, index.Value.ColumnNames, index.Value.Descending, index.Value.NullSortOrders, index.Value.IncludedColumnNames, index.Value.NullsNotDistinct, index.Value.FilterSql))
                .ToArray();

        internal static IReadOnlyList<(TableSchema Table, ColumnSchema[] OldPrimaryKeyColumns, ColumnSchema[] NewPrimaryKeyColumns)> GetPrimaryKeyChanges(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);

            var changes = new List<(TableSchema Table, ColumnSchema[] OldPrimaryKeyColumns, ColumnSchema[] NewPrimaryKeyColumns)>();
            foreach (var group in diff.AlteredColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var oldByName = group.ToDictionary(static item => item.NewColumn.Name, static item => item.OldColumn, StringComparer.OrdinalIgnoreCase);
                var oldColumns = table.Columns
                    .Select(column => oldByName.TryGetValue(column.Name, out var oldColumn) ? oldColumn : column)
                    .ToArray();
                var oldPk = oldColumns.Where(static column => column.IsPrimaryKey).ToArray();
                var newPk = table.Columns.Where(static column => column.IsPrimaryKey).ToArray();
                if (PrimaryKeyDefinitionChanged(table.Name, oldPk, newPk))
                    changes.Add((table, oldPk, newPk));
            }

            return changes;
        }

        private static bool PrimaryKeyDefinitionChanged(string tableName, ColumnSchema[] oldPk, ColumnSchema[] newPk)
            => oldPk.Length != newPk.Length
               || !oldPk.Select(static column => column.Name).SequenceEqual(newPk.Select(static column => column.Name), StringComparer.OrdinalIgnoreCase)
               || !string.Equals(GetPrimaryKeyConstraintName(tableName, oldPk), GetPrimaryKeyConstraintName(tableName, newPk), StringComparison.OrdinalIgnoreCase);

        private static string GetPrimaryKeyConstraintName(string tableName, IReadOnlyList<ColumnSchema> pkCols)
            => pkCols.FirstOrDefault(static column => !string.IsNullOrWhiteSpace(column.IndexName))?.IndexName
               ?? $"PK_{tableName}";

        private static bool IsSyntheticIndexKey(string indexName)
            => indexName.StartsWith("__PK__", StringComparison.Ordinal)
               || indexName.StartsWith("__UQ__", StringComparison.Ordinal);

        /// <summary>
        /// Builds a map of index name to (IsUnique, column names[]) from the columns of a table.
        /// Only columns that carry an <see cref="ColumnSchema.IndexName"/> or are PK/Unique are included.
        /// </summary>
        private static Dictionary<string, (bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)> BuildIndexMap(TableSchema table)
        {
            var intermediate = new Dictionary<string, (bool IsUnique, bool NullsNotDistinct, List<(int? Order, int Sequence, string Name, bool IsDescending, IndexNullSortOrder NullSortOrder)> Columns, List<(int Sequence, string Name)> IncludedColumns, string? FilterSql)>(StringComparer.OrdinalIgnoreCase);
            var sequence = 0;
            foreach (var col in table.Columns)
            {
                if (col.Indexes.Count > 0)
                {
                    foreach (var index in col.Indexes)
                    {
                        if (string.IsNullOrWhiteSpace(index.Name))
                            continue;
                        if (!intermediate.TryGetValue(index.Name, out var explicitEntry))
                            explicitEntry = (index.IsUnique, index.NullsNotDistinct, new List<(int? Order, int Sequence, string Name, bool IsDescending, IndexNullSortOrder NullSortOrder)>(), new List<(int Sequence, string Name)>(), null);
                        explicitEntry.IsUnique = explicitEntry.IsUnique || index.IsUnique;
                        explicitEntry.NullsNotDistinct = explicitEntry.NullsNotDistinct || index.NullsNotDistinct;
                        if (!string.IsNullOrWhiteSpace(index.FilterSql) && string.IsNullOrWhiteSpace(explicitEntry.FilterSql))
                            explicitEntry.FilterSql = index.FilterSql;
                        if (index.IsIncluded)
                            explicitEntry.IncludedColumns.Add((sequence++, col.Name));
                        else
                            explicitEntry.Columns.Add((index.Order, sequence++, col.Name, index.IsDescending, index.NullSortOrder));
                        intermediate[index.Name] = explicitEntry;
                    }
                }

                // Include columns that have an explicit IndexName, or are unique/PK (implicit constraint).
                // A primary key's IndexName is the constraint name, not a secondary index name.
                string? indexKey = col.Indexes.Count > 0 || col.IsPrimaryKey ? null : col.IndexName;
                if (indexKey == null)
                {
                    if (col.IsPrimaryKey)
                        indexKey = $"__PK__{col.Name}";
                    else if (col.IsUnique)
                        indexKey = $"__UQ__{col.Name}";
                    else
                        continue;
                }
                if (!intermediate.TryGetValue(indexKey, out var entry))
                    entry = (col.IsUnique || col.IsPrimaryKey, false, new List<(int? Order, int Sequence, string Name, bool IsDescending, IndexNullSortOrder NullSortOrder)>(), new List<(int Sequence, string Name)>(), null);
                entry.IsUnique = entry.IsUnique || col.IsUnique || col.IsPrimaryKey;
                entry.Columns.Add((col.IndexOrder, sequence++, col.Name, false, IndexNullSortOrder.Default));
                intermediate[indexKey] = entry;
            }

            var map = new Dictionary<string, (bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql)>(intermediate.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var (key, (isUnique, nullsNotDistinct, columns, includedColumnsRaw, filterSql)) in intermediate)
            {
                var orderedColumns = columns
                    .OrderBy(static column => column.Order ?? int.MaxValue)
                    .ThenBy(static column => column.Sequence)
                    .ToArray();
                if (orderedColumns.Length == 0)
                    continue;

                var includedColumns = includedColumnsRaw
                    .OrderBy(static column => column.Sequence)
                    .Select(static column => column.Name)
                    .ToArray();
                map[key] = (isUnique, orderedColumns.Select(static column => column.Name).ToArray(), orderedColumns.Select(static column => column.IsDescending).ToArray(), orderedColumns.Select(static column => column.NullSortOrder).ToArray(), includedColumns, nullsNotDistinct, filterSql);
            }
            return map;
        }

        private static Dictionary<string, CheckConstraintSchema> BuildCheckMap(TableSchema table)
        {
            var map = new Dictionary<string, CheckConstraintSchema>(table.CheckConstraints.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var check in table.CheckConstraints)
            {
                if (!map.TryAdd(check.ConstraintName, check))
                    throw new InvalidOperationException(
                        $"Duplicate CHECK constraint name '{check.ConstraintName}' on table '{table.Name}'.");
            }
            return map;
        }

        private static Dictionary<string, ExpressionIndexSchema> BuildExpressionIndexMap(TableSchema table)
        {
            var map = new Dictionary<string, ExpressionIndexSchema>(table.ExpressionIndexes.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var expressionIndex in table.ExpressionIndexes)
            {
                if (!map.TryAdd(expressionIndex.Name, expressionIndex))
                    throw new InvalidOperationException(
                        $"Duplicate expression index name '{expressionIndex.Name}' on table '{table.Name}'.");
            }
            return map;
        }

    }
}
