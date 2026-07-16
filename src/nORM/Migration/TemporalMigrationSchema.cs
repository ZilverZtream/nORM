using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Migration
{
    /// <summary>
    /// Column-level per-direction schema reconstruction for temporal companion DDL on the server
    /// generators. The versioning triggers enumerate the main table's full column set, so
    /// re-emitting them needs the complete post-Up (new) or post-Down (old) schema - not just the
    /// diff deltas. Deltas are applied idempotently, so the result is correct whether the base
    /// table already reflects them (real diffs) or not (manually-constructed test diffs).
    /// Temporal metadata (IsTemporal, TenantColumnName) always carries over: a lost tenant column
    /// would silently drop the tenant predicate from the regenerated triggers.
    /// </summary>
    internal static class TemporalMigrationSchema
    {
        internal static bool NameEquals(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Temporal tables whose COLUMN SET the diff changes (added/dropped/renamed columns) or
        /// whose column definitions change (altered). Added/dropped tables are handled separately.
        /// </summary>
        internal static IReadOnlyList<string> GetTemporalChangedTableNames(SchemaDiff diff)
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            void Consider(TableSchema table)
            {
                if (table.IsTemporal)
                    names.Add(table.Name);
            }
            foreach (var (table, _) in diff.AddedColumns) Consider(table);
            foreach (var (table, _) in diff.DroppedColumns) Consider(table);
            foreach (var (table, _, _) in diff.AlteredColumns) Consider(table);
            foreach (var (table, _, _) in diff.RenamedColumns) Consider(table);
            return names.OrderBy(static n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        }

        private static TableSchema ResolveBaseTable(SchemaDiff diff, string tableName)
        {
            foreach (var (t, _, _) in diff.AlteredColumns) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.AddedColumns) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _, _) in diff.RenamedColumns) if (NameEquals(t.Name, tableName)) return t;
            foreach (var (t, _) in diff.DroppedColumns) if (NameEquals(t.Name, tableName)) return t;
            throw new InvalidOperationException($"No schema source for temporal table '{tableName}'.");
        }

        private static TableSchema Copy(TableSchema baseTable, IEnumerable<ColumnSchema> cols)
        {
            var t = new TableSchema
            {
                Name = baseTable.Name,
                IsTemporal = baseTable.IsTemporal,
                TenantColumnName = baseTable.TenantColumnName
            };
            t.Columns.AddRange(cols);
            return t;
        }

        /// <summary>The complete post-Up column set for a temporal table.</summary>
        internal static TableSchema BuildNewSchema(SchemaDiff diff, string tableName)
        {
            var baseTable = ResolveBaseTable(diff, tableName);
            var droppedNames = diff.DroppedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .Select(static x => x.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var alteredNew = diff.AlteredColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(static x => x.NewColumn.Name, static x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
            var renamedNewByOld = diff.RenamedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(static x => x.OldColumnName, static x => x.NewColumn, StringComparer.OrdinalIgnoreCase);
            var added = diff.AddedColumns.Where(x => NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();

            var cols = new List<ColumnSchema>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in baseTable.Columns)
            {
                if (droppedNames.Contains(c.Name))
                    continue;
                // A base still carrying the pre-rename name maps forward to the new column.
                var col = renamedNewByOld.TryGetValue(c.Name, out var renamed)
                    ? renamed
                    : alteredNew.TryGetValue(c.Name, out var altered) ? altered : c;
                if (seen.Add(col.Name))
                    cols.Add(col);
            }
            foreach (var a in added)
                if (seen.Add(a.Name))
                    cols.Add(a);
            return Copy(baseTable, cols);
        }

        /// <summary>The complete pre-migration column set for a temporal table.</summary>
        internal static TableSchema BuildOldSchema(SchemaDiff diff, string tableName)
        {
            var baseTable = ResolveBaseTable(diff, tableName);
            var alteredOld = diff.AlteredColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(static x => x.NewColumn.Name, static x => x.OldColumn, StringComparer.OrdinalIgnoreCase);
            var addedNames = diff.AddedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .Select(static x => x.Column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var renamedOldByNew = diff.RenamedColumns.Where(x => NameEquals(x.Table.Name, tableName))
                .ToDictionary(static x => x.NewColumn.Name, static x => x.OldColumnName, StringComparer.OrdinalIgnoreCase);
            var dropped = diff.DroppedColumns.Where(x => NameEquals(x.Table.Name, tableName)).Select(static x => x.Column).ToList();

            var cols = new List<ColumnSchema>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in baseTable.Columns)
            {
                if (addedNames.Contains(c.Name))
                    continue;
                ColumnSchema col;
                if (alteredOld.TryGetValue(c.Name, out var oldCol))
                    col = oldCol;
                else if (renamedOldByNew.TryGetValue(c.Name, out var oldName))
                {
                    col = c.Clone();
                    col.Name = oldName;
                }
                else
                    col = c;
                if (seen.Add(col.Name))
                    cols.Add(col);
            }
            foreach (var d in dropped)
                if (seen.Add(d.Name))
                    cols.Add(d);
            return Copy(baseTable, cols);
        }
    }
}
