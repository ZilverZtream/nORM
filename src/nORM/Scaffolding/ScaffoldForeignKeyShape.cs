#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldForeignKeyShape
    {
        public static bool HasPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => primaryKeyColumnsByTable.TryGetValue(tableKey, out var keyColumns)
               && keyColumns.Count == columnNames.Count
               && keyColumns.SequenceEqual(columnNames, StringComparer.OrdinalIgnoreCase);

        public static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
            => primaryKeyColumnsByTable.TryGetValue(tableKey, out var keyColumns)
               && keyColumns.Count == 1
               && keyColumns.Contains(columnName);

        public static bool HasOnlyScaffoldableReferentialActions(IEnumerable<ScaffoldForeignKeyInfo> foreignKeys)
            => foreignKeys.All(static fk =>
                IsScaffoldableReferentialAction(fk.OnDelete)
                && IsScaffoldableReferentialAction(fk.OnUpdate));

        public static bool HasExactUniqueColumnSet(
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
            => indexes
                .Where(index => IsUnfilteredUniqueKeyIndex(index)
                                && string.Equals(index.TableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .GroupBy(static index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .Any(group =>
                {
                    var keyColumns = group
                        .Where(static index => !index.IsIncluded)
                        .OrderBy(static index => index.Ordinal)
                        .Select(static index => index.ColumnName)
                        .ToArray();
                    return keyColumns.Length == columnNames.Count
                           && keyColumns.All(columnNames.Contains);
                });

        public static bool HasExactOrderedUniqueIndex(
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => indexes
                .Where(index => IsUnfilteredUniqueKeyIndex(index)
                                && string.Equals(index.TableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .GroupBy(static index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .Any(group =>
                {
                    var keyColumns = group
                        .Where(static index => !index.IsIncluded)
                        .OrderBy(static index => index.Ordinal)
                        .Select(static index => index.ColumnName)
                        .ToArray();
                    return keyColumns.Length == columnNames.Count
                           && group.All(col => col.ColumnCount == columnNames.Count)
                           && keyColumns.SequenceEqual(columnNames, StringComparer.OrdinalIgnoreCase);
                });

        public static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<ScaffoldForeignKeyInfo>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var groups = foreignKeyGroups
                .Select(group => group.ToArray())
                .Where(group => group.Length > 0)
                .ToArray();

            return groups.Length >= 2
                   && groups.All(group =>
                   {
                       var dependentColumns = group.Select(static row => row.DependentColumn).ToArray();
                       return HasPrimaryKeyColumns(primaryKeyColumnsByTable, dependentTableKey, dependentColumns)
                              || HasExactUniqueColumnSet(indexes, dependentTableKey, dependentColumns.ToHashSet(StringComparer.OrdinalIgnoreCase));
                   });
        }

        public static bool ReferencesScaffoldablePrincipalKey(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes)
            => ReferencesPrimaryKey(foreignKeyGroup, primaryKeyColumnsByTable)
               || ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes);

        public static bool ReferencesPrimaryKey(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
        {
            if (rows.Count == 0)
                return false;

            var principalKey = TableKey(rows[0].PrincipalSchema, rows[0].PrincipalTable);
            var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
            return primaryKeyColumnsByTable.TryGetValue(principalKey, out var keyColumns)
                   && keyColumns.Count == rows.Count
                   && keyColumns.SequenceEqual(principalColumns, StringComparer.OrdinalIgnoreCase);
        }

        public static bool ReferencesUniqueIndex(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            if (rows.Count == 0)
                return false;

            var principalKey = TableKey(rows[0].PrincipalSchema, rows[0].PrincipalTable);
            if (!primaryKeyColumnsByTable.TryGetValue(principalKey, out var primaryKeyColumns)
                || primaryKeyColumns.Count == 0)
            {
                return false;
            }

            var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
            return HasExactOrderedUniqueIndex(indexes, principalKey, principalColumns);
        }

        public static string NormalizeReferentialAction(string? action)
        {
            if (string.IsNullOrWhiteSpace(action))
                return "NO ACTION";

            return action.Replace('_', ' ').Trim().ToUpperInvariant();
        }

        public static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        public static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndexInfo index)
            => index.IsUnique
               && !index.IsIncluded
               && string.IsNullOrWhiteSpace(index.FilterSql);

        private static bool IsScaffoldableReferentialAction(string? action)
            => NormalizeReferentialAction(action) is "CASCADE" or "SET NULL" or "SET DEFAULT" or "RESTRICT" or "NO ACTION";
    }
}
