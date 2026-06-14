#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyShape
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
                           && group.All(col => col.ColumnCount == columnNames.Count)
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

        public static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndexInfo index)
            => index.IsUnique
               && !index.IsIncluded
               && string.IsNullOrWhiteSpace(index.FilterSql);
    }
}
