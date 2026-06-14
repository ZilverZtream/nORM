#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableMetadataBuilder
    {
        private static IReadOnlyDictionary<string, object?>[] BuildForeignKeyUniqueIndexCandidateMetadata(
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            string tableKey,
            IReadOnlySet<string> foreignKeyColumns)
            => indexes
                .Where(index => index.IsUnique
                                && string.Equals(index.TableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .GroupBy(static index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .Select(group =>
                {
                    var rows = group.ToArray();
                    var keyRows = rows
                        .Where(static index => !index.IsIncluded)
                        .OrderBy(static index => index.Ordinal)
                        .ToArray();
                    var keyColumns = keyRows
                        .Select(static index => index.ColumnName)
                        .ToArray();
                    var isExactForeignKeyColumnSet = keyColumns.Length == foreignKeyColumns.Count
                                                     && keyRows.All(index => index.ColumnCount == foreignKeyColumns.Count)
                                                     && keyColumns.All(foreignKeyColumns.Contains);
                    var filterSql = rows
                        .Select(static index => index.FilterSql)
                        .FirstOrDefault(static filter => !string.IsNullOrWhiteSpace(filter));
                    return new
                    {
                        Rows = rows,
                        KeyColumns = keyColumns,
                        IsExactForeignKeyColumnSet = isExactForeignKeyColumnSet,
                        FilterSql = filterSql
                    };
                })
                .Where(static candidate => candidate.IsExactForeignKeyColumnSet)
                .OrderBy(static candidate => candidate.Rows[0].IndexName, StringComparer.Ordinal)
                .Select(static candidate => new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["indexName"] = candidate.Rows[0].IndexName,
                    ["columns"] = candidate.KeyColumns,
                    ["isFiltered"] = !string.IsNullOrWhiteSpace(candidate.FilterSql),
                    ["filterSql"] = candidate.FilterSql,
                    ["isUnfilteredExactForeignKeyUniqueIndex"] = string.IsNullOrWhiteSpace(candidate.FilterSql)
                })
                .ToArray();
    }
}
