#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldJoinTableShape
    {
        private static readonly IReadOnlySet<string> EmptyStringSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public static bool HasExactBridgePrimaryKey(
            IReadOnlyList<string> primaryKeyColumns,
            IReadOnlySet<string> foreignKeyColumns)
            => primaryKeyColumns.Count == foreignKeyColumns.Count
               && primaryKeyColumns.All(foreignKeyColumns.Contains);

        public static bool HasGeneratedSurrogatePrimaryKey(
            IReadOnlyList<string> primaryKeyColumns,
            IReadOnlySet<string> foreignKeyColumns,
            IReadOnlySet<string> databaseGeneratedColumns,
            IReadOnlySet<string> identityColumns)
        {
            var primaryKeyExtraColumns = primaryKeyColumns
                .Where(column => !foreignKeyColumns.Contains(column))
                .ToArray();
            return primaryKeyColumns.Count == 1
                   && primaryKeyExtraColumns.Length == 1
                   && (databaseGeneratedColumns.Contains(primaryKeyExtraColumns[0])
                       || identityColumns.Contains(primaryKeyExtraColumns[0]));
        }

        public static IReadOnlySet<string> GetColumnSet(
            IReadOnlyDictionary<string, IReadOnlySet<string>> columnsByTable,
            string tableKey)
            => columnsByTable.TryGetValue(tableKey, out var columns) ? columns : EmptyStringSet;
    }
}
