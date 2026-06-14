#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyShape
    {
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
    }
}
