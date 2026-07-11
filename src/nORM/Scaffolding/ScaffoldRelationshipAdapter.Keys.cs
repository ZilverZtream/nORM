#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipAdapter
    {
        public static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
            => ScaffoldForeignKeyShape.HasSinglePrimaryKeyColumn(primaryKeyColumnsByTable, tableKey, columnName);

        public static bool HasPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, tableKey, columnNames);

        public static bool HasOnlyScaffoldableReferentialActions(
            IEnumerable<ScaffoldForeignKey> foreignKeys)
            => ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(ConvertForeignKeyInfos(foreignKeys.ToArray()));

        public static bool HasExactUniqueColumnSet(
            IReadOnlyList<ScaffoldIndex> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
            => ScaffoldForeignKeyShape.HasExactUniqueColumnSet(ConvertIndexInfos(indexes), tableKey, columnNames);

        public static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<ScaffoldForeignKey>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.AllForeignKeyGroupsAreUniqueDependentKeys(
                dependentTableKey,
                foreignKeyGroups.Select(ConvertForeignKeyInfos).ToArray(),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        public static bool HasNonNullableColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
               && columnNames.All(nonNullableColumns.Contains);

        public static bool ReferencesPrimaryKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => ScaffoldForeignKeyShape.ReferencesPrimaryKey(ConvertForeignKeyInfos(foreignKeyGroup.ToArray()), primaryKeyColumnsByTable);

        public static bool ReferencesScaffoldablePrincipalKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(
                ConvertForeignKeyInfos(foreignKeyGroup.ToArray()),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        public static bool ReferencesUniqueIndex(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ReferencesUniqueIndex(foreignKeyGroup.ToArray(), primaryKeyColumnsByTable, indexes);

        public static bool ReferencesUniqueIndex(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.ReferencesUniqueIndex(
                ConvertForeignKeyInfos(rows),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        public static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndex index)
            => ScaffoldForeignKeyShape.IsUnfilteredUniqueKeyIndex(ConvertIndexInfos(new[] { index })[0]);
    }
}
