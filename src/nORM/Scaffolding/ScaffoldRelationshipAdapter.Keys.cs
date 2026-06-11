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
            IEnumerable<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys)
            => ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(ConvertForeignKeyInfos(foreignKeys.ToArray()));

        public static bool HasExactUniqueIndex(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
            => ScaffoldForeignKeyShape.HasExactUniqueIndex(ConvertIndexInfos(indexes), tableKey, columnNames);

        public static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
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
            IGrouping<string, DatabaseScaffolder.ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => ScaffoldForeignKeyShape.ReferencesPrimaryKey(ConvertForeignKeyInfos(foreignKeyGroup.ToArray()), primaryKeyColumnsByTable);

        public static bool ReferencesScaffoldablePrincipalKey(
            IGrouping<string, DatabaseScaffolder.ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(
                ConvertForeignKeyInfos(foreignKeyGroup.ToArray()),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> BuildPrimaryKeyConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, string> primaryKeyConstraintNamesByTable,
            IReadOnlySet<string> skippedTableKeys)
            => ConvertPrimaryKeyConfigurations(ScaffoldPrimaryKeyConfigurationBuilder.BuildPrimaryKeyConfigurations(
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                skippedTableKeys));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> ConvertPrimaryKeyConfigurations(
            IReadOnlyList<ScaffoldPrimaryKeyConfigurationInfo> primaryKeys)
            => primaryKeys
                .Select(static key => new DatabaseScaffolder.ScaffoldPrimaryKey(key.EntityName, key.PropertyNames.ToArray(), key.ConstraintName))
                .ToArray();

        public static bool ReferencesUniqueIndex(
            IGrouping<string, DatabaseScaffolder.ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
            => ReferencesUniqueIndex(foreignKeyGroup.ToArray(), primaryKeyColumnsByTable, indexes);

        public static bool ReferencesUniqueIndex(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.ReferencesUniqueIndex(
                ConvertForeignKeyInfos(rows),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        public static bool IsUnfilteredUniqueKeyIndex(DatabaseScaffolder.ScaffoldIndex index)
            => ScaffoldForeignKeyShape.IsUnfilteredUniqueKeyIndex(ConvertIndexInfos(new[] { index })[0]);
    }
}
