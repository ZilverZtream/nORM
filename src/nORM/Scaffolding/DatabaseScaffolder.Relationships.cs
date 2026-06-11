#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyList<ScaffoldManyToManyJoin> BuildManyToManyJoins(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            Dictionary<string, HashSet<string>> memberNamesByTable)
            => ScaffoldRelationshipAdapter.BuildManyToManyJoins(
                foreignKeys,
                tables,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                identityColumnsByTable,
                databaseGeneratedColumnsByTable,
                indexes,
                nonNullableColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                memberNamesByTable);

        private static IReadOnlyList<ScaffoldForeignKeyInfo> ConvertForeignKeyInfos(IReadOnlyList<ScaffoldForeignKey> foreignKeys)
            => ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys);

        private static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> ConvertUnsupportedFeatureInfos(
            IReadOnlyList<ScaffoldUnsupportedFeature> features)
            => ScaffoldDiagnosticsAdapter.ConvertUnsupportedFeatureInfos(features);

        private static IReadOnlyList<ScaffoldIndexInfo> ConvertIndexInfos(IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes);

        private static IReadOnlyList<ScaffoldManyToManyJoin> ConvertManyToManyJoins(IReadOnlyList<ScaffoldManyToManyJoinInfo> joins)
            => ScaffoldRelationshipAdapter.ConvertManyToManyJoins(joins);

        private static IReadOnlyList<ScaffoldRelationship> ConvertRelationships(IReadOnlyList<ScaffoldRelationshipInfo> relationships)
            => ScaffoldRelationshipAdapter.ConvertRelationships(relationships);

        private static IReadOnlyList<ScaffoldManyToManyNavigation> BuildManyToManyNavigations(
            IReadOnlyList<ScaffoldManyToManyJoin> joins,
            string tableKey)
            => ScaffoldRelationshipAdapter.BuildManyToManyNavigations(joins, tableKey);

        private static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
            => ScaffoldRelationshipAdapter.HasSinglePrimaryKeyColumn(primaryKeyColumnsByTable, tableKey, columnName);

        private static bool HasPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => ScaffoldRelationshipAdapter.HasPrimaryKeyColumns(primaryKeyColumnsByTable, tableKey, columnNames);

        private static bool HasOnlyScaffoldableReferentialActions(IEnumerable<ScaffoldForeignKey> foreignKeys)
            => ScaffoldRelationshipAdapter.HasOnlyScaffoldableReferentialActions(foreignKeys);

        private static bool HasExactUniqueIndex(
            IReadOnlyList<ScaffoldIndex> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
            => ScaffoldRelationshipAdapter.HasExactUniqueIndex(indexes, tableKey, columnNames);

        private static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<ScaffoldForeignKey>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.AllForeignKeyGroupsAreUniqueDependentKeys(
                dependentTableKey,
                foreignKeyGroups,
                primaryKeyColumnsByTable,
                indexes);

        private static bool HasNonNullableColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => ScaffoldRelationshipAdapter.HasNonNullableColumns(nonNullableColumnsByTable, tableKey, columnNames);

        private static bool ReferencesPrimaryKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => ScaffoldRelationshipAdapter.ReferencesPrimaryKey(foreignKeyGroup, primaryKeyColumnsByTable);

        private static bool ReferencesScaffoldablePrincipalKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ReferencesScaffoldablePrincipalKey(
                foreignKeyGroup,
                primaryKeyColumnsByTable,
                indexes);

        private static IReadOnlyList<ScaffoldPrimaryKey> BuildPrimaryKeyConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, string> primaryKeyConstraintNamesByTable,
            IReadOnlySet<string> skippedTableKeys)
            => ScaffoldRelationshipAdapter.BuildPrimaryKeyConfigurations(
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                skippedTableKeys);

        private static IReadOnlyList<ScaffoldPrimaryKey> ConvertPrimaryKeyConfigurations(
            IReadOnlyList<ScaffoldPrimaryKeyConfigurationInfo> primaryKeys)
            => ScaffoldRelationshipAdapter.ConvertPrimaryKeyConfigurations(primaryKeys);

        private static bool ReferencesUniqueIndex(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes);

        private static bool ReferencesUniqueIndex(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ReferencesUniqueIndex(rows, primaryKeyColumnsByTable, indexes);

        private static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndex index)
            => ScaffoldRelationshipAdapter.IsUnfilteredUniqueKeyIndex(index);

        private static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
            => ScaffoldRelationshipAdapter.BuildRelationships(
                foreignKeys,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable,
                memberNamesByTable);
    }
}
