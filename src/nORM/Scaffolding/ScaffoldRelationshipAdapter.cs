#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldRelationshipAdapter
    {
        public static IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> BuildManyToManyJoins(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var joins = ScaffoldManyToManyJoinDiscovery.BuildManyToManyJoins(
                ConvertForeignKeyInfos(foreignKeys),
                ScaffoldSchemaDiscoveryAdapter.ToScaffoldTableInfos(tables),
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                identityColumnsByTable,
                databaseGeneratedColumnsByTable,
                ConvertIndexInfos(indexes),
                nonNullableColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                memberNamesByTable);
            return ConvertManyToManyJoins(joins);
        }

        public static IReadOnlyList<ScaffoldForeignKeyInfo> ConvertForeignKeyInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys)
        {
            var converted = new ScaffoldForeignKeyInfo[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new ScaffoldForeignKeyInfo(
                    foreignKey.DependentSchema,
                    foreignKey.DependentTable,
                    foreignKey.DependentColumn,
                    foreignKey.PrincipalSchema,
                    foreignKey.PrincipalTable,
                    foreignKey.PrincipalColumn,
                    foreignKey.ConstraintName,
                    foreignKey.ColumnCount,
                    foreignKey.OnDelete,
                    foreignKey.OnUpdate,
                    foreignKey.IsSyntheticConstraintName);
            }

            return converted;
        }

        public static IReadOnlyList<ScaffoldIndexInfo> ConvertIndexInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var converted = new ScaffoldIndexInfo[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new ScaffoldIndexInfo(
                    index.TableKey,
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql,
                    index.IsSyntheticName);
            }

            return converted;
        }

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> ConvertManyToManyJoins(
            IReadOnlyList<ScaffoldManyToManyJoinInfo> joins)
        {
            var converted = new DatabaseScaffolder.ScaffoldManyToManyJoin[joins.Count];
            for (var i = 0; i < joins.Count; i++)
            {
                var join = joins[i];
                converted[i] = new DatabaseScaffolder.ScaffoldManyToManyJoin(
                    join.JoinTableKey,
                    join.LeftTableKey,
                    join.RightTableKey,
                    join.JoinTableName,
                    join.JoinTableSchema,
                    join.LeftEntityName,
                    join.RightEntityName,
                    join.LeftForeignKeyColumns,
                    join.RightForeignKeyColumns,
                    join.LeftPrincipalKeyProperties,
                    join.RightPrincipalKeyProperties,
                    join.LeftOnDelete,
                    join.LeftOnUpdate,
                    join.RightOnDelete,
                    join.RightOnUpdate,
                    join.UsesPrimaryKeys,
                    join.LeftCollectionNavigationName,
                    join.RightCollectionNavigationName);
            }

            return converted;
        }

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> ConvertRelationships(
            IReadOnlyList<ScaffoldRelationshipInfo> relationships)
        {
            var converted = new DatabaseScaffolder.ScaffoldRelationship[relationships.Count];
            for (var i = 0; i < relationships.Count; i++)
            {
                var relationship = relationships[i];
                converted[i] = new DatabaseScaffolder.ScaffoldRelationship(
                    relationship.DependentTableKey,
                    relationship.PrincipalTableKey,
                    relationship.DependentEntityName,
                    relationship.PrincipalEntityName,
                    relationship.ForeignKeyPropertyName,
                    relationship.PrincipalKeyPropertyName,
                    relationship.ReferenceNavigationName,
                    relationship.CollectionNavigationName,
                    relationship.IsUniqueDependentKey,
                    relationship.CascadeDelete,
                    relationship.OnDelete,
                    relationship.OnUpdate,
                    relationship.ConstraintName)
                {
                    IsRequired = relationship.IsRequired,
                    ForeignKeyPropertyNames = relationship.ForeignKeyPropertyNames,
                    PrincipalKeyPropertyNames = relationship.PrincipalKeyPropertyNames
                };
            }

            return converted;
        }

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyNavigation> BuildManyToManyNavigations(
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> joins,
            string tableKey)
        {
            var navigations = new List<DatabaseScaffolder.ScaffoldManyToManyNavigation>();
            foreach (var join in joins)
            {
                if (string.Equals(join.LeftTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                {
                    navigations.Add(new DatabaseScaffolder.ScaffoldManyToManyNavigation(
                        join.RightEntityName,
                        join.LeftCollectionNavigationName));
                }

                if (string.Equals(join.RightTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                {
                    navigations.Add(new DatabaseScaffolder.ScaffoldManyToManyNavigation(
                        join.LeftEntityName,
                        join.RightCollectionNavigationName));
                }
            }

            return navigations;
        }

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

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
            => ConvertRelationships(ScaffoldRelationshipDiscovery.BuildRelationships(
                ConvertForeignKeyInfos(foreignKeys),
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes),
                nonNullableColumnsByTable,
                memberNamesByTable));
    }
}
