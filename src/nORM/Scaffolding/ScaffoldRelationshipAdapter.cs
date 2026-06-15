#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipAdapter
    {
        public static IReadOnlyList<ScaffoldManyToManyJoin> BuildManyToManyJoins(
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

        public static IReadOnlyList<ScaffoldManyToManyNavigation> BuildManyToManyNavigations(
            IReadOnlyList<ScaffoldManyToManyJoin> joins,
            string tableKey)
        {
            var navigations = new List<ScaffoldManyToManyNavigation>();
            foreach (var join in joins)
            {
                if (string.Equals(join.LeftTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                {
                    navigations.Add(new ScaffoldManyToManyNavigation(
                        join.RightEntityName,
                        join.LeftCollectionNavigationName));
                }

                if (string.Equals(join.RightTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                {
                    navigations.Add(new ScaffoldManyToManyNavigation(
                        join.LeftEntityName,
                        join.RightCollectionNavigationName));
                }
            }

            return navigations;
        }

        public static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
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
