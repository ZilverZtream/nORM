#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelCompositionBuilder
    {
        private static IReadOnlyList<ScaffoldManyToManyJoin> BuildManyToManyJoins(
            ScaffoldModelDiscoveryResult discovery,
            ScaffoldFeatureConfigurations featureConfigurations)
            => ScaffoldRelationshipAdapter.BuildManyToManyJoins(
                discovery.ForeignKeys,
                discovery.Tables,
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.IdentityColumnsByTable,
                featureConfigurations.ComputedColumnsByTable,
                discovery.Indexes,
                discovery.NonNullableColumnsByTable,
                featureConfigurations.ProviderOwnedWriteBlockedTableKeys,
                discovery.MemberNamesByTable);

        private static IReadOnlySet<string> BuildManyToManyJoinTableKeys(
            IEnumerable<ScaffoldManyToManyJoin> manyToManyJoins)
            => manyToManyJoins
                .Select(static join => join.JoinTableKey)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<ScaffoldRelationship> BuildNonJoinRelationships(
            ScaffoldModelDiscoveryResult discovery,
            IReadOnlySet<string> manyToManyJoinTableKeys)
            => ScaffoldRelationshipAdapter.BuildRelationships(
                discovery.ForeignKeys
                    .Where(fk => !manyToManyJoinTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable)))
                    .ToArray(),
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.Indexes,
                discovery.NonNullableColumnsByTable,
                discovery.MemberNamesByTable);

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
