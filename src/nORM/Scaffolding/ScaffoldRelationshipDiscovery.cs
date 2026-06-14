#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipDiscovery
    {
        public static IReadOnlyList<ScaffoldRelationshipInfo> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var relationships = new List<ScaffoldRelationshipInfo>();
            var relationshipPairCounts = BuildRelationshipPairCounts(foreignKeys);

            foreach (var foreignKeyGroup in foreignKeys
                .GroupBy(
                    static fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}",
                    StringComparer.OrdinalIgnoreCase))
            {
                if (TryBuildRelationship(
                    foreignKeyGroup,
                    entityByTable,
                    columnPropertiesByTable,
                    primaryKeyColumnsByTable,
                    indexes,
                    nonNullableColumnsByTable,
                    relationshipPairCounts,
                    memberNamesByTable,
                    out var relationship))
                {
                    relationships.Add(relationship);
                }
            }

            return relationships;
        }
    }
}
