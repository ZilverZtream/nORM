#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipNavigationNameBuilder
    {
        public static (string ReferenceName, string CollectionName) BuildNavigationNames(
            string dependentKey,
            string principalKey,
            string dependentEntity,
            string principalEntity,
            IReadOnlyList<string> foreignKeyProperties,
            IReadOnlyList<string> principalKeyProperties,
            bool isUniqueDependentKey,
            IReadOnlyDictionary<string, int> relationshipPairCounts,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var (roleForeignKeyProperty, rolePrincipalKeyProperty) = SelectRelationshipRoleProperties(
                foreignKeyProperties,
                principalKeyProperties);
            var hasMultipleRelationshipsToSamePrincipal = relationshipPairCounts.TryGetValue(
                dependentKey + "\u001f" + principalKey,
                out var relationshipPairCount)
                && relationshipPairCount > 1;
            var isSelfRelationship = string.Equals(dependentKey, principalKey, StringComparison.OrdinalIgnoreCase);
            var dependentMemberNames = GetOrCreateMemberNames(memberNamesByTable, dependentKey);
            var principalMemberNames = GetOrCreateMemberNames(memberNamesByTable, principalKey);
            var referenceBase = BuildReferenceNavigationBase(
                principalEntity,
                roleForeignKeyProperty,
                rolePrincipalKeyProperty,
                isSelfRelationship,
                hasMultipleRelationshipsToSamePrincipal,
                dependentMemberNames);
            var collectionBase = BuildCollectionNavigationBase(
                dependentEntity,
                roleForeignKeyProperty,
                isUniqueDependentKey,
                isSelfRelationship,
                hasMultipleRelationshipsToSamePrincipal,
                principalMemberNames);

            return (
                ScaffoldNameHelper.MakeUnique(referenceBase, dependentMemberNames),
                ScaffoldNameHelper.MakeUnique(collectionBase, principalMemberNames));
        }

        private static string BuildReferenceNavigationBase(
            string principalEntity,
            string roleForeignKeyProperty,
            string rolePrincipalKeyProperty,
            bool isSelfRelationship,
            bool hasMultipleRelationshipsToSamePrincipal,
            IReadOnlySet<string> dependentMemberNames)
        {
            var referenceBase = ScaffoldNameHelper.ToNavigationName(principalEntity);
            if (!isSelfRelationship && !hasMultipleRelationshipsToSamePrincipal)
                return referenceBase;

            referenceBase = BuildRelationshipReferenceNavigationBase(roleForeignKeyProperty, principalEntity, rolePrincipalKeyProperty);
            if (dependentMemberNames.Contains(referenceBase))
                referenceBase += "Navigation";
            if (string.IsNullOrWhiteSpace(referenceBase))
                referenceBase = ScaffoldNameHelper.ToNavigationName(principalEntity) + "Navigation";

            return referenceBase;
        }

        private static string BuildCollectionNavigationBase(
            string dependentEntity,
            string roleForeignKeyProperty,
            bool isUniqueDependentKey,
            bool isSelfRelationship,
            bool hasMultipleRelationshipsToSamePrincipal,
            IReadOnlySet<string> principalMemberNames)
        {
            var collectionBase = BuildDependentNavigationBase(dependentEntity, isUniqueDependentKey);
            if (!isSelfRelationship
                && !hasMultipleRelationshipsToSamePrincipal
                && !principalMemberNames.Contains(collectionBase))
            {
                return collectionBase;
            }

            return collectionBase + "By" + ScaffoldNameHelper.ToNavigationName(roleForeignKeyProperty);
        }

        private static string BuildDependentNavigationBase(string dependentEntity, bool isUniqueDependentKey)
            => isUniqueDependentKey
                ? ScaffoldNameHelper.ToNavigationName(dependentEntity)
                : ScaffoldNameHelper.Pluralize(ScaffoldNameHelper.ToNavigationName(dependentEntity));

        private static HashSet<string> GetOrCreateMemberNames(
            Dictionary<string, HashSet<string>> memberNamesByTable,
            string tableKey)
        {
            if (!memberNamesByTable.TryGetValue(tableKey, out var names))
            {
                names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                memberNamesByTable[tableKey] = names;
            }

            return names;
        }
    }
}
