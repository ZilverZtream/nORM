#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static class ScaffoldRelationshipNavigationNameBuilder
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
            var referenceBase = ScaffoldNameHelper.ToNavigationName(principalEntity);
            if (isSelfRelationship || hasMultipleRelationshipsToSamePrincipal)
            {
                referenceBase = BuildRelationshipReferenceNavigationBase(roleForeignKeyProperty, principalEntity, rolePrincipalKeyProperty);
                if (dependentMemberNames.Contains(referenceBase))
                    referenceBase += "Navigation";
                if (string.IsNullOrWhiteSpace(referenceBase))
                    referenceBase = ScaffoldNameHelper.ToNavigationName(principalEntity) + "Navigation";
            }

            var principalMemberNames = GetOrCreateMemberNames(memberNamesByTable, principalKey);
            var collectionBase = isUniqueDependentKey
                ? ScaffoldNameHelper.ToNavigationName(dependentEntity)
                : ScaffoldNameHelper.Pluralize(ScaffoldNameHelper.ToNavigationName(dependentEntity));
            if (isSelfRelationship
                || hasMultipleRelationshipsToSamePrincipal
                || principalMemberNames.Contains(collectionBase))
            {
                collectionBase = (isUniqueDependentKey
                    ? ScaffoldNameHelper.ToNavigationName(dependentEntity)
                    : ScaffoldNameHelper.Pluralize(ScaffoldNameHelper.ToNavigationName(dependentEntity)))
                    + "By"
                    + ScaffoldNameHelper.ToNavigationName(roleForeignKeyProperty);
            }

            return (
                ScaffoldNameHelper.MakeUnique(referenceBase, dependentMemberNames),
                ScaffoldNameHelper.MakeUnique(collectionBase, principalMemberNames));
        }

        private static (string ForeignKeyProperty, string PrincipalKeyProperty) SelectRelationshipRoleProperties(
            IReadOnlyList<string> foreignKeyProperties,
            IReadOnlyList<string> principalKeyProperties)
        {
            var count = Math.Min(foreignKeyProperties.Count, principalKeyProperties.Count);
            for (var i = 0; i < count; i++)
            {
                if (!string.Equals(foreignKeyProperties[i], principalKeyProperties[i], StringComparison.OrdinalIgnoreCase))
                    return (foreignKeyProperties[i], principalKeyProperties[i]);
            }

            if (foreignKeyProperties.Count == 0)
                return (string.Empty, string.Empty);

            var lastIndex = foreignKeyProperties.Count - 1;
            var principalKeyProperty = principalKeyProperties.Count > lastIndex
                ? principalKeyProperties[lastIndex]
                : string.Empty;
            return (foreignKeyProperties[lastIndex], principalKeyProperty);
        }

        private static string BuildRelationshipReferenceNavigationBase(
            string foreignKeyProperty,
            string principalEntityName,
            string principalKeyProperty)
        {
            var candidate = TrimIdSuffix(foreignKeyProperty);
            if (!IsIdentityKeyProperty(principalKeyProperty, principalEntityName)
                && !string.IsNullOrWhiteSpace(principalKeyProperty)
                && foreignKeyProperty.EndsWith(principalKeyProperty, StringComparison.Ordinal)
                && foreignKeyProperty.Length > principalKeyProperty.Length)
            {
                var prefix = foreignKeyProperty[..^principalKeyProperty.Length];
                if (!string.IsNullOrWhiteSpace(prefix))
                {
                    var principalNavigationName = RelationshipRoleTargetFromPrincipalKey(principalKeyProperty);
                    if (string.IsNullOrWhiteSpace(principalNavigationName))
                        principalNavigationName = ScaffoldNameHelper.ToNavigationName(principalEntityName);
                    candidate = prefix.EndsWith(principalNavigationName, StringComparison.Ordinal)
                        ? prefix
                        : prefix + principalNavigationName;
                }
            }

            return ScaffoldNameHelper.ToNavigationName(candidate);
        }

        private static string RelationshipRoleTargetFromPrincipalKey(string principalKeyProperty)
        {
            if (string.IsNullOrWhiteSpace(principalKeyProperty))
                return string.Empty;

            foreach (var suffix in new[] { "Number", "Code", "Key", "No" })
            {
                if (principalKeyProperty.EndsWith(suffix, StringComparison.Ordinal)
                    && principalKeyProperty.Length > suffix.Length)
                {
                    return ScaffoldNameHelper.ToNavigationName(principalKeyProperty[..^suffix.Length]);
                }
            }

            return ScaffoldNameHelper.ToNavigationName(principalKeyProperty);
        }

        private static bool IsIdentityKeyProperty(string propertyName, string principalEntityName)
        {
            if (string.Equals(propertyName, "Id", StringComparison.Ordinal)
                || string.Equals(propertyName, "ID", StringComparison.Ordinal))
            {
                return true;
            }

            return string.Equals(
                ScaffoldNameHelper.ToNavigationName(TrimIdSuffix(propertyName)),
                ScaffoldNameHelper.ToNavigationName(principalEntityName),
                StringComparison.Ordinal);
        }

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

        private static string TrimIdSuffix(string name)
        {
            if (name.EndsWith("Id", StringComparison.Ordinal) && name.Length > 2)
                return name[..^2];

            if (name.EndsWith("_id", StringComparison.OrdinalIgnoreCase) && name.Length > 3)
                return name[..^3];

            return name;
        }
    }
}
