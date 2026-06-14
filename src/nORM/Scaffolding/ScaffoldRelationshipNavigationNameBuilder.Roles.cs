#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipNavigationNameBuilder
    {
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
