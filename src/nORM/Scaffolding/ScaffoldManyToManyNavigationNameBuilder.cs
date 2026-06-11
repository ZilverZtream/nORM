#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static class ScaffoldManyToManyNavigationNameBuilder
    {
        public static (string LeftCollectionName, string RightCollectionName) BuildCollectionNames(
            string joinTableKey,
            ScaffoldForeignKeyInfo left,
            ScaffoldForeignKeyInfo right,
            string leftTableKey,
            string leftEntityName,
            string rightTableKey,
            string rightEntityName,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var leftCollectionBase = ScaffoldNameHelper.Pluralize(ScaffoldNameHelper.ToNavigationName(rightEntityName));
            var rightCollectionBase = ScaffoldNameHelper.Pluralize(ScaffoldNameHelper.ToNavigationName(leftEntityName));
            var isSelfJoin = string.Equals(leftTableKey, rightTableKey, StringComparison.OrdinalIgnoreCase);
            if (isSelfJoin)
            {
                leftCollectionBase += "By" + ScaffoldNameHelper.ToNavigationName(GetColumnPropertyName(columnPropertiesByTable, joinTableKey, left.DependentColumn));
                rightCollectionBase += "By" + ScaffoldNameHelper.ToNavigationName(GetColumnPropertyName(columnPropertiesByTable, joinTableKey, right.DependentColumn));
            }

            var leftCollectionName = ScaffoldNameHelper.MakeUnique(leftCollectionBase, GetOrCreateMemberNames(memberNamesByTable, leftTableKey));
            var rightCollectionName = ScaffoldNameHelper.MakeUnique(rightCollectionBase, GetOrCreateMemberNames(memberNamesByTable, rightTableKey));
            return (leftCollectionName, rightCollectionName);
        }

        private static string GetColumnPropertyName(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            string tableKey,
            string columnName)
        {
            if (columnPropertiesByTable.TryGetValue(tableKey, out var properties)
                && properties.TryGetValue(columnName, out var propertyName))
            {
                return propertyName;
            }

            return ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(columnName));
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
    }
}
