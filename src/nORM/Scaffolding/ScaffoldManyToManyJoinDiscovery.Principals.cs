#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldManyToManyJoinDiscovery
    {
        private static bool TryGetManyToManyPrincipalInfo(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeyGroup,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            out ScaffoldManyToManyPrincipalInfo principal)
        {
            principal = default;
            if (foreignKeyGroup.Count == 0)
                return false;

            var first = foreignKeyGroup[0];
            var tableKey = TableKey(first.PrincipalSchema, first.PrincipalTable);
            if (!entityByTable.TryGetValue(tableKey, out var entityName))
                return false;

            var principalColumns = foreignKeyGroup.Select(static fk => fk.PrincipalColumn).ToArray();
            var usesPrimaryKey = ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, tableKey, principalColumns);
            if (!usesPrimaryKey && !ScaffoldForeignKeyShape.ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes))
                return false;

            var principalKeyProperties = principalColumns
                .Select(column => GetColumnPropertyName(columnPropertiesByTable, tableKey, column))
                .ToArray();
            principal = new ScaffoldManyToManyPrincipalInfo(
                tableKey,
                entityName,
                principalColumns,
                principalKeyProperties,
                usesPrimaryKey);
            return true;
        }

        private static (string LeftCollectionName, string RightCollectionName) BuildManyToManyCollectionNames(
            string joinTableKey,
            ScaffoldForeignKeyInfo left,
            ScaffoldForeignKeyInfo right,
            ScaffoldManyToManyPrincipalInfo leftPrincipal,
            ScaffoldManyToManyPrincipalInfo rightPrincipal,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
            => ScaffoldManyToManyNavigationNameBuilder.BuildCollectionNames(
                joinTableKey,
                left,
                right,
                leftPrincipal.TableKey,
                leftPrincipal.EntityName,
                rightPrincipal.TableKey,
                rightPrincipal.EntityName,
                columnPropertiesByTable,
                memberNamesByTable);

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

        private readonly record struct ScaffoldManyToManyPrincipalInfo(
            string TableKey,
            string EntityName,
            string[] PrincipalColumns,
            string[] PrincipalKeyProperties,
            bool UsesPrimaryKey);
    }
}
