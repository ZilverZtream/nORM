#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldRelationshipDiscovery
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

        private static IReadOnlyDictionary<string, int> BuildRelationshipPairCounts(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
            => foreignKeys
                .GroupBy(
                    static fk => $"{ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable)}\u001f{ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable)}\u001f{fk.ConstraintName}",
                    StringComparer.OrdinalIgnoreCase)
                .Select(static g => g.First())
                .GroupBy(
                    static fk => ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable) + "\u001f" + ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable),
                    StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static g => g.Key, static g => g.Count(), StringComparer.OrdinalIgnoreCase);

        private static bool TryBuildRelationship(
            IGrouping<string, ScaffoldForeignKeyInfo> foreignKeyGroup,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, int> relationshipPairCounts,
            Dictionary<string, HashSet<string>> memberNamesByTable,
            out ScaffoldRelationshipInfo relationship)
        {
            relationship = default;
            var rows = foreignKeyGroup.ToArray();
            var foreignKey = rows[0];
            var dependentKey = ScaffoldForeignKeyShape.TableKey(foreignKey.DependentSchema, foreignKey.DependentTable);
            var principalKey = ScaffoldForeignKeyShape.TableKey(foreignKey.PrincipalSchema, foreignKey.PrincipalTable);
            if (!entityByTable.TryGetValue(dependentKey, out var dependentEntity)
                || !entityByTable.TryGetValue(principalKey, out var principalEntity)
                || !ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(rows, primaryKeyColumnsByTable, indexes)
                || !primaryKeyColumnsByTable.TryGetValue(dependentKey, out var dependentPrimaryKeyColumns)
                || dependentPrimaryKeyColumns.Count == 0
                || !ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(rows))
            {
                return false;
            }

            var foreignKeyProperties = rows
                .Select(row => GetColumnPropertyName(columnPropertiesByTable, dependentKey, row.DependentColumn))
                .ToArray();
            var principalKeyProperties = rows
                .Select(row => GetColumnPropertyName(columnPropertiesByTable, principalKey, row.PrincipalColumn))
                .ToArray();
            var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
            var isRequired = HasNonNullableColumns(nonNullableColumnsByTable, dependentKey, dependentColumns);
            var isUniqueDependentKey = ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, dependentKey, dependentColumns)
                || ScaffoldForeignKeyShape.HasExactUniqueIndex(indexes, dependentKey, dependentColumns.ToHashSet(StringComparer.OrdinalIgnoreCase));
            var (referenceName, collectionName) = ScaffoldRelationshipNavigationNameBuilder.BuildNavigationNames(
                dependentKey,
                principalKey,
                dependentEntity,
                principalEntity,
                foreignKeyProperties,
                principalKeyProperties,
                isUniqueDependentKey,
                relationshipPairCounts,
                memberNamesByTable);

            relationship = new ScaffoldRelationshipInfo(
                dependentKey,
                principalKey,
                dependentEntity,
                principalEntity,
                foreignKeyProperties[0],
                principalKeyProperties[0],
                referenceName,
                collectionName,
                isUniqueDependentKey,
                string.Equals(foreignKey.OnDelete, "CASCADE", StringComparison.OrdinalIgnoreCase),
                ScaffoldForeignKeyShape.NormalizeReferentialAction(foreignKey.OnDelete),
                ScaffoldForeignKeyShape.NormalizeReferentialAction(foreignKey.OnUpdate),
                foreignKey.IsSyntheticConstraintName ? null : NullIfWhiteSpace(foreignKey.ConstraintName))
            {
                IsRequired = isRequired,
                ForeignKeyPropertyNames = foreignKeyProperties,
                PrincipalKeyPropertyNames = principalKeyProperties
            };
            return true;
        }

        private static bool HasNonNullableColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
               && columnNames.All(nonNullableColumns.Contains);

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

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
