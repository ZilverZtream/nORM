#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipDiscovery
    {
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
            if (!TryGetRelationshipEntities(entityByTable, dependentKey, principalKey, out var dependentEntity, out var principalEntity)
                || !CanScaffoldRelationship(rows, dependentKey, primaryKeyColumnsByTable, indexes))
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
                || ScaffoldForeignKeyShape.HasExactUniqueColumnSet(indexes, dependentKey, dependentColumns.ToHashSet(StringComparer.OrdinalIgnoreCase));
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

        private static bool TryGetRelationshipEntities(
            IReadOnlyDictionary<string, string> entityByTable,
            string dependentKey,
            string principalKey,
            out string dependentEntity,
            out string principalEntity)
        {
            dependentEntity = string.Empty;
            principalEntity = string.Empty;
            if (!entityByTable.TryGetValue(dependentKey, out var dependent)
                || !entityByTable.TryGetValue(principalKey, out var principal))
            {
                return false;
            }

            dependentEntity = dependent;
            principalEntity = principal;
            return true;
        }

        private static bool CanScaffoldRelationship(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            string dependentKey,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes)
            => ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(rows, primaryKeyColumnsByTable, indexes)
               && primaryKeyColumnsByTable.TryGetValue(dependentKey, out var dependentPrimaryKeyColumns)
               && dependentPrimaryKeyColumns.Count > 0
               && ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(rows);
    }
}
