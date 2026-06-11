#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldManyToManyJoinDiscovery
    {
        public static IReadOnlyList<ScaffoldManyToManyJoinInfo> BuildManyToManyJoins(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var tablesByKey = tables.ToDictionary(t => TableKey(t.Schema, t.Name), StringComparer.OrdinalIgnoreCase);
            var joins = new List<ScaffoldManyToManyJoinInfo>();

            foreach (var group in foreignKeys
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase))
            {
                if (TryBuildManyToManyJoin(
                    group,
                    tablesByKey,
                    entityByTable,
                    columnPropertiesByTable,
                    primaryKeyColumnsByTable,
                    identityColumnsByTable,
                    databaseGeneratedColumnsByTable,
                    indexes,
                    nonNullableColumnsByTable,
                    providerOwnedWriteBlockedTableKeys,
                    memberNamesByTable,
                    out var join))
                {
                    joins.Add(join);
                }
            }

            return joins;
        }

        private static bool TryBuildManyToManyJoin(
            IGrouping<string, ScaffoldForeignKeyInfo> group,
            IReadOnlyDictionary<string, ScaffoldTableInfo> tablesByKey,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            Dictionary<string, HashSet<string>> memberNamesByTable,
            out ScaffoldManyToManyJoinInfo join)
        {
            join = default;
            var joinTableKey = group.Key;
            if (!tablesByKey.TryGetValue(joinTableKey, out var joinTable))
                return false;

            var canonicalJoinTableKey = TableKey(joinTable.Schema, joinTable.Name);
            if (providerOwnedWriteBlockedTableKeys.Contains(canonicalJoinTableKey))
                return false;

            if (!TryGetManyToManyForeignKeyGroups(joinTableKey, joinTable.Name, group, primaryKeyColumnsByTable, indexes, out var fkGroups))
                return false;

            if (!HasScaffoldableManyToManyBridgeShape(
                joinTableKey,
                fkGroups,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                identityColumnsByTable,
                databaseGeneratedColumnsByTable,
                indexes,
                nonNullableColumnsByTable))
            {
                return false;
            }

            var leftGroup = fkGroups[0];
            var rightGroup = fkGroups[1];
            if (!TryGetManyToManyPrincipalInfo(leftGroup, entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, indexes, out var leftPrincipal)
                || !TryGetManyToManyPrincipalInfo(rightGroup, entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, indexes, out var rightPrincipal))
            {
                return false;
            }

            var (leftCollectionName, rightCollectionName) = BuildManyToManyCollectionNames(
                joinTableKey,
                leftGroup[0],
                rightGroup[0],
                leftPrincipal,
                rightPrincipal,
                columnPropertiesByTable,
                memberNamesByTable);

            join = new ScaffoldManyToManyJoinInfo(
                canonicalJoinTableKey,
                leftPrincipal.TableKey,
                rightPrincipal.TableKey,
                joinTable.Name,
                joinTable.Schema,
                leftPrincipal.EntityName,
                rightPrincipal.EntityName,
                leftGroup.Select(static fk => fk.DependentColumn).ToArray(),
                rightGroup.Select(static fk => fk.DependentColumn).ToArray(),
                leftPrincipal.PrincipalKeyProperties,
                rightPrincipal.PrincipalKeyProperties,
                ScaffoldForeignKeyShape.NormalizeReferentialAction(leftGroup[0].OnDelete),
                ScaffoldForeignKeyShape.NormalizeReferentialAction(leftGroup[0].OnUpdate),
                ScaffoldForeignKeyShape.NormalizeReferentialAction(rightGroup[0].OnDelete),
                ScaffoldForeignKeyShape.NormalizeReferentialAction(rightGroup[0].OnUpdate),
                leftPrincipal.UsesPrimaryKey && rightPrincipal.UsesPrimaryKey,
                leftCollectionName,
                rightCollectionName);
            return true;
        }

        private static bool TryGetManyToManyForeignKeyGroups(
            string joinTableKey,
            string joinTableName,
            IEnumerable<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            out ScaffoldForeignKeyInfo[][] fkGroups)
        {
            fkGroups = OrderManyToManyForeignKeyGroups(
                joinTableName,
                foreignKeys
                    .GroupBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.ToArray())
                    .ToArray());

            return fkGroups.Length == 2
                   && fkGroups.All(rows => rows.Length > 0 && rows.All(row => row.ColumnCount == rows.Length))
                   && !ScaffoldForeignKeyShape.AllForeignKeyGroupsAreUniqueDependentKeys(joinTableKey, fkGroups, primaryKeyColumnsByTable, indexes)
                   && fkGroups.All(ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions);
        }

        private static bool HasScaffoldableManyToManyBridgeShape(
            string joinTableKey,
            ScaffoldForeignKeyInfo[][] fkGroups,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
        {
            if (!columnPropertiesByTable.TryGetValue(joinTableKey, out var joinColumns)
                || !primaryKeyColumnsByTable.TryGetValue(joinTableKey, out var joinPrimaryKeyColumns))
            {
                return false;
            }

            var fkColumnNames = fkGroups
                .SelectMany(static rows => rows.Select(static fk => fk.DependentColumn))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var hasExactBridgePrimaryKey = ScaffoldJoinTableShape.HasExactBridgePrimaryKey(joinPrimaryKeyColumns, fkColumnNames);
            var databaseGeneratedColumns = ScaffoldJoinTableShape.GetColumnSet(databaseGeneratedColumnsByTable, joinTableKey);
            var payloadColumns = joinColumns.Keys
                .Where(column => !fkColumnNames.Contains(column) && !databaseGeneratedColumns.Contains(column))
                .ToArray();
            var hasGeneratedSurrogatePrimaryKey = payloadColumns.Length == 1
                && joinPrimaryKeyColumns.Count == 1
                && string.Equals(joinPrimaryKeyColumns[0], payloadColumns[0], StringComparison.OrdinalIgnoreCase)
                && identityColumnsByTable.TryGetValue(joinTableKey, out var identityColumns)
                && identityColumns.Contains(payloadColumns[0])
                && ScaffoldForeignKeyShape.HasExactUniqueIndex(indexes, joinTableKey, fkColumnNames);

            if (payloadColumns.Length > 0 && !hasGeneratedSurrogatePrimaryKey)
                return false;

            if (!hasExactBridgePrimaryKey && !hasGeneratedSurrogatePrimaryKey)
                return false;

            return nonNullableColumnsByTable.TryGetValue(joinTableKey, out var nonNullableColumns)
                   && fkColumnNames.All(column => nonNullableColumns.Contains(column));
        }

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

        private static ScaffoldForeignKeyInfo[][] OrderManyToManyForeignKeyGroups(string joinTableName, ScaffoldForeignKeyInfo[][] foreignKeyGroups)
            => foreignKeyGroups
                .OrderBy(group => PrincipalNamePosition(joinTableName, group[0].PrincipalTable))
                .ThenBy(group => PrincipalNamePosition(joinTableName, TrimIdSuffix(group[0].DependentColumn)))
                .ThenBy(group => group[0].DependentColumn, StringComparer.Ordinal)
                .ToArray();

        private static int PrincipalNamePosition(string joinTableName, string principalTable)
        {
            var position = joinTableName.IndexOf(principalTable, StringComparison.OrdinalIgnoreCase);
            return position < 0 ? int.MaxValue : position;
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

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);

        private static string TrimIdSuffix(string name)
        {
            if (name.EndsWith("Id", StringComparison.Ordinal) && name.Length > 2)
                return name[..^2];

            if (name.EndsWith("_id", StringComparison.OrdinalIgnoreCase) && name.Length > 3)
                return name[..^3];

            return name;
        }

        private readonly record struct ScaffoldManyToManyPrincipalInfo(
            string TableKey,
            string EntityName,
            string[] PrincipalColumns,
            string[] PrincipalKeyProperties,
            bool UsesPrimaryKey);
    }
}
