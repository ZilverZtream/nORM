#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldManyToManyJoinDiscovery
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

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
