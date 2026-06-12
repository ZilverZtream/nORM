#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldManyToManyJoinDiscovery
    {
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
                && ScaffoldForeignKeyShape.HasExactUniqueColumnSet(indexes, joinTableKey, fkColumnNames);

            if (payloadColumns.Length > 0 && !hasGeneratedSurrogatePrimaryKey)
                return false;

            if (!hasExactBridgePrimaryKey && !hasGeneratedSurrogatePrimaryKey)
                return false;

            return nonNullableColumnsByTable.TryGetValue(joinTableKey, out var nonNullableColumns)
                   && fkColumnNames.All(column => nonNullableColumns.Contains(column));
        }

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
