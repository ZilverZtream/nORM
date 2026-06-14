#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableReasonBuilder
    {
        private static void AddForeignKeyShapeReasons(
            IReadOnlyList<IReadOnlyList<ScaffoldForeignKeyInfo>> constraints,
            ISet<string> reasons)
        {
            if (constraints.Count != 2)
                reasons.Add("not-two-foreign-keys");
            if (constraints.Any(static group => group.Count == 0 || group.Any(fk => fk.ColumnCount != group.Count)))
                reasons.Add("foreign-key-metadata-incomplete");
            if (constraints.Any(static group => !ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(group)))
                reasons.Add("referential-action-not-scaffoldable");
        }

        private static void AddPayloadColumnReason(
            string tableKey,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlySet<string> foreignKeyColumns,
            IReadOnlySet<string> databaseGeneratedColumns,
            IReadOnlySet<string> identityColumns,
            ISet<string> reasons)
        {
            if (columnPropertiesByTable.TryGetValue(tableKey, out var columns)
                && columns.Keys.Any(column => !foreignKeyColumns.Contains(column)
                                             && !databaseGeneratedColumns.Contains(column)
                                             && !identityColumns.Contains(column)))
            {
                reasons.Add("payload-columns");
            }
        }

        private static void AddPrimaryKeyShapeReason(
            string tableKey,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlySet<string> foreignKeyColumns,
            IReadOnlySet<string> databaseGeneratedColumns,
            IReadOnlySet<string> identityColumns,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            ISet<string> reasons)
        {
            if (!primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
                || primaryKeyColumns.Count == 0)
            {
                reasons.Add("missing-primary-key");
                return;
            }

            if (ScaffoldJoinTableShape.HasExactBridgePrimaryKey(primaryKeyColumns, foreignKeyColumns))
                return;

            reasons.Add(ScaffoldJoinTableShape.HasGeneratedSurrogatePrimaryKey(primaryKeyColumns, foreignKeyColumns, databaseGeneratedColumns, identityColumns)
                        && !ScaffoldForeignKeyShape.HasExactUniqueColumnSet(indexes, tableKey, foreignKeyColumns)
                ? "missing-exact-unique-index"
                : "primary-key-not-exact-bridge-columns");
        }

        private static void AddNullableForeignKeyReason(
            string tableKey,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> foreignKeyColumns,
            ISet<string> reasons)
        {
            if (nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
                && foreignKeyColumns.Any(column => !nonNullableColumns.Contains(column)))
            {
                reasons.Add("nullable-foreign-key");
            }
        }
    }
}
