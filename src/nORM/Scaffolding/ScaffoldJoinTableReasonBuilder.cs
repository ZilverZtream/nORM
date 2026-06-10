#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldJoinTableReasonBuilder
    {
        public static string[] BuildPossibleJoinTableReasons(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
        {
            var reasons = new HashSet<string>(StringComparer.Ordinal);
            var foreignKeyColumns = foreignKeys
                .Select(static fk => fk.DependentColumn)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var constraints = foreignKeys
                .GroupBy(static fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase)
                .Select(static group => group.ToArray())
                .ToArray();
            var databaseGeneratedColumns = ScaffoldJoinTableShape.GetColumnSet(databaseGeneratedColumnsByTable, tableKey);
            var identityColumns = ScaffoldJoinTableShape.GetColumnSet(identityColumnsByTable, tableKey);

            AddProviderOwnedReason(tableKey, providerOwnedWriteBlockedTableKeys, reasons);
            AddForeignKeyShapeReasons(constraints, reasons);
            AddPayloadColumnReason(tableKey, columnPropertiesByTable, foreignKeyColumns, databaseGeneratedColumns, identityColumns, reasons);
            AddPrimaryKeyShapeReason(tableKey, primaryKeyColumnsByTable, foreignKeyColumns, databaseGeneratedColumns, identityColumns, indexes, reasons);
            AddNullableForeignKeyReason(tableKey, nonNullableColumnsByTable, foreignKeyColumns, reasons);
            AddPrincipalKeyReasons(foreignKeys, constraints, primaryKeyColumnsByTable, indexes, reasons);

            if (reasons.Count == 0)
                reasons.Add("review-shape");

            return reasons.OrderBy(static reason => reason, StringComparer.Ordinal).ToArray();
        }

        private static void AddProviderOwnedReason(
            string tableKey,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            ISet<string> reasons)
        {
            if (providerOwnedWriteBlockedTableKeys.Contains(tableKey))
                reasons.Add("provider-owned-write-blocking-schema");
        }

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
                        && !ScaffoldForeignKeyShape.HasExactUniqueIndex(indexes, tableKey, foreignKeyColumns)
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

        private static void AddPrincipalKeyReasons(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<IReadOnlyList<ScaffoldForeignKeyInfo>> constraints,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            ISet<string> reasons)
        {
            foreach (var constraint in constraints)
            {
                var first = constraint[0];
                var principalTableKey = ScaffoldForeignKeyShape.TableKey(first.PrincipalSchema, first.PrincipalTable);
                var principalColumns = foreignKeys
                    .Where(row => string.Equals(row.ConstraintName, first.ConstraintName, StringComparison.OrdinalIgnoreCase))
                    .Select(static row => row.PrincipalColumn)
                    .ToArray();
                if (!ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, principalTableKey, principalColumns)
                    && !ScaffoldForeignKeyShape.ReferencesUniqueIndex(constraint, primaryKeyColumnsByTable, indexes))
                {
                    reasons.Add("principal-key-not-scaffoldable");
                }
            }
        }
    }
}
