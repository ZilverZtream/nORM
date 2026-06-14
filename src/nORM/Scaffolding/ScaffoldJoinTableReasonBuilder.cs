#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableReasonBuilder
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
    }
}
