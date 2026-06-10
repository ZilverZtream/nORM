#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldJoinTableMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildPossibleJoinTableMetadata(
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
            var foreignKeyColumns = GetForeignKeyColumns(foreignKeys);
            var foreignKeyColumnSet = foreignKeyColumns.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var primaryKeyColumns = GetPrimaryKeyColumns(primaryKeyColumnsByTable, tableKey);
            var databaseGeneratedColumns = GetOrderedColumns(databaseGeneratedColumnsByTable, tableKey);
            var databaseGeneratedColumnSet = databaseGeneratedColumns.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var identityColumns = GetOrderedColumns(identityColumnsByTable, tableKey);
            var identityColumnSet = identityColumns.ToHashSet(StringComparer.OrdinalIgnoreCase);

            return new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["foreignKeyConstraintCount"] = foreignKeys
                    .Select(static fk => fk.ConstraintName)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count(),
                ["foreignKeyColumns"] = foreignKeyColumns,
                ["primaryKeyColumns"] = primaryKeyColumns,
                ["payloadColumns"] = GetPayloadColumns(columnPropertiesByTable, tableKey, foreignKeyColumnSet, databaseGeneratedColumnSet, identityColumnSet),
                ["databaseGeneratedColumns"] = databaseGeneratedColumns,
                ["identityColumns"] = identityColumns,
                ["providerOwnedWriteBlockingSchema"] = providerOwnedWriteBlockedTableKeys.Contains(tableKey),
                ["nullableForeignKeyColumns"] = GetNullableForeignKeyColumns(nonNullableColumnsByTable, tableKey, foreignKeyColumns),
                ["hasExactBridgePrimaryKey"] = ScaffoldJoinTableShape.HasExactBridgePrimaryKey(primaryKeyColumns, foreignKeyColumnSet),
                ["hasGeneratedSurrogatePrimaryKey"] = ScaffoldJoinTableShape.HasGeneratedSurrogatePrimaryKey(primaryKeyColumns, foreignKeyColumnSet, databaseGeneratedColumnSet, identityColumnSet),
                ["hasExactForeignKeyUniqueIndex"] = ScaffoldForeignKeyShape.HasExactUniqueIndex(indexes, tableKey, foreignKeyColumnSet),
                ["foreignKeys"] = BuildForeignKeyConstraintMetadata(foreignKeys)
            };
        }

        private static string[] GetForeignKeyColumns(IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
            => foreignKeys
                .Select(static fk => fk.DependentColumn)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static column => column, StringComparer.Ordinal)
                .ToArray();

        private static string[] GetPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey)
            => primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKey)
                ? primaryKey.ToArray()
                : Array.Empty<string>();

        private static string[] GetOrderedColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> columnsByTable,
            string tableKey)
            => columnsByTable.TryGetValue(tableKey, out var columns)
                ? columns.OrderBy(static column => column, StringComparer.Ordinal).ToArray()
                : Array.Empty<string>();

        private static string[] GetPayloadColumns(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            string tableKey,
            IReadOnlySet<string> foreignKeyColumns,
            IReadOnlySet<string> databaseGeneratedColumns,
            IReadOnlySet<string> identityColumns)
            => columnPropertiesByTable.TryGetValue(tableKey, out var columns)
                ? columns.Keys
                    .Where(column => !foreignKeyColumns.Contains(column)
                                     && !databaseGeneratedColumns.Contains(column)
                                     && !identityColumns.Contains(column))
                    .OrderBy(static column => column, StringComparer.Ordinal)
                    .ToArray()
                : Array.Empty<string>();

        private static string[] GetNullableForeignKeyColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> foreignKeyColumns)
            => nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
                ? foreignKeyColumns
                    .Where(column => !nonNullableColumns.Contains(column))
                    .OrderBy(static column => column, StringComparer.Ordinal)
                    .ToArray()
                : Array.Empty<string>();

        private static IReadOnlyDictionary<string, object?>[] BuildForeignKeyConstraintMetadata(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
            => foreignKeys
                .GroupBy(static fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase)
                .OrderBy(static group => group.Key, StringComparer.Ordinal)
                .Select(static group =>
                {
                    var rows = group.ToArray();
                    var first = rows[0];
                    return new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["constraint"] = first.ConstraintName,
                        ["principalTable"] = ScaffoldForeignKeyShape.TableKey(first.PrincipalSchema, first.PrincipalTable),
                        ["dependentColumns"] = rows.Select(static row => row.DependentColumn).ToArray(),
                        ["principalColumns"] = rows.Select(static row => row.PrincipalColumn).ToArray(),
                        ["onDelete"] = ScaffoldForeignKeyShape.NormalizeReferentialAction(first.OnDelete),
                        ["onUpdate"] = ScaffoldForeignKeyShape.NormalizeReferentialAction(first.OnUpdate),
                        ["referentialActionScaffoldable"] = ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(rows)
                    };
                })
                .ToArray();
    }
}
