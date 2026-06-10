#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldJoinTableDiagnosticBuilder
    {
        public static ScaffoldCompositeForeignKeyDiagnosticInfo[] BuildCompositeForeignKeyDiagnostics(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
        {
            return foreignKeys
                .Where(static fk => fk.ColumnCount > 1)
                .GroupBy(
                    static fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}",
                    StringComparer.OrdinalIgnoreCase)
                .Where(g => !ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(g.ToArray(), primaryKeyColumnsByTable, indexes))
                .OrderBy(g => g.First().DependentSchema, StringComparer.Ordinal)
                .ThenBy(g => g.First().DependentTable, StringComparer.Ordinal)
                .ThenBy(g => g.First().ConstraintName, StringComparer.Ordinal)
                .Select(g => BuildCompositeForeignKeyDiagnostic(g.ToArray(), primaryKeyColumnsByTable, indexes, nonNullableColumnsByTable))
                .ToArray();
        }

        public static ScaffoldPossibleJoinTableDiagnosticInfo[] BuildPossibleJoinTableDiagnostics(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys)
        {
            return foreignKeys
                .GroupBy(fk => ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase)
                .Where(g => !AllForeignKeyGroupsAreUniqueDependentKeys(
                    g.Key,
                    g.GroupBy(static fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase).Select(static group => group.ToArray()),
                    primaryKeyColumnsByTable,
                    indexes))
                .Select(g => BuildPossibleJoinTableDiagnostic(
                    g.Key,
                    g.ToArray(),
                    primaryKeyColumnsByTable,
                    columnPropertiesByTable,
                    nonNullableColumnsByTable,
                    databaseGeneratedColumnsByTable,
                    identityColumnsByTable,
                    indexes,
                    providerOwnedWriteBlockedTableKeys))
                .Where(static g => g.ConstraintNames.Length >= 2 && g.PrincipalTables.Length is 1 or 2)
                .Where(g => emittedManyToManyJoinTableKeys is null || !emittedManyToManyJoinTableKeys.Contains(g.TableKey))
                .OrderBy(static g => g.TableKey, StringComparer.Ordinal)
                .ToArray();
        }

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
            => ScaffoldJoinTableMetadataBuilder.BuildPossibleJoinTableMetadata(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        public static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldCompositeForeignKeyMetadataBuilder.BuildCompositeForeignKeyMetadata(
                rows,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable);

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
            => ScaffoldJoinTableReasonBuilder.BuildPossibleJoinTableReasons(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        private static ScaffoldCompositeForeignKeyDiagnosticInfo BuildCompositeForeignKeyDiagnostic(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
        {
            var first = rows[0];
            return new ScaffoldCompositeForeignKeyDiagnosticInfo(
                first.ConstraintName,
                ScaffoldForeignKeyShape.TableKey(first.DependentSchema, first.DependentTable),
                rows.Select(static r => r.DependentColumn).ToArray(),
                ScaffoldForeignKeyShape.TableKey(first.PrincipalSchema, first.PrincipalTable),
                rows.Select(static r => r.PrincipalColumn).ToArray(),
                BuildCompositeForeignKeyMetadata(rows, primaryKeyColumnsByTable, indexes, nonNullableColumnsByTable));
        }

        private static ScaffoldPossibleJoinTableDiagnosticInfo BuildPossibleJoinTableDiagnostic(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
        {
            var principalTables = rows
                .Select(static fk => ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static key => key, StringComparer.Ordinal)
                .ToArray();
            var constraintNames = rows
                .Select(static fk => fk.ConstraintName)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static name => name, StringComparer.Ordinal)
                .ToArray();
            var reasons = BuildPossibleJoinTableReasons(
                tableKey,
                rows,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);
            var metadata = BuildPossibleJoinTableMetadata(
                tableKey,
                rows,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);
            return new ScaffoldPossibleJoinTableDiagnosticInfo(tableKey, principalTables, constraintNames, reasons, metadata);
        }

        private static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<ScaffoldForeignKeyInfo>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var groups = foreignKeyGroups
                .Select(static group => group.ToArray())
                .Where(static group => group.Length > 0)
                .ToArray();

            return groups.Length >= 2
                   && groups.All(group =>
                   {
                       var dependentColumns = group.Select(static row => row.DependentColumn).ToArray();
                       return ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, dependentTableKey, dependentColumns)
                              || ScaffoldForeignKeyShape.HasExactUniqueIndex(indexes, dependentTableKey, dependentColumns.ToHashSet(StringComparer.OrdinalIgnoreCase));
                   });
        }
    }
}
