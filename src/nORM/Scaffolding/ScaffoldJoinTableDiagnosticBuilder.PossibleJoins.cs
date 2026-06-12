#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableDiagnosticBuilder
    {
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
                              || ScaffoldForeignKeyShape.HasExactUniqueColumnSet(indexes, dependentTableKey, dependentColumns.ToHashSet(StringComparer.OrdinalIgnoreCase));
                   });
        }
    }
}
