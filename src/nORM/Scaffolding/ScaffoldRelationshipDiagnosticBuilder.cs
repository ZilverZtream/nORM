#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldRelationshipDiagnosticBuilder
    {
        public static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> BuildPrincipalKeyDiagnostics(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var diagnostics = new List<ScaffoldUnsupportedFeatureInfo>();
            foreach (var group in foreignKeys
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase))
            {
                var rows = group.ToArray();
                if (ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(rows, primaryKeyColumnsByTable, indexes))
                    continue;

                var fk = rows[0];
                var dependentKey = ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable);
                var principalKey = ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
                var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
                diagnostics.Add(new ScaffoldUnsupportedFeatureInfo(
                    dependentKey,
                    "RelationshipPrincipalKey",
                    fk.ConstraintName,
                    $"FK references {principalKey}.({string.Join(", ", principalColumns)}), which is neither the generated principal primary key nor an exact ordered unfiltered unique index.")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["dependentTable"] = dependentKey,
                        ["dependentColumns"] = dependentColumns,
                        ["principalTable"] = principalKey,
                        ["principalColumns"] = principalColumns,
                        ["columnCount"] = rows.Length
                    }
                });
            }

            return diagnostics;
        }

        public static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> BuildDependentKeyDiagnostics(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
        {
            var diagnostics = new List<ScaffoldUnsupportedFeatureInfo>();
            foreach (var group in foreignKeys
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase))
            {
                var rows = group.ToArray();
                var fk = rows[0];
                var dependentKey = ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable);
                if (primaryKeyColumnsByTable.TryGetValue(dependentKey, out var primaryKeyColumns)
                    && primaryKeyColumns.Count > 0)
                {
                    continue;
                }

                var principalKey = ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
                var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
                diagnostics.Add(new ScaffoldUnsupportedFeatureInfo(
                    dependentKey,
                    "RelationshipDependentKey",
                    fk.ConstraintName,
                    $"FK dependent table {dependentKey} has no primary key; generated navigations are suppressed because nORM cannot track or include the dependent side safely.")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["dependentTable"] = dependentKey,
                        ["dependentColumns"] = dependentColumns,
                        ["principalTable"] = principalKey,
                        ["principalColumns"] = principalColumns,
                        ["columnCount"] = rows.Length
                    }
                });
            }

            return diagnostics;
        }
    }
}
