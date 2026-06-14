#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedDiagnosticAdapter
    {
        public static void AddReferentialActionDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys)
        {
            foreach (var group in foreignKeys.GroupBy(
                fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}",
                StringComparer.OrdinalIgnoreCase))
            {
                var fk = group.First();
                var onDelete = NormalizeReferentialAction(fk.OnDelete);
                var onUpdate = NormalizeReferentialAction(fk.OnUpdate);
                if (TryParseReferentialAction(onDelete, out _)
                    && TryParseReferentialAction(onUpdate, out _))
                    continue;

                var rows = group.ToArray();
                var dependentKey = TableKey(fk.DependentSchema, fk.DependentTable);
                var principalKey = TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
                var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
                features.Add(new DatabaseScaffolder.ScaffoldUnsupportedFeature(
                    dependentKey,
                    "ReferentialAction",
                    fk.ConstraintName,
                    $"ON DELETE {onDelete}; ON UPDATE {onUpdate}")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["dependentTable"] = dependentKey,
                        ["dependentColumns"] = dependentColumns,
                        ["principalTable"] = principalKey,
                        ["principalColumns"] = principalColumns,
                        ["columnCount"] = rows.Length,
                        ["onDelete"] = onDelete,
                        ["onUpdate"] = onUpdate
                    }
                });
            }
        }
    }
}
