#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableMetadataBuilder
    {
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
                        ["declaredColumnCount"] = first.ColumnCount,
                        ["metadataRowCount"] = rows.Length,
                        ["metadataComplete"] = IsForeignKeyMetadataComplete(rows),
                        ["dependentColumns"] = rows.Select(static row => row.DependentColumn).ToArray(),
                        ["principalColumns"] = rows.Select(static row => row.PrincipalColumn).ToArray(),
                        ["onDelete"] = ScaffoldForeignKeyShape.NormalizeReferentialAction(first.OnDelete),
                        ["onUpdate"] = ScaffoldForeignKeyShape.NormalizeReferentialAction(first.OnUpdate),
                        ["referentialActionScaffoldable"] = ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(rows)
                    };
                })
                .ToArray();

        private static bool IsForeignKeyMetadataComplete(IReadOnlyList<ScaffoldForeignKeyInfo> rows)
            => rows.Count > 0
               && rows[0].ColumnCount == rows.Count
               && rows.All(row => row.ColumnCount == rows.Count);
    }
}
