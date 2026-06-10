#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldCompositeForeignKeyMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
        {
            if (rows.Count == 0)
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var first = rows[0];
            var dependentTable = ScaffoldForeignKeyShape.TableKey(first.DependentSchema, first.DependentTable);
            var principalTable = ScaffoldForeignKeyShape.TableKey(first.PrincipalSchema, first.PrincipalTable);
            var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
            var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["relationshipSuppressed"] = true,
                ["reason"] = "principal-key-not-scaffoldable",
                ["columnCount"] = rows.Count,
                ["dependentTable"] = dependentTable,
                ["principalTable"] = principalTable,
                ["dependentColumns"] = dependentColumns,
                ["principalColumns"] = principalColumns,
                ["referencesPrimaryKey"] = ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, principalTable, principalColumns),
                ["referencesScaffoldableUniqueIndex"] = ScaffoldForeignKeyShape.ReferencesUniqueIndex(rows, primaryKeyColumnsByTable, indexes)
            };

            if (primaryKeyColumnsByTable.TryGetValue(principalTable, out var principalPrimaryKeyColumns))
                metadata["principalPrimaryKeyColumns"] = principalPrimaryKeyColumns.ToArray();

            return metadata;
        }
    }
}
