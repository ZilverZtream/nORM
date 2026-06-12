#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableDiagnosticBuilder
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
    }
}
