#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private static void AddSqliteForeignKeyRows(
            ScaffoldTableInfo table,
            IReadOnlyList<SqliteForeignKeyRow> rows,
            IReadOnlyDictionary<string, string> providerSemanticsByColumns,
            IReadOnlyDictionary<string, string> constraintNamesByColumns,
            ICollection<ScaffoldForeignKeyInfo> foreignKeys)
        {
            var providerSemantics = TryGetSqliteProviderSemantics(rows, providerSemanticsByColumns);
            var columnKey = ScaffoldSqliteDdlParser.BuildForeignKeyColumnKey(rows.Select(static row => row.DependentColumn));
            var hasDeclaredConstraintName = constraintNamesByColumns.TryGetValue(columnKey, out var declaredConstraintName)
                                            && !string.IsNullOrWhiteSpace(declaredConstraintName);
            foreach (var row in rows)
            {
                if (string.IsNullOrWhiteSpace(row.PrincipalTable)
                    || string.IsNullOrWhiteSpace(row.DependentColumn)
                    || string.IsNullOrWhiteSpace(row.PrincipalColumn))
                {
                    continue;
                }

                foreignKeys.Add(new ScaffoldForeignKeyInfo(
                    DependentSchema: table.Schema,
                    DependentTable: table.Name,
                    DependentColumn: row.DependentColumn,
                    PrincipalSchema: table.Schema,
                    PrincipalTable: row.PrincipalTable,
                    PrincipalColumn: row.PrincipalColumn,
                    ConstraintName: hasDeclaredConstraintName ? declaredConstraintName! : "sqlite_fk_" + row.Id,
                    ColumnCount: rows.Count,
                    OnDelete: ScaffoldReferentialAction.Normalize(row.OnDelete),
                    OnUpdate: NormalizeSqliteReferentialAction(row.OnUpdate, row.Match, providerSemantics),
                    IsSyntheticConstraintName: !hasDeclaredConstraintName));
            }
        }
    }
}
