#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipDiscovery
    {
        private static IReadOnlyDictionary<string, int> BuildRelationshipPairCounts(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
            => foreignKeys
                .GroupBy(
                    static fk => $"{ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable)}\u001f{ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable)}\u001f{fk.ConstraintName}",
                    StringComparer.OrdinalIgnoreCase)
                .Select(static g => g.First())
                .GroupBy(
                    static fk => ScaffoldForeignKeyShape.TableKey(fk.DependentSchema, fk.DependentTable) + "\u001f" + ScaffoldForeignKeyShape.TableKey(fk.PrincipalSchema, fk.PrincipalTable),
                    StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static g => g.Key, static g => g.Count(), StringComparer.OrdinalIgnoreCase);
    }
}
