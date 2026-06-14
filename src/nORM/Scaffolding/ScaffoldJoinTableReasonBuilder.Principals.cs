#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableReasonBuilder
    {
        private static void AddPrincipalKeyReasons(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<IReadOnlyList<ScaffoldForeignKeyInfo>> constraints,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            ISet<string> reasons)
        {
            foreach (var constraint in constraints)
            {
                var first = constraint[0];
                var principalTableKey = ScaffoldForeignKeyShape.TableKey(first.PrincipalSchema, first.PrincipalTable);
                var principalColumns = foreignKeys
                    .Where(row => string.Equals(row.ConstraintName, first.ConstraintName, StringComparison.OrdinalIgnoreCase))
                    .Select(static row => row.PrincipalColumn)
                    .ToArray();
                if (!ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, principalTableKey, principalColumns)
                    && !ScaffoldForeignKeyShape.ReferencesUniqueIndex(constraint, primaryKeyColumnsByTable, indexes))
                {
                    reasons.Add("principal-key-not-scaffoldable");
                }
            }
        }
    }
}
