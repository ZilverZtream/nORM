#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldManyToManyJoinDiscovery
    {
        public static IReadOnlyList<ScaffoldManyToManyJoinInfo> BuildManyToManyJoins(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var tablesByKey = tables.ToDictionary(t => TableKey(t.Schema, t.Name), StringComparer.OrdinalIgnoreCase);
            // Any table that is the principal (target) of some foreign key is depended upon by another
            // table. Collapsing such a table into an implicit many-to-many join would delete the entity
            // that dependency needs and leave a navigation pointing at a type that was never generated,
            // so those tables must remain first-class entities.
            var referencedPrincipalTableKeys = foreignKeys
                .Select(fk => TableKey(fk.PrincipalSchema, fk.PrincipalTable))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var joins = new List<ScaffoldManyToManyJoinInfo>();

            foreach (var group in foreignKeys
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase))
            {
                if (TryBuildManyToManyJoin(
                    group,
                    tablesByKey,
                    entityByTable,
                    columnPropertiesByTable,
                    primaryKeyColumnsByTable,
                    identityColumnsByTable,
                    databaseGeneratedColumnsByTable,
                    indexes,
                    nonNullableColumnsByTable,
                    providerOwnedWriteBlockedTableKeys,
                    referencedPrincipalTableKeys,
                    memberNamesByTable,
                    out var join))
                {
                    joins.Add(join);
                }
            }

            return joins;
        }

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
