#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        public static void EnsureNoTableKeyCollisions(IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var collisions = tables
                .GroupBy(table => TableKey(table.Schema, table.Name), StringComparer.OrdinalIgnoreCase)
                .Select(group => new
                {
                    DisplayKey = group.Key,
                    Matches = group
                        .GroupBy(table => table.Kind + "\u001f" + (table.Schema ?? string.Empty) + "\u001f" + table.Name, StringComparer.OrdinalIgnoreCase)
                        .Select(inner => DisplaySelectableTableMatch(inner.First()))
                        .OrderBy(value => value, StringComparer.Ordinal)
                        .ToArray()
                })
                .Where(group => group.Matches.Length > 1)
                .ToArray();

            if (collisions.Length == 0)
                return;

            throw new NormConfigurationException(
                "Scaffolding discovered database objects whose display names collide with schema-qualified names: " +
                string.Join("; ", collisions.Select(c => $"{c.DisplayKey} matched {string.Join(", ", c.Matches)}")) +
                ". Rename one object or scaffold a provider-specific model manually; v1 table filters cannot disambiguate literal dotted table names from schema-qualified table names, and same-schema object-kind collisions must be scaffolded in separate runs.");
        }

        private static string DisplaySelectableTableMatch(ScaffoldTableInfo table)
            => $"{table.Kind} {DisplayTableMatch(table)}";
    }
}
