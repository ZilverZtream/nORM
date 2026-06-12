#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldEntityNameBuilder
    {
        public static IReadOnlyDictionary<string, string> BuildEntityNameMap(
            IReadOnlyList<ScaffoldTableInfo> tables,
            bool useDatabaseNames,
            bool usePluralizer = true)
        {
            var names = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var existingNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var duplicateTableNames = tables
                .GroupBy(table => table.Name, StringComparer.OrdinalIgnoreCase)
                .Where(group => group.Count() > 1)
                .Select(group => group.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var table in tables)
            {
                var sourceName = duplicateTableNames.Contains(table.Name)
                    ? string.IsNullOrWhiteSpace(table.Schema)
                        ? "Default_" + table.Name
                        : table.Schema + "_" + table.Name
                    : table.Name;
                var clrName = ScaffoldNameHelper.ToScaffoldClrName(sourceName, useDatabaseNames);
                var baseName = usePluralizer && !useDatabaseNames
                    ? ScaffoldNameHelper.Singularize(clrName)
                    : clrName;
                names[TableKey(table.Schema, table.Name)] = ScaffoldNameHelper.MakeUnique(baseName, existingNames);
            }

            return names;
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";
    }
}
