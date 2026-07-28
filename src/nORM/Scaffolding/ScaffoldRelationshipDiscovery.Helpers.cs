#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipDiscovery
    {
        private static bool HasNonNullableColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
               && columnNames.All(nonNullableColumns.Contains);

        private static string GetColumnPropertyName(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            string tableKey,
            string columnName)
        {
            if (columnPropertiesByTable.TryGetValue(tableKey, out var properties))
            {
                if (properties.TryGetValue(columnName, out var propertyName))
                    return propertyName;

                // SQLite's PRAGMA foreign_key_list returns the referenced ("to") column with the casing written
                // in the REFERENCES clause, which can differ from the parent column's DECLARED casing that this
                // map is keyed by (Ordinal). Column names are case-insensitively unique, so a case-insensitive
                // match resolves to the CORRECT entity property. Without it the ToPascalCase fallback below emits
                // a property name the entity does not have, and the generated DbContext fails to compile.
                foreach (var candidate in properties)
                    if (string.Equals(candidate.Key, columnName, System.StringComparison.OrdinalIgnoreCase))
                        return candidate.Value;
            }

            return ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(columnName));
        }

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
