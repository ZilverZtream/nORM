#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        public static string[] GetRequestedTableFilters(ScaffoldOptions options)
        {
            if (options.Tables is null)
                return Array.Empty<string>();

            return options.Tables
                .Where(table => !string.IsNullOrWhiteSpace(table))
                .Select(table => table.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        public static string[] GetRequestedSchemaFilters(ScaffoldOptions options)
        {
            if (options.Schemas is null)
                return Array.Empty<string>();

            return options.Schemas
                .Where(schema => !string.IsNullOrWhiteSpace(schema))
                .Select(schema => schema.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
    }
}
