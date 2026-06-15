#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        private readonly record struct ScaffoldObjectFilterRequest(string Identifier, string? KindSelector);

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

        private static ScaffoldObjectFilterRequest ParseObjectFilterRequest(string request)
        {
            var trimmed = request.Trim();
            var separator = trimmed.IndexOf(':');
            if (separator <= 0)
                return new ScaffoldObjectFilterRequest(trimmed, null);

            var kindSelector = NormalizeObjectKindSelector(trimmed[..separator].Trim());
            if (kindSelector is null)
                return new ScaffoldObjectFilterRequest(trimmed, null);

            return new ScaffoldObjectFilterRequest(
                trimmed[(separator + 1)..].Trim(),
                kindSelector);
        }

        private static string? NormalizeObjectKindSelector(string selector)
        {
            if (selector.Length == 0)
                return null;

            switch (selector.ToLowerInvariant())
            {
                case "table":
                case "tables":
                    return "Table";
                case "view":
                case "views":
                    return "View";
                case "query":
                case "queries":
                case "artifact":
                case "artifacts":
                case "queryartifact":
                case "queryartifacts":
                case "query-artifact":
                case "query-artifacts":
                    return "QueryArtifact";
                case "materialized-view":
                case "materialized-views":
                case "materializedview":
                case "materializedviews":
                    return "MaterializedView";
                case "virtual-table":
                case "virtual-tables":
                case "virtualtable":
                case "virtualtables":
                    return "VirtualTable";
                case "synonym":
                case "synonyms":
                    return "Synonym";
                case "routine":
                case "routines":
                    return "Routine";
                case "sequence":
                case "sequences":
                    return "Sequence";
                default:
                    return null;
            }
        }
    }
}
