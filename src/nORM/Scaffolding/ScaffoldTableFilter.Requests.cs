#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        private readonly record struct ScaffoldObjectFilterRequest(string Identifier, string? KindSelector, bool LiteralNameOnly);

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
                return new ScaffoldObjectFilterRequest(trimmed, null, LiteralNameOnly: false);

            var selector = trimmed[..separator].Trim();
            var identifier = trimmed[(separator + 1)..].Trim();
            if (IsLiteralNameSelector(selector))
                return new ScaffoldObjectFilterRequest(identifier, null, LiteralNameOnly: true);

            var kindSelector = NormalizeObjectKindSelector(selector);
            if (kindSelector is null)
                return new ScaffoldObjectFilterRequest(trimmed, null, LiteralNameOnly: false);

            if (TryStripLiteralNameSelector(identifier, out var literalIdentifier))
                return new ScaffoldObjectFilterRequest(literalIdentifier, kindSelector, LiteralNameOnly: true);

            return new ScaffoldObjectFilterRequest(
                identifier,
                kindSelector,
                LiteralNameOnly: false);
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

        private static bool TryStripLiteralNameSelector(string identifier, out string literalIdentifier)
        {
            var separator = identifier.IndexOf(':');
            if (separator > 0 && IsLiteralNameSelector(identifier[..separator].Trim()))
            {
                literalIdentifier = identifier[(separator + 1)..].Trim();
                return true;
            }

            literalIdentifier = identifier;
            return false;
        }

        private static bool IsLiteralNameSelector(string selector)
        {
            switch (selector.ToLowerInvariant())
            {
                case "name":
                case "names":
                case "literal":
                case "literals":
                case "literal-name":
                case "literal-names":
                case "literalname":
                case "literalnames":
                case "object-name":
                case "object-names":
                case "objectname":
                case "objectnames":
                    return true;
                default:
                    return false;
            }
        }
    }
}
