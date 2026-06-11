#nullable enable
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        public static void AppendMarkdown(
            StringBuilder sb,
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
        {
            sb.AppendLine("# nORM Scaffold Warnings");
            sb.AppendLine();
            sb.AppendLine("The scaffolder detected database features that were not converted into runnable nORM model code.");
            sb.AppendLine("Review these items before using the generated model for migrations or navigation queries.");

            AppendCompositeForeignKeys(sb, compositeForeignKeys);
            AppendPossibleJoinTables(sb, possibleJoinTables);
            AppendUnsupportedFeatures(sb, unsupportedFeatures);
            AppendSkippedObjects(sb, skippedObjects);
        }

        public static string WriteJson(
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
        {
            var compositeForeignKeyItems = BuildCompositeForeignKeyJsonItems(compositeForeignKeys);
            var possibleJoinTableItems = BuildPossibleJoinTableJsonItems(possibleJoinTables);
            var providerOwnedSchemaFeatures = BuildProviderOwnedSchemaFeatureJsonItems(unsupportedFeatures);
            var skippedDatabaseObjects = BuildSkippedDatabaseObjectJsonItems(skippedObjects);

            var diagnosticCodes = compositeForeignKeyItems
                .Select(item => item.code)
                .Concat(possibleJoinTableItems.Select(item => item.code))
                .Concat(providerOwnedSchemaFeatures.Select(item => item.code))
                .Concat(skippedDatabaseObjects.Select(item => item.code))
                .CountByOrdinalValue();

            var diagnosticCategories = compositeForeignKeyItems
                .Select(item => item.category)
                .Concat(possibleJoinTableItems.Select(item => item.category))
                .Concat(providerOwnedSchemaFeatures.Select(item => item.category))
                .Concat(skippedDatabaseObjects.Select(item => item.category))
                .CountByOrdinalValue();

            return JsonSerializer.Serialize(
                new
                {
                    version = 1,
                    summary = new
                    {
                        totalWarnings = compositeForeignKeyItems.Length
                            + possibleJoinTableItems.Length
                            + providerOwnedSchemaFeatures.Length
                            + skippedDatabaseObjects.Length,
                        sectionCounts = new
                        {
                            compositeForeignKeys = compositeForeignKeyItems.Length,
                            possibleManyToManyJoinTables = possibleJoinTableItems.Length,
                            providerOwnedSchemaFeatures = providerOwnedSchemaFeatures.Length,
                            skippedDatabaseObjects = skippedDatabaseObjects.Length
                        },
                        codes = diagnosticCodes,
                        categories = diagnosticCategories
                    },
                    compositeForeignKeys = compositeForeignKeyItems,
                    possibleManyToManyJoinTables = possibleJoinTableItems,
                    providerOwnedSchemaFeatures,
                    skippedDatabaseObjects
                },
                new JsonSerializerOptions { WriteIndented = true });
        }

    }
}
