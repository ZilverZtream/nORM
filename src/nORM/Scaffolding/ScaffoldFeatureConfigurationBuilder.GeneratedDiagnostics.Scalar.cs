#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        internal static void RemoveSupportedProviderSpecificColumnTypeDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            ICollection<int> generatedFeatureIndexes)
        {
            RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "ProviderSpecificColumnType", StringComparison.OrdinalIgnoreCase)
                && ScaffoldProviderSpecificTypeClassifier.IsScaffoldableProviderSpecificColumnType(input.Feature.Detail)
                && columnPropertiesByTable.TryGetValue(input.Feature.TableKey, out var properties)
                && properties.ContainsKey(input.Feature.Name));
        }

        internal static void RemoveDefaultDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "Default", StringComparison.OrdinalIgnoreCase)
                && defaultValuesByTable.TryGetValue(input.Feature.TableKey, out var defaults)
                && defaults.ContainsKey(input.Feature.Name));

        internal static void RemoveCollationDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldCollationConfigurationInfo> collationConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "Collation", StringComparison.OrdinalIgnoreCase)
                && collationConfigurations.Any(collation =>
                    string.Equals(collation.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(collation.ColumnName, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));

        internal static void RemoveDecimalPrecisionDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, static input =>
                string.Equals(input.Feature.Kind, "PrecisionScale", StringComparison.OrdinalIgnoreCase)
                && ScaffoldSqlMetadataParser.TryParseDecimalPrecision(input.Feature.Detail, out _, out _));
    }
}
