#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        internal static void RemoveCheckConstraintDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> checkConstraints,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "CheckConstraint", StringComparison.OrdinalIgnoreCase)
                && checkConstraints.Any(check => CheckConstraintConfigurationMatchesFeature(check, input.Feature)));

        internal static void RemoveGeneratedProviderValueCheckDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> enumCheckConstraintConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "ProviderSpecificColumnType", StringComparison.OrdinalIgnoreCase)
                && !ScaffoldProviderSpecificTypeClassifier.TryGetPostgresDomainBaseTypeText(input.Feature.Detail, out _)
                && columnPropertiesByTable.TryGetValue(input.Feature.TableKey, out var properties)
                && properties.TryGetValue(input.Feature.Name, out var propertyName)
                && enumCheckConstraintConfigurations.Any(check =>
                    string.Equals(check.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && check.Name.EndsWith("_" + propertyName + "_Enum", StringComparison.Ordinal)));
    }
}
