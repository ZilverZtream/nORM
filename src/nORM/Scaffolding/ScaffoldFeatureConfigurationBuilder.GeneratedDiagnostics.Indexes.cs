#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        internal static void RemoveExpressionIndexDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> expressionIndexConfigurations,
            ICollection<int> generatedFeatureIndexes)
        {
            RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "ExpressionIndex", StringComparison.OrdinalIgnoreCase)
                && expressionIndexConfigurations.Any(index =>
                    string.Equals(index.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(index.Name, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));
            RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "PartialIndex", StringComparison.OrdinalIgnoreCase)
                && expressionIndexConfigurations.Any(index =>
                    !string.IsNullOrWhiteSpace(index.FilterSql)
                    && string.Equals(index.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(index.Name, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));
            RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "DescendingIndex", StringComparison.OrdinalIgnoreCase)
                && expressionIndexConfigurations.Any(index =>
                    string.Equals(index.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(index.Name, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));
        }
    }
}
