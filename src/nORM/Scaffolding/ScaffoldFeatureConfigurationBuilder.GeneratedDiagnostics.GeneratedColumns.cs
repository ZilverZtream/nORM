#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        internal static void RemoveComputedColumnDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> computedColumnConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "Computed", StringComparison.OrdinalIgnoreCase)
                && computedColumnConfigurations.Any(computed =>
                    string.Equals(computed.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(computed.ColumnName, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));

        internal static void RemoveIdentityOptionDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> identityOptionConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "IdentityStrategy", StringComparison.OrdinalIgnoreCase)
                && identityOptionConfigurations.Any(identity =>
                    string.Equals(identity.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(identity.ColumnName, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));
    }
}
