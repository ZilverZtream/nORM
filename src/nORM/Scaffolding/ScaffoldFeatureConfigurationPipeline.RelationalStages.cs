#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationPipeline
    {
        private static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildCheckConstraintsStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> enumCheckConstraintConfigurations,
            List<int> generatedFeatureIndexes)
        {
            var checkConstraints = ScaffoldFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(entityByTable, remainingFeatures)
                .Concat(enumCheckConstraintConfigurations)
                .ToArray();
            ScaffoldFeatureConfigurationBuilder.RemoveCheckConstraintDiagnostics(remainingFeatures, checkConstraints, generatedFeatureIndexes);
            ScaffoldFeatureConfigurationBuilder.RemoveGeneratedProviderValueCheckDiagnostics(remainingFeatures, columnPropertiesByTable, enumCheckConstraintConfigurations, generatedFeatureIndexes);
            return checkConstraints;
        }

        private static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            List<int> generatedFeatureIndexes)
        {
            var expressionIndexConfigurations = ScaffoldFeatureConfigurationBuilder.BuildExpressionIndexConfigurations(entityByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveExpressionIndexDiagnostics(remainingFeatures, expressionIndexConfigurations, generatedFeatureIndexes);
            return expressionIndexConfigurations;
        }
    }
}
