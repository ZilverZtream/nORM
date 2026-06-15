#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        private static IReadOnlyList<ScaffoldFeatureInput> ConvertFeatureInputs(IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
        {
            var converted = new ScaffoldFeatureInput[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                converted[i] = new ScaffoldFeatureInput(i, ConvertUnsupportedFeatureInputInfo(features[i]));
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldFeatureInput> ConvertFeatureInputs(IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
        {
            var featureList = features as IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> ?? features.ToArray();
            return ConvertFeatureInputs(featureList);
        }

        private static ScaffoldUnsupportedFeatureInfo ConvertUnsupportedFeatureInputInfo(
            DatabaseScaffolder.ScaffoldUnsupportedFeature feature)
            => new(feature.TableKey, feature.Kind, feature.Name, feature.Detail)
            {
                Metadata = feature.Metadata
            };

        private static DatabaseScaffolder.ScaffoldFeatureConfigurations ConvertFeatureConfigurations(
            ScaffoldFeatureConfigurationsInfo configurations,
            IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics)
            => new(
                generatedModelFeatureDiagnostics,
                configurations.ProviderSpecificColumnTypesByTable,
                configurations.DefaultValuesByTable,
                configurations.DefaultConstraintNamesByTable,
                configurations.ProviderSpecificDefaultTableKeys,
                ConvertCheckConstraintConfigurations(configurations.CheckConstraints),
                ConvertExpressionIndexConfigurations(configurations.ExpressionIndexConfigurations),
                ConvertCollationConfigurations(configurations.CollationConfigurations),
                ConvertComputedColumnConfigurations(configurations.ComputedColumnConfigurations),
                configurations.ComputedColumnsByTable,
                ConvertDecimalPrecisionMap(configurations.DecimalPrecisionByTable),
                ConvertPrecisionConfigurations(configurations.PrecisionConfigurations),
                ConvertColumnFacetConfigurations(configurations.ColumnFacetConfigurations),
                configurations.RowVersionColumnsByTable,
                configurations.ProviderNativeTemporalTableKeys,
                configurations.ProviderOwnedTriggerTableKeys,
                ConvertIdentityOptionConfigurations(configurations.IdentityOptionConfigurations),
                configurations.ProviderSpecificIdentityStrategyTableKeys,
                configurations.ProviderOwnedWriteBlockedTableKeys);
    }
}
