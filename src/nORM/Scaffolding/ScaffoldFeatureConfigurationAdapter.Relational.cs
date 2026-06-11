#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        public static IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertCheckConstraintConfigurations(ScaffoldFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(
                entityByTable,
                ConvertFeatureInputs(features)));

        public static bool CheckConstraintConfigurationMatchesFeature(
            DatabaseScaffolder.ScaffoldCheckConstraintConfiguration check,
            DatabaseScaffolder.ScaffoldUnsupportedFeature feature)
            => ScaffoldFeatureConfigurationBuilder.CheckConstraintConfigurationMatchesFeature(
                ConvertCheckConstraintConfiguration(check),
                ConvertUnsupportedFeatureInputInfo(feature));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertCheckConstraintConfigurations(ScaffoldFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertExpressionIndexConfigurations(ScaffoldFeatureConfigurationBuilder.BuildExpressionIndexConfigurations(
                entityByTable,
                ConvertFeatureInputs(features)));
    }
}
