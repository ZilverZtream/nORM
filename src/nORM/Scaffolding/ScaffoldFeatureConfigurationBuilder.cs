#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        public static ScaffoldFeatureConfigurationsInfo BuildFeatureConfigurations(
            IReadOnlyList<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
            => ScaffoldFeatureConfigurationPipeline.Build(
                unsupportedFeatures,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldFeatureInput> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(features, columnPropertiesByTable);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultConstraintNameMap(
            IEnumerable<ScaffoldFeatureInput> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildScaffoldDefaultConstraintNameMap(features, columnPropertiesByTable);

        public static IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultConstraintNamesByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildDefaultValueConfigurations(entityByTable, columnPropertiesByTable, defaultValuesByTable, defaultConstraintNamesByTable);

        public static IReadOnlyList<ScaffoldPrecisionConfigurationInfo> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildPrecisionConfigurations(entityByTable, columnPropertiesByTable, decimalPrecisionByTable);

        public static IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildColumnFacetConfigurations(entityByTable, columnPropertiesByTable, columnFacetsByTable);

        public static IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(entityByTable, columnPropertiesByTable, features);

        public static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldCheckFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(entityByTable, features);

        public static bool CheckConstraintConfigurationMatchesFeature(
            ScaffoldCheckConstraintConfigurationInfo check,
            ScaffoldUnsupportedFeatureInfo feature)
            => ScaffoldCheckFeatureConfigurationBuilder.CheckConstraintConfigurationMatchesFeature(check, feature);

        public static string BuildGeneratedCheckConstraintName(string entityName, string sql)
            => ScaffoldCheckFeatureConfigurationBuilder.BuildGeneratedCheckConstraintName(entityName, sql);

        public static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldCheckFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(entityByTable, columnPropertiesByTable, features);

        public static bool TryBuildProviderValueCheckSql(
            string columnName,
            string? detail,
            out string checkKind,
            out string sql)
            => ScaffoldCheckFeatureConfigurationBuilder.TryBuildProviderValueCheckSql(columnName, detail, out checkKind, out sql);

        public static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldExpressionIndexConfigurationBuilder.BuildExpressionIndexConfigurations(entityByTable, features);

        public static IReadOnlyList<ScaffoldCollationConfigurationInfo> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildCollationConfigurations(entityByTable, columnPropertiesByTable, features);

        public static IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildComputedColumnConfigurations(entityByTable, columnPropertiesByTable, features);

        public static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
            => ScaffoldFeatureMapBuilder.BuildFeatureNameMap(features, kinds);

        public static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldFeatureMapBuilder.BuildProviderNativeTemporalTableKeys(features);

        public static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureMapBuilder.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        public static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
            => ScaffoldFeatureMapBuilder.BuildFeatureTableKeys(features, kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
            => ScaffoldFeatureMapBuilder.BuildFeatureDetailMap(features, kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldFeatureMapBuilder.BuildDecimalPrecisionMap(features);

        public static bool IsProviderOwnedExpressionIndexDetail(string detail)
            => ScaffoldExpressionIndexConfigurationBuilder.IsProviderOwnedExpressionIndexDetail(detail);
    }
}
