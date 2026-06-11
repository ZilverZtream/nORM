#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldFeatureConfigurationAdapter
    {
        public static DatabaseScaffolder.ScaffoldFeatureConfigurations BuildFeatureConfigurations(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
        {
            var featureInputs = ConvertFeatureInputs(unsupportedFeatures);
            var configurations = ScaffoldFeatureConfigurationBuilder.BuildFeatureConfigurations(
                featureInputs,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);
            var generatedFeatureIndexes = configurations.GeneratedFeatureIndexes.ToArray();
            var generatedFeatureIndexSet = generatedFeatureIndexes.ToHashSet();
            var generatedModelFeatureDiagnostics = generatedFeatureIndexes
                .Select(index => unsupportedFeatures[index])
                .ToArray();
            for (var i = unsupportedFeatures.Count - 1; i >= 0; i--)
            {
                if (generatedFeatureIndexSet.Contains(i))
                    unsupportedFeatures.RemoveAt(i);
            }

            return ConvertFeatureConfigurations(configurations, generatedModelFeatureDiagnostics);
        }

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(
                ConvertFeatureInputs(features),
                columnPropertiesByTable);

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
            => ConvertDefaultValueConfigurations(ScaffoldFeatureConfigurationBuilder.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ConvertPrecisionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertDecimalPrecisionInfoMap(decimalPrecisionByTable)));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ConvertColumnFacetConfigurations(ScaffoldFeatureConfigurationBuilder.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertIdentityOptionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

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

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertCollationConfigurations(ScaffoldFeatureConfigurationBuilder.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertComputedColumnConfigurations(ScaffoldFeatureConfigurationBuilder.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureNameMap(ConvertFeatureInputs(features), kinds);

        public static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationBuilder.BuildProviderNativeTemporalTableKeys(ConvertFeatureInputs(features));

        public static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureConfigurationBuilder.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        public static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(ConvertFeatureInputs(features), kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureDetailMap(ConvertFeatureInputs(features), kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertDecimalPrecisionMap(ScaffoldFeatureConfigurationBuilder.BuildDecimalPrecisionMap(ConvertFeatureInputs(features)));

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

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> ConvertCheckConstraintConfigurations(
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> checks)
            => checks
                .Select(static check => new DatabaseScaffolder.ScaffoldCheckConstraintConfiguration(check.TableKey, check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldCheckConstraintConfigurationInfo ConvertCheckConstraintConfiguration(
            DatabaseScaffolder.ScaffoldCheckConstraintConfiguration check)
            => new(check.TableKey, check.EntityName, check.Name, check.Sql);

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> ConvertDefaultValueConfigurations(
            IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> defaultValues)
            => defaultValues
                .Select(static value => new DatabaseScaffolder.ScaffoldDefaultValueConfiguration(value.TableKey, value.EntityName, value.ColumnName, value.PropertyName, value.DefaultValueSql))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> ConvertExpressionIndexConfigurations(
            IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> expressionIndexes)
            => expressionIndexes
                .Select(static index => new DatabaseScaffolder.ScaffoldExpressionIndexConfiguration(index.TableKey, index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> ConvertCollationConfigurations(
            IReadOnlyList<ScaffoldCollationConfigurationInfo> collations)
            => collations
                .Select(static collation => new DatabaseScaffolder.ScaffoldCollationConfiguration(collation.TableKey, collation.EntityName, collation.ColumnName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> ConvertComputedColumnConfigurations(
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> computedColumns)
            => computedColumns
                .Select(static computed => new DatabaseScaffolder.ScaffoldComputedColumnConfiguration(computed.TableKey, computed.EntityName, computed.ColumnName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> ConvertDecimalPrecisionMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new DatabaseScaffolder.ScaffoldDecimalPrecision(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> ConvertDecimalPrecisionInfoMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new ScaffoldDecimalPrecisionInfo(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> ConvertPrecisionConfigurations(
            IReadOnlyList<ScaffoldPrecisionConfigurationInfo> precisionConfigurations)
            => precisionConfigurations
                .Select(static precision => new DatabaseScaffolder.ScaffoldPrecisionConfiguration(precision.TableKey, precision.EntityName, precision.ColumnName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> ConvertColumnFacetConfigurations(
            IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(static facet => new DatabaseScaffolder.ScaffoldColumnFacetConfiguration(facet.TableKey, facet.EntityName, facet.ColumnName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> ConvertIdentityOptionConfigurations(
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> identityOptions)
            => identityOptions
                .Select(static identity => new DatabaseScaffolder.ScaffoldIdentityOptionConfiguration(identity.TableKey, identity.EntityName, identity.ColumnName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();
    }
}
