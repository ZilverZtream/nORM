#nullable enable
using System;
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
                .Select(static value => new DatabaseScaffolder.ScaffoldDefaultValueConfiguration(value.TableKey, value.EntityName, value.ColumnName, value.PropertyName, value.DefaultValueSql, value.ConstraintName))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> ConvertExpressionIndexConfigurations(
            IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> expressionIndexes)
            => expressionIndexes
                .Select(static index => new DatabaseScaffolder.ScaffoldExpressionIndexConfiguration(index.TableKey, index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql)
                {
                    IncludedColumnNames = index.IncludedColumnNames,
                    NullSortOrder = index.NullSortOrder,
                    NullsNotDistinct = index.NullsNotDistinct
                })
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
