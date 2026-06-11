#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        private static void RemoveSupportedProviderSpecificColumnTypeDiagnostics(
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

        private static void RemoveDefaultDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "Default", StringComparison.OrdinalIgnoreCase)
                && defaultValuesByTable.TryGetValue(input.Feature.TableKey, out var defaults)
                && defaults.ContainsKey(input.Feature.Name));

        private static void RemoveCheckConstraintDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> checkConstraints,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "CheckConstraint", StringComparison.OrdinalIgnoreCase)
                && checkConstraints.Any(check => CheckConstraintConfigurationMatchesFeature(check, input.Feature)));

        private static void RemoveGeneratedProviderValueCheckDiagnostics(
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

        private static void RemoveExpressionIndexDiagnostics(
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

        private static void RemoveCollationDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldCollationConfigurationInfo> collationConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "Collation", StringComparison.OrdinalIgnoreCase)
                && collationConfigurations.Any(collation =>
                    string.Equals(collation.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(collation.ColumnName, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));

        private static void RemoveComputedColumnDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> computedColumnConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "Computed", StringComparison.OrdinalIgnoreCase)
                && computedColumnConfigurations.Any(computed =>
                    string.Equals(computed.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(computed.ColumnName, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));

        private static void RemoveDecimalPrecisionDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, static input =>
                string.Equals(input.Feature.Kind, "PrecisionScale", StringComparison.OrdinalIgnoreCase)
                && ScaffoldSqlMetadataParser.TryParseDecimalPrecision(input.Feature.Detail, out _, out _));

        private static void RemoveIdentityOptionDiagnostics(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> identityOptionConfigurations,
            ICollection<int> generatedFeatureIndexes)
            => RemoveGeneratedUnsupportedFeatures(unsupportedFeatures, generatedFeatureIndexes, input =>
                string.Equals(input.Feature.Kind, "IdentityStrategy", StringComparison.OrdinalIgnoreCase)
                && identityOptionConfigurations.Any(identity =>
                    string.Equals(identity.TableKey, input.Feature.TableKey, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(identity.ColumnName, input.Feature.Name, StringComparison.OrdinalIgnoreCase)));

        private static void RemoveGeneratedUnsupportedFeatures(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            ICollection<int> generatedFeatureIndexes,
            Predicate<ScaffoldFeatureInput> predicate)
        {
            for (var i = unsupportedFeatures.Count - 1; i >= 0; i--)
            {
                var input = unsupportedFeatures[i];
                if (!predicate(input))
                    continue;

                generatedFeatureIndexes.Add(input.SourceIndex);
                unsupportedFeatures.RemoveAt(i);
            }
        }
    }
}
