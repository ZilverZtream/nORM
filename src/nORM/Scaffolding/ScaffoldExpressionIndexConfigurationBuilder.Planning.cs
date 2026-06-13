#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldExpressionIndexConfigurationBuilder
    {
        private static Dictionary<string, ScaffoldUnsupportedFeatureInfo> BuildFeatureMap(
            IEnumerable<ScaffoldFeatureInput> features,
            string kind)
            => features
                .Where(input => string.Equals(input.Feature.Kind, kind, StringComparison.OrdinalIgnoreCase)
                                && !string.IsNullOrWhiteSpace(input.Feature.Name))
                .GroupBy(static input => FeatureKey(input.Feature), StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static group => group.Key, static group => group.First().Feature, StringComparer.OrdinalIgnoreCase);

        private static bool TryBuildExpressionIndexConfiguration(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, ScaffoldUnsupportedFeatureInfo> providerSpecificIndexes,
            IReadOnlyDictionary<string, ScaffoldUnsupportedFeatureInfo> includedColumnIndexes,
            ScaffoldUnsupportedFeatureInfo feature,
            out ScaffoldExpressionIndexConfigurationInfo configuration)
        {
            configuration = default;
            if (!TryGetExpressionIndexEntityName(feature, entityByTable, out var entityName))
                return false;

            var key = FeatureKey(feature);
            var values = ScaffoldSemicolonParser.Parse(feature.Detail, out _);
            var expressionSql = ExtractExpressionSql(feature, values);
            if (expressionSql.EndsWith("expression index", StringComparison.OrdinalIgnoreCase))
                return false;

            var isUnique = IsExpressionIndexUnique(feature, values);
            var filterSql = ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(feature.Detail);
            var includedColumnNames = ResolveIncludedColumnNames(feature, key, includedColumnIndexes);
            if (includedColumnNames is null)
                return false;

            var nullSortOrder = ExtractTrailingNullSortOrder(ref expressionSql);
            var nullsNotDistinct = false;
            if (providerSpecificIndexes.TryGetValue(key, out var providerSpecificFeature)
                && !TryApplyProviderSpecificExpressionIndexFacets(
                    providerSpecificFeature.Detail,
                    isUnique,
                    ref expressionSql,
                    ref nullSortOrder,
                    ref nullsNotDistinct))
            {
                return false;
            }

            configuration = new ScaffoldExpressionIndexConfigurationInfo(
                feature.TableKey,
                entityName,
                feature.Name,
                expressionSql,
                IsUnique: isUnique,
                FilterSql: filterSql)
            {
                IncludedColumnNames = includedColumnNames,
                NullSortOrder = nullSortOrder,
                NullsNotDistinct = nullsNotDistinct
            };
            return true;
        }

        private static bool TryGetExpressionIndexEntityName(
            ScaffoldUnsupportedFeatureInfo feature,
            IReadOnlyDictionary<string, string> entityByTable,
            out string entityName)
        {
            entityName = string.Empty;
            if (!string.Equals(feature.Kind, "ExpressionIndex", StringComparison.OrdinalIgnoreCase)
                || string.IsNullOrWhiteSpace(feature.Name)
                || string.IsNullOrWhiteSpace(feature.Detail)
                || IsProviderOwnedExpressionIndexDetail(feature.Detail)
                || !entityByTable.TryGetValue(feature.TableKey, out var mappedEntityName))
            {
                return false;
            }

            entityName = mappedEntityName;
            return true;
        }

        private static string ExtractExpressionSql(
            ScaffoldUnsupportedFeatureInfo feature,
            IReadOnlyDictionary<string, string> values)
            => values.TryGetValue("expression", out var expressionValue)
                ? expressionValue.Trim()
                : ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(feature.Detail) ?? feature.Detail.Trim();

        private static bool IsExpressionIndexUnique(
            ScaffoldUnsupportedFeatureInfo feature,
            IReadOnlyDictionary<string, string> values)
            => values.TryGetValue("isUnique", out var isUniqueValue)
               && ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(isUniqueValue, out var parsedIsUnique)
                ? parsedIsUnique
                : ScaffoldSqlMetadataParser.IsCreateIndexUnique(feature.Detail);

        private static string[]? ResolveIncludedColumnNames(
            ScaffoldUnsupportedFeatureInfo feature,
            string key,
            IReadOnlyDictionary<string, ScaffoldUnsupportedFeatureInfo> includedColumnIndexes)
        {
            if (!includedColumnIndexes.TryGetValue(key, out var includedFeature))
                return Array.Empty<string>();

            var includedColumnNames = ScaffoldSqlMetadataParser.ExtractCreateIndexIncludedColumnNames(includedFeature.Detail);
            if (includedColumnNames.Length == 0)
                includedColumnNames = ScaffoldSqlMetadataParser.ExtractCreateIndexIncludedColumnNames(feature.Detail);

            return includedColumnNames.Length == 0 ? null : includedColumnNames;
        }

        private static string FeatureKey(ScaffoldUnsupportedFeatureInfo feature)
            => feature.TableKey + "\u001f" + feature.Name;
    }
}
