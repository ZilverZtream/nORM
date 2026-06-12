#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static class ScaffoldExpressionIndexConfigurationBuilder
    {
        public static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var featureList = features as IReadOnlyList<ScaffoldFeatureInput> ?? features.ToArray();
            var providerSpecificIndexes = BuildFeatureMap(featureList, "ProviderSpecificIndex");
            var includedColumnIndexes = BuildFeatureMap(featureList, "IncludedColumnIndex");
            var result = new List<ScaffoldExpressionIndexConfigurationInfo>();
            foreach (var input in featureList)
            {
                var feature = input.Feature;
                var key = FeatureKey(feature);
                if (!string.Equals(feature.Kind, "ExpressionIndex", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || IsProviderOwnedExpressionIndexDetail(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName))
                {
                    continue;
                }

                var values = ScaffoldSemicolonParser.Parse(feature.Detail, out _);
                var expressionSql = values.TryGetValue("expression", out var expressionValue)
                    ? expressionValue.Trim()
                    : ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(feature.Detail) ?? feature.Detail.Trim();
                if (expressionSql.EndsWith("expression index", StringComparison.OrdinalIgnoreCase))
                    continue;

                var isUnique = values.TryGetValue("isUnique", out var isUniqueValue)
                               && ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(isUniqueValue, out var parsedIsUnique)
                    ? parsedIsUnique
                    : ScaffoldSqlMetadataParser.IsCreateIndexUnique(feature.Detail);
                var filterSql = ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(feature.Detail);
                var includedColumnNames = Array.Empty<string>();
                var nullSortOrder = ExtractTrailingNullSortOrder(ref expressionSql);
                var nullsNotDistinct = false;

                if (includedColumnIndexes.TryGetValue(key, out var includedFeature))
                {
                    includedColumnNames = ScaffoldSqlMetadataParser.ExtractCreateIndexIncludedColumnNames(includedFeature.Detail);
                    if (includedColumnNames.Length == 0)
                        continue;
                }

                if (providerSpecificIndexes.TryGetValue(key, out var providerSpecificFeature)
                    && !TryApplyProviderSpecificExpressionIndexFacets(
                        providerSpecificFeature.Detail,
                        isUnique,
                        ref expressionSql,
                        ref nullSortOrder,
                        ref nullsNotDistinct))
                {
                    continue;
                }

                result.Add(new ScaffoldExpressionIndexConfigurationInfo(
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
                });
            }

            return result;
        }

        public static bool IsProviderOwnedExpressionIndexDetail(string detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return true;

            var values = ScaffoldSemicolonParser.Parse(detail, out var header);
            if (values.ContainsKey("expression")
                && header.StartsWith("MySQL expression index", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return values.ContainsKey("expression")
                   && header.EndsWith("expression index", StringComparison.OrdinalIgnoreCase)
                   && !header.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase);
        }

        private static Dictionary<string, ScaffoldUnsupportedFeatureInfo> BuildFeatureMap(
            IEnumerable<ScaffoldFeatureInput> features,
            string kind)
            => features
                .Where(input => string.Equals(input.Feature.Kind, kind, StringComparison.OrdinalIgnoreCase)
                                && !string.IsNullOrWhiteSpace(input.Feature.Name))
                .GroupBy(static input => FeatureKey(input.Feature), StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static group => group.Key, static group => group.First().Feature, StringComparer.OrdinalIgnoreCase);

        private static bool TryApplyProviderSpecificExpressionIndexFacets(
            string detail,
            bool isUnique,
            ref string expressionSql,
            ref IndexNullSortOrder nullSortOrder,
            ref bool nullsNotDistinct)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            if (!values.TryGetValue("accessMethod", out var accessMethod)
                || !accessMethod.Equals("btree", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (TryGetMetadataBoolean(values, "hasNonDefaultOperatorClass")
                || TryGetMetadataBoolean(values, "hasIndexCollation"))
            {
                return false;
            }

            if (TryGetMetadataBoolean(values, "hasNullsNotDistinct"))
            {
                if (!isUnique)
                    return false;
                nullsNotDistinct = true;
            }

            if (TryGetMetadataBoolean(values, "hasNonDefaultNullOrdering"))
            {
                var indexSql = values.TryGetValue("indexSql", out var sql) ? sql : detail;
                var parsedExpression = ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(indexSql);
                if (string.IsNullOrWhiteSpace(parsedExpression))
                    return false;

                expressionSql = parsedExpression;
                nullSortOrder = ExtractTrailingNullSortOrder(ref expressionSql);
                if (nullSortOrder == IndexNullSortOrder.Default)
                    return false;
            }

            return true;
        }

        private static bool TryGetMetadataBoolean(IReadOnlyDictionary<string, string> values, string key)
            => values.TryGetValue(key, out var value)
               && ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(value, out var parsed)
               && parsed;

        private static IndexNullSortOrder ExtractTrailingNullSortOrder(ref string expressionSql)
        {
            const string NullsFirst = " NULLS FIRST";
            const string NullsLast = " NULLS LAST";
            var trimmed = expressionSql.Trim();
            if (trimmed.EndsWith(NullsFirst, StringComparison.OrdinalIgnoreCase))
            {
                expressionSql = trimmed[..^NullsFirst.Length].TrimEnd();
                return IndexNullSortOrder.First;
            }

            if (trimmed.EndsWith(NullsLast, StringComparison.OrdinalIgnoreCase))
            {
                expressionSql = trimmed[..^NullsLast.Length].TrimEnd();
                return IndexNullSortOrder.Last;
            }

            expressionSql = trimmed;
            return IndexNullSortOrder.Default;
        }

        private static string FeatureKey(ScaffoldUnsupportedFeatureInfo feature)
            => feature.TableKey + "\u001f" + feature.Name;
    }
}
