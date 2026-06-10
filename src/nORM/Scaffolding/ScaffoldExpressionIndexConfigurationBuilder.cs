#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldExpressionIndexConfigurationBuilder
    {
        public static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var featureList = features as IReadOnlyList<ScaffoldFeatureInput> ?? features.ToArray();
            var unrepresentableExpressionIndexes = featureList
                .Where(static input => string.Equals(input.Feature.Kind, "ProviderSpecificIndex", StringComparison.OrdinalIgnoreCase)
                                       || string.Equals(input.Feature.Kind, "IncludedColumnIndex", StringComparison.OrdinalIgnoreCase))
                .Select(static input => input.Feature.TableKey + "\u001f" + input.Feature.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new List<ScaffoldExpressionIndexConfigurationInfo>();
            foreach (var input in featureList)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "ExpressionIndex", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || IsProviderOwnedExpressionIndexDetail(feature.Detail)
                    || unrepresentableExpressionIndexes.Contains(feature.TableKey + "\u001f" + feature.Name)
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

                result.Add(new ScaffoldExpressionIndexConfigurationInfo(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    expressionSql,
                    IsUnique: isUnique,
                    FilterSql: ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(feature.Detail)));
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
    }
}
