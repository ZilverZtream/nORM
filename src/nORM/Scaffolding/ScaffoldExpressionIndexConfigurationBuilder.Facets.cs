#nullable enable
using System;
using System.Collections.Generic;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldExpressionIndexConfigurationBuilder
    {
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
    }
}
