#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static class ScaffoldIndexFeatureMetadataBuilder
    {
        public static void AddIndexFeatureMetadata(IDictionary<string, object?> metadata, string detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return;

            var values = ScaffoldSemicolonParser.Parse(detail, out var header);
            var provider = ParseSkippedObjectProvider(header);
            if (IsKnownProviderName(provider))
                metadata["provider"] = provider;

            AddMetadataValue(metadata, values, "indexType");
            AddMetadataValue(metadata, values, "accessMethod");
            AddMetadataValue(metadata, values, "filterSql");
            AddMetadataBooleanValue(metadata, values, "hasNullsNotDistinct");
            AddMetadataBooleanValue(metadata, values, "hasNonDefaultOperatorClass");
            AddMetadataBooleanValue(metadata, values, "hasIndexCollation");
            AddMetadataBooleanValue(metadata, values, "hasNonDefaultNullOrdering");

            if (values.TryGetValue("prefixColumns", out var prefixColumns)
                && !string.IsNullOrWhiteSpace(prefixColumns))
            {
                metadata["prefixColumns"] = ParsePrefixIndexColumns(prefixColumns);
            }

            var indexSql = values.TryGetValue("indexSql", out var explicitIndexSql)
                ? NullIfWhiteSpace(explicitIndexSql)
                : ExtractCreateIndexStatement(detail);
            if (string.IsNullOrWhiteSpace(indexSql))
                return;

            metadata["indexSql"] = indexSql;
            metadata["isUnique"] = ScaffoldSqlMetadataParser.IsCreateIndexUnique(indexSql);
            var keySql = ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(indexSql);
            if (!string.IsNullOrWhiteSpace(keySql))
                metadata["keySql"] = keySql;
            var filterSql = ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(indexSql);
            if (!string.IsNullOrWhiteSpace(filterSql))
                metadata["filterSql"] = filterSql;
        }

        public static void AddExpressionIndexFeatureMetadata(IDictionary<string, object?> metadata, string detail)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            if (values.TryGetValue("expression", out var expression)
                && !string.IsNullOrWhiteSpace(expression))
            {
                metadata["expressionSql"] = expression.Trim();
            }

            var indexSql = ExtractCreateIndexStatement(detail);
            var expressionSql = ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(indexSql ?? detail);
            if (!string.IsNullOrWhiteSpace(expressionSql)
                && !expressionSql.EndsWith("expression index", StringComparison.OrdinalIgnoreCase))
            {
                metadata["expressionSql"] = expressionSql;
            }
        }

        private static void AddMetadataBooleanValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (!values.TryGetValue(key, out var value)
                || !ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(value, out var parsed))
            {
                return;
            }

            metadata[key] = parsed;
        }

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParsePrefixIndexColumns(string prefixColumns)
        {
            var result = new List<IReadOnlyDictionary<string, object?>>();
            foreach (var rawPart in ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(prefixColumns))
            {
                var part = rawPart.Trim();
                if (part.Length == 0)
                    continue;

                var separator = part.LastIndexOf(':');
                if (separator <= 0)
                    continue;

                var lengths = part[(separator + 1)..];
                var slash = lengths.IndexOf('/');
                var prefixLengthText = slash >= 0 ? lengths[..slash] : lengths;
                var declaredLengthText = slash >= 0 ? lengths[(slash + 1)..] : string.Empty;
                var column = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["name"] = part[..separator].Trim()
                };

                var prefixLength = ParseNullableInt(prefixLengthText);
                if (prefixLength.HasValue)
                    column["prefixLength"] = prefixLength.Value;

                var declaredLength = ParseNullableInt(declaredLengthText);
                if (declaredLength.HasValue)
                    column["declaredLength"] = declaredLength.Value;

                result.Add(column);
            }

            return result;
        }

        private static string? ExtractCreateIndexStatement(string detail)
        {
            var createIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(detail, "CREATE", 0);
            if (createIndex < 0)
                return null;

            var candidate = detail[createIndex..].Trim();
            var index = 0;
            return ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(candidate, ref index, "CREATE")
                ? candidate
                : null;
        }

        private static bool IsKnownProviderName(string value)
            => value.Equals("SQL Server", StringComparison.OrdinalIgnoreCase)
               || value.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase)
               || value.Equals("MySQL", StringComparison.OrdinalIgnoreCase)
               || value.Equals("SQLite", StringComparison.OrdinalIgnoreCase);

        private static void AddMetadataValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
                metadata[key] = value;
        }

        private static string ParseSkippedObjectProvider(string detail)
            => ScaffoldSkippedObjectMetadataBuilder.ParseSkippedObjectProvider(detail);

        private static int? ParseNullableInt(string? value)
            => int.TryParse(value, out var parsed) ? parsed : null;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
