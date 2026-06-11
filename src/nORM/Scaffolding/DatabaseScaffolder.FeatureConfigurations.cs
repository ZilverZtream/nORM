#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildScaffoldDefaultValueMap(features, columnPropertiesByTable);

        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable);

        private static IReadOnlyList<ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                decimalPrecisionByTable);

        private static IReadOnlyList<ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable);

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildCheckConstraintConfigurations(entityByTable, features);

        private static bool CheckConstraintConfigurationMatchesFeature(
            ScaffoldCheckConstraintConfiguration check,
            ScaffoldUnsupportedFeature feature)
            => ScaffoldFeatureConfigurationAdapter.CheckConstraintConfigurationMatchesFeature(check, feature);

        private static string BuildGeneratedCheckConstraintName(string entityName, string sql)
            => ScaffoldFeatureConfigurationBuilder.BuildGeneratedCheckConstraintName(entityName, sql);

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildEnumCheckConstraintConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static bool TryBuildProviderValueCheckSql(
            string columnName,
            string? detail,
            out string checkKind,
            out string sql)
            => ScaffoldFeatureConfigurationBuilder.TryBuildProviderValueCheckSql(columnName, detail, out checkKind, out sql);

        private static bool TryParseEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseEnumValues(detail, out values);

        private static bool TryParsePostgresDomainEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresDomainEnumValues(detail, out values);

        private static bool TryParseMySqlEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseMySqlEnumValues(detail, out values);

        private static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(detail, out values);

        private static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseMySqlQuotedTypeValues(detail, typeName, out values);

        private static bool TryParsePostgresEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresEnumValues(detail, out values);

        private static bool TryParseSqlStringLiteralList(string body, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseSqlStringLiteralList(body, out values);

        private static IReadOnlyList<ScaffoldExpressionIndexConfiguration> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildExpressionIndexConfigurations(entityByTable, features);

        private static bool IsProviderOwnedExpressionIndexDetail(string detail)
            => ScaffoldFeatureConfigurationBuilder.IsProviderOwnedExpressionIndexDetail(detail);

        private static IReadOnlyList<ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static string? ExtractCreateIndexWhereClause(string sql)
            => ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(sql);

        private static bool IsCreateIndexUnique(string? createIndexSql)
            => ScaffoldSqlMetadataParser.IsCreateIndexUnique(createIndexSql);

        private static bool TryConsumeSqlKeyword(string sql, ref int index, string keyword)
            => ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(sql, ref index, keyword);

        private static string? ExtractCreateIndexExpressionSql(string? createIndexSql)
            => ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(createIndexSql);

        private static int FindCreateIndexKeyListOpen(string sql, int startIndex)
            => ScaffoldSqlMetadataParser.FindCreateIndexKeyListOpen(sql, startIndex);

        private static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
            => ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, keyword, startIndex);

        private static IReadOnlyList<ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static (string Sql, bool Stored) NormalizeScaffoldComputedSql(string raw)
            => ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(raw);

        private static bool TryTrimTrailingComputedStorageToken(string sql, string token, out string trimmedSql)
            => ScaffoldSqlMetadataParser.TryTrimTrailingComputedStorageToken(sql, token, out trimmedSql);

        private static string NormalizeScaffoldCheckSql(string raw)
            => ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(raw);

        private static bool TryNormalizeScaffoldDefaultSql(string? raw, out string defaultValueSql)
            => ScaffoldSqlMetadataParser.TryNormalizeScaffoldDefaultSql(raw, out defaultValueSql);

        private static bool HasBalancedOuterParentheses(string value)
            => ScaffoldSqlMetadataParser.HasBalancedOuterParentheses(value);

        private static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureNameMap(features, kinds);

        private static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildProviderNativeTemporalTableKeys(features);

        private static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        private static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureTableKeys(features, kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureDetailMap(features, kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildDecimalPrecisionMap(features);

        private static bool TryParseDecimalPrecision(string? typeName, out int precision, out int? scale)
            => ScaffoldSqlMetadataParser.TryParseDecimalPrecision(typeName, out precision, out scale);

        private static bool TryParseIdentityOptions(string? detail, out long seed, out long increment)
            => ScaffoldSqlMetadataParser.TryParseIdentityOptions(detail, out seed, out increment);
    }
}
