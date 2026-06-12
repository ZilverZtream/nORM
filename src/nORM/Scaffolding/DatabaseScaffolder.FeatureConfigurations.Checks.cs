#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
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
    }
}
