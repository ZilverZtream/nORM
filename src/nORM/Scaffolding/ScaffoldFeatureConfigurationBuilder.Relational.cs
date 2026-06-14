#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        public static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldCheckFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(entityByTable, features);

        public static bool CheckConstraintConfigurationMatchesFeature(
            ScaffoldCheckConstraintConfigurationInfo check,
            ScaffoldUnsupportedFeatureInfo feature)
            => ScaffoldCheckFeatureConfigurationBuilder.CheckConstraintConfigurationMatchesFeature(check, feature);

        public static string BuildGeneratedCheckConstraintName(string entityName, string sql)
            => ScaffoldCheckFeatureConfigurationBuilder.BuildGeneratedCheckConstraintName(entityName, sql);

        public static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldCheckFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(entityByTable, columnPropertiesByTable, features);

        public static bool TryBuildProviderValueCheckSql(
            string columnName,
            string? detail,
            out string checkKind,
            out string sql)
            => ScaffoldCheckFeatureConfigurationBuilder.TryBuildProviderValueCheckSql(columnName, detail, out checkKind, out sql);

        public static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldExpressionIndexConfigurationBuilder.BuildExpressionIndexConfigurations(entityByTable, features);

        public static bool IsProviderOwnedExpressionIndexDetail(string detail)
            => ScaffoldExpressionIndexConfigurationBuilder.IsProviderOwnedExpressionIndexDetail(detail);
    }
}
