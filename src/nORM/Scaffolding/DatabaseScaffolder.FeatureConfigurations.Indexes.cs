#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyList<ScaffoldExpressionIndexConfiguration> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildExpressionIndexConfigurations(entityByTable, features);

        private static bool IsProviderOwnedExpressionIndexDetail(string detail)
            => ScaffoldFeatureConfigurationBuilder.IsProviderOwnedExpressionIndexDetail(detail);

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
    }
}
