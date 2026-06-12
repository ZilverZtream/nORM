#nullable enable

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
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

        private static bool TryParseDecimalPrecision(string? typeName, out int precision, out int? scale)
            => ScaffoldSqlMetadataParser.TryParseDecimalPrecision(typeName, out precision, out scale);

        private static bool TryParseIdentityOptions(string? detail, out long seed, out long increment)
            => ScaffoldSqlMetadataParser.TryParseIdentityOptions(detail, out seed, out increment);
    }
}
