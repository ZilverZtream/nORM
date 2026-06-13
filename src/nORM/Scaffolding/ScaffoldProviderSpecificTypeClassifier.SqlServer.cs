#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldProviderSpecificTypeClassifier
    {
        public static bool IsSafeSqlServerAliasColumnType(string? detail)
            => ScaffoldSqlServerAliasTypeClassifier.IsSafeSqlServerAliasColumnType(detail);

        public static bool TryGetSqlServerAliasBaseTypeText(string? detail, out string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.TryGetSqlServerAliasBaseTypeText(detail, out typeText);

        public static bool IsSafeSqlServerAliasBaseType(string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.IsSafeSqlServerAliasBaseType(typeText);

        public static bool TryMapSqlServerAliasBaseClrType(string? detail, out Type type)
            => ScaffoldSqlServerAliasTypeClassifier.TryMapSqlServerAliasBaseClrType(detail, out type);

        public static bool TryMapSqlServerAliasBaseClrTypeName(string? typeText, out Type type)
            => ScaffoldSqlServerAliasTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(typeText, out type);

        public static string NormalizeSqlServerAliasBaseTypeName(string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.NormalizeSqlServerAliasBaseTypeName(typeText);

        public static int? GetSqlServerAliasBaseMaxLength(string? detail)
            => ScaffoldSqlServerAliasTypeClassifier.GetSqlServerAliasBaseMaxLength(detail);

        public static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string typeText)
            => ScaffoldSqlServerAliasTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);
    }
}
