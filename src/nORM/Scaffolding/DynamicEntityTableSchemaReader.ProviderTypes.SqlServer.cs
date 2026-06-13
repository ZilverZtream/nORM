#nullable enable
using System;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private static bool TryResolveSqlServerProviderSpecificClrType(
            DbConnection connection,
            string? sqlServerAliasBaseType,
            out Type providerClrType)
        {
            providerClrType = typeof(object);
            return DynamicEntityConnectionKind.IsSqlServer(connection)
                   && ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(
                       sqlServerAliasBaseType,
                       out providerClrType);
        }

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string? typeText)
            => string.IsNullOrWhiteSpace(typeText)
                ? null
                : ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);
    }
}
