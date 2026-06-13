#nullable enable
using System;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static bool TryNormalizeSqlServerScaffoldClrType(
            DatabaseProvider provider,
            string? providerSpecificColumnType,
            out Type normalizedClrType)
        {
            normalizedClrType = typeof(object);
            return ScaffoldProviderKind.IsSqlServer(provider)
                   && ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrType(
                       providerSpecificColumnType,
                       out normalizedClrType);
        }
    }
}
