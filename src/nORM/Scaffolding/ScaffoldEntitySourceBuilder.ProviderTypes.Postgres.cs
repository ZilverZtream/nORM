#nullable enable
using System;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static bool TryNormalizePostgresScaffoldClrType(
            DatabaseProvider provider,
            string? providerSpecificColumnType,
            out Type normalizedClrType)
        {
            normalizedClrType = typeof(object);
            return ScaffoldProviderKind.IsPostgres(provider)
                   && ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayType(
                       providerSpecificColumnType,
                       out normalizedClrType);
        }
    }
}
