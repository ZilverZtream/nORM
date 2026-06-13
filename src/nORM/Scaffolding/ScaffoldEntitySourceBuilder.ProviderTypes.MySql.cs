#nullable enable
using System;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static bool TryNormalizeMySqlScaffoldClrType(
            DatabaseProvider provider,
            string? providerSpecificColumnType,
            out Type normalizedClrType)
        {
            normalizedClrType = typeof(object);
            return ScaffoldProviderKind.IsMySql(provider)
                   && ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(
                       providerSpecificColumnType,
                       out normalizedClrType);
        }
    }
}
