#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private static bool TryResolvePostgresProviderSpecificClrType(
            DbConnection connection,
            Type normalizedClrType,
            string columnName,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes,
            out Type providerClrType)
        {
            providerClrType = normalizedClrType;
            if (!DynamicEntityConnectionKind.IsPostgres(connection)
                || normalizedClrType != typeof(Array)
                || !postgresDomainColumnCastTypes.TryGetValue(columnName, out var domainCastType))
            {
                return false;
            }

            return ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(
                domainCastType.Trim().ToLowerInvariant(),
                out providerClrType);
        }
    }
}
