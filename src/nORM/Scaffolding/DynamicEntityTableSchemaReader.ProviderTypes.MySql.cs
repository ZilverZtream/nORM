#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private static bool TryResolveMySqlProviderSpecificClrType(
            DbConnection connection,
            string columnName,
            IReadOnlyDictionary<string, string> mySqlUnsignedColumnTypes,
            out Type providerClrType)
        {
            providerClrType = typeof(object);
            return DynamicEntityConnectionKind.IsMySql(connection)
                   && mySqlUnsignedColumnTypes.TryGetValue(columnName, out var unsignedColumnType)
                   && ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(
                       unsignedColumnType,
                       out providerClrType);
        }
    }
}
