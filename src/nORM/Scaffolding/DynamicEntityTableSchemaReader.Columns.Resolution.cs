#nullable enable
using System;
using System.Data;
using System.Data.Common;

using DynamicScaffoldColumnFacet = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldColumnFacet;
using DynamicScaffoldDecimalPrecision = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldDecimalPrecision;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private static Type ResolveColumnClrType(
            DbConnection connection,
            Type clrType,
            string colName,
            ColumnSchemaFlags flags,
            DynamicColumnMetadata metadata)
        {
            var normalizedClrType = NormalizeScaffoldClrType(
                connection,
                clrType,
                flags.EffectiveAllowNull,
                flags.IsKey,
                flags.IsAuto,
                flags.DeclaredType,
                flags.ColumnStoreType);

            return ResolveProviderSpecificClrType(
                connection,
                normalizedClrType,
                colName,
                metadata.PostgresDomainColumnCastTypes,
                flags.SqlServerAliasBaseType,
                metadata.MySqlUnsignedColumnTypes);
        }

        private static int? ResolveColumnMaxLength(
            Type normalizedClrType,
            DataRow row,
            string? sqlServerAliasBaseType,
            DynamicScaffoldColumnFacet columnFacet)
            => GetScaffoldMaxLength(normalizedClrType, row)
               ?? GetSqlServerAliasBaseMaxLengthFromTypeText(sqlServerAliasBaseType)
               ?? columnFacet.MaxLength;

        private static DynamicScaffoldDecimalPrecision? GetColumnDecimalPrecision(
            Type normalizedClrType,
            string colName,
            DynamicColumnMetadata metadata)
            => normalizedClrType == typeof(decimal)
               && metadata.DecimalPrecisions.TryGetValue(colName, out var precision)
                ? precision
                : (DynamicScaffoldDecimalPrecision?)null;
    }
}
