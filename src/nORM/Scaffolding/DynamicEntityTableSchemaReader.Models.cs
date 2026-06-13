#nullable enable
using System.Collections.Generic;

using DynamicScaffoldColumnFacet = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldColumnFacet;
using DynamicScaffoldComputedColumn = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldComputedColumn;
using DynamicScaffoldDecimalPrecision = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldDecimalPrecision;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private readonly record struct ColumnSchemaFlags(
            bool IsKey,
            int KeyOrdinal,
            bool IsAuto,
            bool IsComputed,
            DynamicScaffoldComputedColumn? ComputedColumn,
            bool IsRowVersion,
            bool EffectiveAllowNull,
            string? DeclaredType,
            string? ColumnStoreType,
            string? SqlServerAliasBaseType);

        private readonly record struct DynamicColumnMetadata(
            IReadOnlyDictionary<string, string> PostgresDomainColumnCastTypes,
            IReadOnlyDictionary<string, DynamicScaffoldComputedColumn> ComputedColumns,
            IReadOnlySet<string> IdentityColumns,
            IReadOnlySet<string> RowVersionColumns,
            IReadOnlyDictionary<string, string> SqliteDeclaredTypes,
            IReadOnlyDictionary<string, string> ColumnStoreTypes,
            IReadOnlyDictionary<string, string> SqlServerAliasBaseTypes,
            IReadOnlyDictionary<string, string> MySqlUnsignedColumnTypes,
            IReadOnlyDictionary<string, DynamicScaffoldDecimalPrecision> DecimalPrecisions,
            IReadOnlyDictionary<string, DynamicScaffoldColumnFacet> ColumnFacets,
            IReadOnlyDictionary<string, int> PrimaryKeyOrdinals);
    }
}
