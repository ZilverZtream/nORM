#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTypeBuilder
    {
        private static IReadOnlyList<DynamicEntityTypeGenerator.ColumnInfo> OrderDynamicColumns(
            IReadOnlyList<DynamicEntityTypeGenerator.ColumnInfo> columns)
            => columns
                .OrderBy(static column => column.IsKey ? 0 : 1)
                .ThenBy(static column => column.IsKey ? column.KeyOrdinal : int.MaxValue)
                .ThenBy(static column => column.SourceOrdinal)
                .ToArray();
    }
}
