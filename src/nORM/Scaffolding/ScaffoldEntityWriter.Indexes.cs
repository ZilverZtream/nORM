#nullable enable
using System;
using System.Globalization;
using System.Linq;
using System.Text;
using nORM.Configuration;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityWriter
    {
        private static void AppendIndexes(StringBuilder sb, System.Collections.Generic.IReadOnlyList<ScaffoldEntityIndexInfo> indexes)
        {
            foreach (var index in indexes
                .OrderBy(i => i.IndexName, StringComparer.Ordinal)
                .ThenBy(i => i.Ordinal))
            {
                if (index.IndexName.Length == 0)
                    continue;

                var safeIndexName = EscapeStringLiteral(index.IndexName);
                var uniqueSuffix = index.IsUnique ? ", IsUnique = true" : string.Empty;
                var orderSuffix = index.ColumnCount > 1 && !index.IsIncluded ? $", Order = {index.Ordinal.ToString(CultureInfo.InvariantCulture)}" : string.Empty;
                var descendingSuffix = index.IsDescending ? ", IsDescending = true" : string.Empty;
                var includedSuffix = index.IsIncluded ? ", IsIncluded = true" : string.Empty;
                var nullSortOrderSuffix = index.NullSortOrder == IndexNullSortOrder.Default ? string.Empty : $", NullSortOrder = IndexNullSortOrder.{index.NullSortOrder}";
                var nullsNotDistinctSuffix = index.NullsNotDistinct ? ", NullsNotDistinct = true" : string.Empty;
                var filterSuffix = string.IsNullOrWhiteSpace(index.FilterSql) ? string.Empty : $", FilterSql = \"{EscapeStringLiteral(index.FilterSql)}\"";
                sb.AppendLine($"    [Index(\"{safeIndexName}\"{uniqueSuffix}{orderSuffix}{descendingSuffix}{includedSuffix}{nullSortOrderSuffix}{nullsNotDistinctSuffix}{filterSuffix})]");
            }
        }
    }
}
