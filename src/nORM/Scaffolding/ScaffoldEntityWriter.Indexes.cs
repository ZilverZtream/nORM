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
                var uniqueSuffix = !index.IsIncluded && index.IsUnique ? ", IsUnique = true" : string.Empty;
                var orderSuffix = index.ColumnCount > 1 && !index.IsIncluded ? $", Order = {index.Ordinal.ToString(CultureInfo.InvariantCulture)}" : string.Empty;
                var descendingSuffix = !index.IsIncluded && index.IsDescending ? ", IsDescending = true" : string.Empty;
                var includedSuffix = index.IsIncluded ? ", IsIncluded = true" : string.Empty;
                var nullSortOrderSuffix = !index.IsIncluded && index.NullSortOrder != IndexNullSortOrder.Default ? $", NullSortOrder = IndexNullSortOrder.{index.NullSortOrder}" : string.Empty;
                var nullsNotDistinctSuffix = !index.IsIncluded && index.NullsNotDistinct ? ", NullsNotDistinct = true" : string.Empty;
                var filterSuffix = !index.IsIncluded && !string.IsNullOrWhiteSpace(index.FilterSql) ? $", FilterSql = \"{EscapeStringLiteral(index.FilterSql)}\"" : string.Empty;
                sb.AppendLine($"    [Index(\"{safeIndexName}\"{uniqueSuffix}{orderSuffix}{descendingSuffix}{includedSuffix}{nullSortOrderSuffix}{nullsNotDistinctSuffix}{filterSuffix})]");
            }
        }
    }
}
