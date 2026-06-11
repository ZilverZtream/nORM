#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static IReadOnlyList<ScaffoldEntityIndexInfo> GetColumnIndexes(
            IReadOnlyList<ScaffoldEntityIndexSourceInfo>? indexes,
            string columnName)
            => (indexes ?? Array.Empty<ScaffoldEntityIndexSourceInfo>())
                .Where(index => string.Equals(index.ColumnName, columnName, StringComparison.Ordinal))
                .Select(index => new ScaffoldEntityIndexInfo(
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql))
                .ToArray();
    }
}
