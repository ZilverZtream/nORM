#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexFeatureMetadataBuilder
    {
        private static void AddPrefixIndexMetadata(IDictionary<string, object?> metadata, string prefixColumns)
        {
            var columns = ParsePrefixIndexColumns(prefixColumns);
            if (columns.Count == 0)
                return;

            metadata["prefixColumns"] = columns;
            metadata["allPrefixColumnsCoverDeclaredLength"] = columns.All(ColumnCoversDeclaredLength);
        }

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParsePrefixIndexColumns(string prefixColumns)
        {
            var result = new List<IReadOnlyDictionary<string, object?>>();
            foreach (var rawPart in ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(prefixColumns))
            {
                var part = rawPart.Trim();
                if (part.Length == 0)
                    continue;

                var separator = part.LastIndexOf(':');
                if (separator <= 0)
                    continue;

                var lengths = part[(separator + 1)..];
                var slash = lengths.IndexOf('/');
                var prefixLengthText = slash >= 0 ? lengths[..slash] : lengths;
                var declaredLengthText = slash >= 0 ? lengths[(slash + 1)..] : string.Empty;
                var column = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["name"] = part[..separator].Trim()
                };

                var prefixLength = ParseNullableInt(prefixLengthText);
                if (prefixLength.HasValue)
                    column["prefixLength"] = prefixLength.Value;

                var declaredLength = ParseNullableInt(declaredLengthText);
                column["declaredLengthKnown"] = declaredLength.HasValue;
                if (declaredLength.HasValue)
                    column["declaredLength"] = declaredLength.Value;

                if (prefixLength.HasValue && declaredLength.HasValue)
                    column["coversDeclaredLength"] = prefixLength.Value >= declaredLength.Value;

                result.Add(column);
            }

            return result;
        }

        private static bool ColumnCoversDeclaredLength(IReadOnlyDictionary<string, object?> column)
            => column.TryGetValue("coversDeclaredLength", out var value)
               && value is true;

        private static int? ParseNullableInt(string? value)
            => int.TryParse(value, out var parsed) ? parsed : null;
    }
}
