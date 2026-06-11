#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static class ScaffoldIndexNameNormalizer
    {
        public static IReadOnlyList<ScaffoldIndexInfo> NormalizeSyntheticIndexNames(IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            if (!indexes.Any(static index => index.IsSyntheticName))
                return indexes;

            var generatedNames = indexes
                .Where(static index => index.IsSyntheticName)
                .GroupBy(index => index.TableKey + "\u001f" + index.IndexName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    group => group.Key,
                    group =>
                    {
                        var first = group.First();
                        var keyColumns = group
                            .Where(static index => !index.IsIncluded)
                            .OrderBy(static index => index.Ordinal)
                            .Select(static index => index.ColumnName)
                            .ToArray();
                        return BuildGeneratedIndexName(first.TableKey, keyColumns, first.IsUnique);
                    },
                    StringComparer.OrdinalIgnoreCase);

            return indexes
                .Select(index => generatedNames.TryGetValue(index.TableKey + "\u001f" + index.IndexName, out var generatedName)
                    ? index with { IndexName = generatedName, IsSyntheticName = false }
                    : index)
                .ToArray();
        }

        private static string BuildGeneratedIndexName(string tableKey, IReadOnlyList<string> columnNames, bool isUnique)
        {
            var prefix = isUnique ? "UX_" : "IX_";
            var tableSegment = LastTableKeySegment(tableKey);
            var segments = new[] { tableSegment }
                .Concat(columnNames)
                .Select(SanitizeConstraintNameSegment)
                .Where(static segment => segment.Length > 0);
            var baseName = prefix + string.Join("_", segments);
            if (baseName.Length <= 128)
                return baseName;

            var hash = StableIdentifierHash(tableKey + "\u001f" + string.Join("\u001f", columnNames));
            var maxBaseLength = 127 - hash.Length;
            return baseName[..Math.Max(prefix.Length, maxBaseLength)] + "_" + hash;
        }

        private static string LastTableKeySegment(string tableKey)
        {
            var dot = tableKey.LastIndexOf('.');
            return dot >= 0 && dot < tableKey.Length - 1 ? tableKey[(dot + 1)..] : tableKey;
        }

        private static string SanitizeConstraintNameSegment(string value)
        {
            var sb = new StringBuilder(value.Length);
            foreach (var ch in value)
                sb.Append(char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_');

            return sb.Length == 0 ? "Table" : sb.ToString();
        }

        private static string StableIdentifierHash(string value)
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            var hash = offset;
            foreach (var b in Encoding.UTF8.GetBytes(value))
            {
                hash ^= b;
                hash *= prime;
            }

            return hash.ToString("X16", CultureInfo.InvariantCulture);
        }
    }
}
