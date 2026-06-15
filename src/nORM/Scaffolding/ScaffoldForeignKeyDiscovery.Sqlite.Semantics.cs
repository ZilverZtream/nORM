#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private static string? TryGetSqliteProviderSemantics(
            IReadOnlyList<SqliteForeignKeyRow> rows,
            IReadOnlyDictionary<string, string> providerSemanticsByColumns)
        {
            var columnKey = ScaffoldSqliteDdlParser.BuildForeignKeyColumnKey(rows.Select(static row => row.DependentColumn));
            return providerSemanticsByColumns.TryGetValue(columnKey, out var semantics) ? semantics : null;
        }

        private static string NormalizeSqliteReferentialAction(string action, string match, string? providerSemantics)
        {
            var normalized = ScaffoldReferentialAction.Normalize(action);
            var normalizedSemantics = string.IsNullOrWhiteSpace(providerSemantics)
                ? NormalizeSqliteMatch(match)
                : providerSemantics;
            return normalizedSemantics is null ? normalized : normalized + " " + normalizedSemantics;
        }

        private static string? NormalizeSqliteMatch(string? match)
        {
            if (string.IsNullOrWhiteSpace(match))
                return null;

            var normalized = match.Replace('_', ' ').Trim().ToUpperInvariant();
            return normalized is "NONE" or "SIMPLE" ? null : "MATCH " + normalized;
        }
    }
}
