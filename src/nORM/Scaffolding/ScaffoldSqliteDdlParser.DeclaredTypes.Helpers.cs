#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        private static bool TrySplitDeclaredType(string? declaredType, out string baseName, out string body)
        {
            baseName = string.Empty;
            body = string.Empty;
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var open = declaredType.IndexOf('(');
            if (open < 0)
                return false;

            var close = declaredType.IndexOf(')', open + 1);
            if (close < 0)
                return false;

            baseName = declaredType[..open].Trim();
            body = declaredType.Substring(open + 1, close - open - 1).Trim();
            return baseName.Length > 0 && body.Length > 0;
        }

        private static string NormalizeDeclaredTypeBase(string baseName)
        {
            var normalized = baseName.Trim()
                .Replace('_', ' ')
                .ToUpperInvariant();
            while (normalized.Contains("  ", StringComparison.Ordinal))
                normalized = normalized.Replace("  ", " ", StringComparison.Ordinal);

            return normalized;
        }
    }
}
