#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        private static bool TryGetPostgresArrayElementType(string normalized, out string element)
        {
            element = string.Empty;
            normalized = normalized.Trim();
            if (normalized.EndsWith("[]", StringComparison.Ordinal))
            {
                element = normalized[..^2].Trim().TrimStart('_');
                return element.Length > 0;
            }

            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var afterKeyword = "array".Length;
            if (afterKeyword < normalized.Length
                && !char.IsWhiteSpace(normalized[afterKeyword])
                && normalized[afterKeyword] != '(')
            {
                return false;
            }

            var open = normalized.IndexOf('(', afterKeyword);
            if (open < 0)
                return false;

            var close = FindMatchingPostgresTypeParenthesis(normalized, open);
            if (close <= open)
                return false;

            element = normalized.Substring(open + 1, close - open - 1).Trim().TrimStart('_');
            return element.Length > 0;
        }

        private static int FindMatchingPostgresTypeParenthesis(string value, int open)
        {
            var depth = 0;
            for (var i = open; i < value.Length; i++)
            {
                if (value[i] == '(')
                {
                    depth++;
                }
                else if (value[i] == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }

            return -1;
        }

        private static string StripPostgresTypeArguments(string typeText)
        {
            var open = typeText.IndexOf('(');
            return open < 0 ? typeText : typeText[..open].Trim();
        }
    }
}
