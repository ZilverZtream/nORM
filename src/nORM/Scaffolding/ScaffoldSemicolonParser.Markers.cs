#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSemicolonParser
    {
        private static bool TryReadSemicolonValueMarker(
            string detail,
            int semicolonIndex,
            out string key,
            out int valueStart)
        {
            key = string.Empty;
            valueStart = 0;

            var index = semicolonIndex + 1;
            var sawWhitespace = false;
            while (index < detail.Length && char.IsWhiteSpace(detail[index]))
            {
                sawWhitespace = true;
                index++;
            }

            if (!sawWhitespace || index >= detail.Length || !char.IsLetter(detail[index]))
                return false;

            var keyStart = index;
            while (index < detail.Length && char.IsLetterOrDigit(detail[index]))
                index++;

            var keyEnd = index;
            while (index < detail.Length && char.IsWhiteSpace(detail[index]))
                index++;

            if (index >= detail.Length || detail[index] != '=')
                return false;

            key = detail.Substring(keyStart, keyEnd - keyStart);
            if (!IsKnownSemicolonValueKey(key))
                return false;

            valueStart = index + 1;
            return true;
        }
    }
}
