#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSemicolonParser
    {
        private static List<SemicolonValueMarker> FindSemicolonValueMarkers(string detail)
        {
            var markers = new List<SemicolonValueMarker>();
            char? quote = null;
            string? dollarQuote = null;
            for (var i = 0; i < detail.Length; i++)
            {
                var ch = detail[i];
                if (dollarQuote is not null)
                {
                    ScaffoldSqlMetadataParser.TryAdvancePostgresDollarQuote(detail, ref i, ref dollarQuote);
                    continue;
                }

                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < detail.Length && detail[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ScaffoldSqlMetadataParser.TryAdvancePostgresDollarQuote(detail, ref i, ref dollarQuote))
                    continue;

                if (ch == ';' && TryReadSemicolonValueMarker(detail, i, out var key, out var valueStart))
                    markers.Add(new SemicolonValueMarker(i, key, valueStart));
            }

            return markers;
        }

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
