#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldNameHelper
    {
        public static string Pluralize(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return "Items";

            var verbatimPrefix = name.StartsWith("@", StringComparison.Ordinal) ? "@" : string.Empty;
            var bareName = verbatimPrefix.Length == 0 ? name : name[1..];
            if (bareName.Length == 0)
                return "Items";

            if (TryPluralizeIrregular(bareName, out var irregular))
                return verbatimPrefix + irregular;

            if (ShouldUseRowsSuffixForQueryProperty(bareName))
                return name + "Rows";

            if (bareName.EndsWith("f", StringComparison.OrdinalIgnoreCase) && bareName.Length > 1)
                return verbatimPrefix + bareName[..^1] + MatchCaseSuffix(bareName[^1], "ves");

            if (bareName.EndsWith("fe", StringComparison.OrdinalIgnoreCase) && bareName.Length > 2)
                return verbatimPrefix + bareName[..^2] + MatchCaseSuffix(bareName[^1], "ves");

            if (name.EndsWith("y", StringComparison.OrdinalIgnoreCase)
                && name.Length > 1
                && !"aeiou".Contains(char.ToLowerInvariant(name[^2]), StringComparison.Ordinal))
            {
                return name[..^1] + "ies";
            }

            if (name.EndsWith("s", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("x", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("z", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("ch", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("sh", StringComparison.OrdinalIgnoreCase))
            {
                return name + "es";
            }

            return name + "s";
        }

        private static bool TryPluralizeIrregular(string name, out string plural)
        {
            var lower = name.ToLowerInvariant();
            plural = lower switch
            {
                "person" => MatchCase(name, "people"),
                "child" => MatchCase(name, "children"),
                "man" => MatchCase(name, "men"),
                "woman" => MatchCase(name, "women"),
                "mouse" => MatchCase(name, "mice"),
                "goose" => MatchCase(name, "geese"),
                "tooth" => MatchCase(name, "teeth"),
                "foot" => MatchCase(name, "feet"),
                _ => string.Empty
            };

            return plural.Length > 0;
        }

        private static bool ShouldUseRowsSuffixForQueryProperty(string name)
            => name.EndsWith("ed", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("data", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("info", StringComparison.OrdinalIgnoreCase);

        private static string MatchCase(string source, string lowerPlural)
        {
            if (source.All(char.IsUpper))
                return lowerPlural.ToUpperInvariant();

            if (char.IsUpper(source[0]))
                return char.ToUpperInvariant(lowerPlural[0]) + lowerPlural[1..];

            return lowerPlural;
        }

        private static string MatchCaseSuffix(char source, string lowerSuffix)
            => char.IsUpper(source) ? lowerSuffix.ToUpperInvariant() : lowerSuffix;
    }
}
