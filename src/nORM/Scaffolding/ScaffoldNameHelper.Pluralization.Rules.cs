#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldNameHelper
    {
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

        private static bool TrySingularizeIrregular(string name, out string singular)
        {
            var lower = name.ToLowerInvariant();
            singular = lower switch
            {
                "people" => MatchCase(name, "person"),
                "children" => MatchCase(name, "child"),
                "men" => MatchCase(name, "man"),
                "women" => MatchCase(name, "woman"),
                "mice" => MatchCase(name, "mouse"),
                "geese" => MatchCase(name, "goose"),
                "teeth" => MatchCase(name, "tooth"),
                "feet" => MatchCase(name, "foot"),
                _ => string.Empty
            };

            return singular.Length > 0;
        }

        private static bool LooksPlural(string name)
            => TrySingularizeIrregular(name, out _)
               || IsUninflected(name)
               || name.EndsWith("ies", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("ves", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("ches", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("shes", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("sses", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("xes", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("zes", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("oes", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("ses", StringComparison.OrdinalIgnoreCase)
               || (name.EndsWith("s", StringComparison.OrdinalIgnoreCase)
                   && !IsLikelySingularTrailingS(name));

        private static bool IsUninflected(string name)
        {
            var lower = name.ToLowerInvariant();
            return lower is "data"
                or "info"
                or "metadata"
                or "news"
                or "series"
                or "species"
                or "equipment";
        }

        private static bool IsLikelySingularTrailingS(string name)
            => name.EndsWith("ss", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("us", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("is", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("ous", StringComparison.OrdinalIgnoreCase)
               || name.EndsWith("vas", StringComparison.OrdinalIgnoreCase);

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
