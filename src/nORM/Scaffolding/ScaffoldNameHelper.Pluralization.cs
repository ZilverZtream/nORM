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

            if (LooksPlural(bareName))
                return name;

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

        public static string Singularize(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return "Item";

            var verbatimPrefix = name.StartsWith("@", StringComparison.Ordinal) ? "@" : string.Empty;
            var bareName = verbatimPrefix.Length == 0 ? name : name[1..];
            if (bareName.Length == 0)
                return "Item";

            if (TrySingularizeIrregular(bareName, out var irregular))
                return verbatimPrefix + irregular;

            if (IsUninflected(bareName) || IsLikelySingularTrailingS(bareName))
                return name;

            if (bareName.EndsWith("ies", StringComparison.OrdinalIgnoreCase)
                && bareName.Length > 3
                && !"aeiou".Contains(char.ToLowerInvariant(bareName[^4]), StringComparison.Ordinal))
            {
                return verbatimPrefix + bareName[..^3] + MatchCaseSuffix(bareName[^1], "y");
            }

            if (bareName.EndsWith("ves", StringComparison.OrdinalIgnoreCase) && bareName.Length > 3)
            {
                if (bareName.EndsWith("lves", StringComparison.OrdinalIgnoreCase)
                    || bareName.EndsWith("rves", StringComparison.OrdinalIgnoreCase))
                {
                    return verbatimPrefix + bareName[..^3] + MatchCaseSuffix(bareName[^1], "f");
                }

                return verbatimPrefix + bareName[..^1];
            }

            if (bareName.EndsWith("ches", StringComparison.OrdinalIgnoreCase)
                || bareName.EndsWith("shes", StringComparison.OrdinalIgnoreCase)
                || bareName.EndsWith("sses", StringComparison.OrdinalIgnoreCase)
                || bareName.EndsWith("xes", StringComparison.OrdinalIgnoreCase)
                || bareName.EndsWith("zes", StringComparison.OrdinalIgnoreCase)
                || bareName.EndsWith("oes", StringComparison.OrdinalIgnoreCase)
                || bareName.EndsWith("ses", StringComparison.OrdinalIgnoreCase))
            {
                return verbatimPrefix + bareName[..^2];
            }

            if (bareName.EndsWith("s", StringComparison.OrdinalIgnoreCase) && bareName.Length > 1)
                return verbatimPrefix + bareName[..^1];

            return name;
        }

    }
}
