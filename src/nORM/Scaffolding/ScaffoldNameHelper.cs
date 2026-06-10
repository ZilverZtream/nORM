#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static class ScaffoldNameHelper
    {
        public static string ToPascalCase(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return name;

            var sb = new StringBuilder(name.Length);
            var start = 0;
            for (var i = 0; i <= name.Length; i++)
            {
                if (i < name.Length && char.IsLetterOrDigit(name[i]))
                    continue;

                AppendPascalSegment(sb, name.AsSpan(start, i - start));
                start = i + 1;
            }

            return sb.ToString();
        }

        public static string ToScaffoldClrName(string databaseName, bool useDatabaseNames)
            => EscapeCSharpIdentifier(useDatabaseNames ? databaseName : ToPascalCase(databaseName));

        public static string ToScaffoldClrNamePart(string databaseName, bool useDatabaseNames)
        {
            var name = ToScaffoldClrName(databaseName, useDatabaseNames);
            return name.StartsWith("@", StringComparison.Ordinal) ? name[1..] : name;
        }

        public static string ToNavigationName(string generatedName)
            => EscapeCSharpIdentifier(ToPascalCase(generatedName));

        public static string EscapeCSharpIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                return "_";

            if (identifier[0] == '@' && IsValidEscapedCSharpIdentifier(identifier))
                return identifier;

            var sb = new StringBuilder(identifier.Length + 1);
            for (var i = 0; i < identifier.Length; i++)
            {
                var ch = identifier[i];
                var valid = i == 0
                    ? char.IsLetter(ch) || ch == '_'
                    : char.IsLetterOrDigit(ch) || ch == '_';

                if (valid)
                    sb.Append(ch);
                else if (i == 0 && char.IsDigit(ch))
                    sb.Append('_').Append(ch);
                else
                    sb.Append('_');
            }

            if (sb.Length == 0)
                sb.Append('_');

            var escaped = sb.ToString();
            return CSharpKeywords.Contains(escaped) ? "@" + escaped : escaped;
        }

        public static bool IsValidNamespaceName(string namespaceName)
        {
            if (string.IsNullOrWhiteSpace(namespaceName))
                return false;

            foreach (var segment in namespaceName.Split('.'))
            {
                if (!IsValidNamespaceSegment(segment))
                    return false;
            }

            return true;
        }

        public static string MakeUniqueContextName(string contextName, IEnumerable<string> entityNames)
        {
            var existingNames = new HashSet<string>(entityNames, StringComparer.OrdinalIgnoreCase);
            if (!existingNames.Contains(contextName))
                return contextName;

            var preferred = contextName.EndsWith("Context", StringComparison.Ordinal)
                ? contextName
                : contextName + "Context";
            return MakeUnique(preferred, existingNames);
        }

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

        private static void AppendPascalSegment(StringBuilder sb, ReadOnlySpan<char> segment)
        {
            if (segment.IsEmpty)
                return;

            var hasLower = false;
            for (var i = 0; i < segment.Length; i++)
            {
                if (char.IsLower(segment[i]))
                {
                    hasLower = true;
                    break;
                }
            }

            sb.Append(char.ToUpperInvariant(segment[0]));
            for (var i = 1; i < segment.Length; i++)
                sb.Append(hasLower ? segment[i] : char.ToLowerInvariant(segment[i]));
        }

        private static bool IsValidEscapedCSharpIdentifier(string identifier)
        {
            if (identifier.Length == 1)
                return false;

            var first = identifier[1];
            if (!(char.IsLetter(first) || first == '_'))
                return false;

            for (var i = 2; i < identifier.Length; i++)
            {
                var ch = identifier[i];
                if (!(char.IsLetterOrDigit(ch) || ch == '_'))
                    return false;
            }

            return true;
        }

        private static bool IsValidNamespaceSegment(string segment)
        {
            if (string.IsNullOrWhiteSpace(segment))
                return false;

            var start = segment[0] == '@' ? 1 : 0;
            if (start == segment.Length)
                return false;

            if (!(char.IsLetter(segment[start]) || segment[start] == '_'))
                return false;

            for (var i = start + 1; i < segment.Length; i++)
            {
                if (!(char.IsLetterOrDigit(segment[i]) || segment[i] == '_'))
                    return false;
            }

            if (start == 0 && CSharpKeywords.Contains(segment))
                return false;

            return true;
        }

        public static string MakeUnique(string baseName, HashSet<string> existingNames)
        {
            var candidate = EscapeCSharpIdentifier(string.IsNullOrWhiteSpace(baseName) ? "_" : baseName);
            var unique = candidate;
            var suffix = 2;
            while (existingNames.Contains(unique))
            {
                unique = candidate + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
                suffix++;
            }

            existingNames.Add(unique);
            return unique;
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

        private static readonly HashSet<string> CSharpKeywords = new(StringComparer.Ordinal)
        {
            "abstract","as","base","bool","break","byte","case","catch","char","checked","class","const",
            "continue","decimal","default","delegate","do","double","else","enum","event","explicit","extern",
            "false","finally","fixed","float","for","foreach","goto","if","implicit","in","int","interface",
            "internal","is","lock","long","namespace","new","null","object","operator","out","override","params",
            "private","protected","public","readonly","ref","return","sbyte","sealed","short","sizeof","stackalloc",
            "static","string","struct","switch","this","throw","true","try","typeof","uint","ulong","unchecked",
            "unsafe","ushort","using","virtual","void","volatile","while",
            "record","partial","var","dynamic","async","await","nameof","when","and","or","not","with",
            "init","required","file","scoped","global","managed","unmanaged","nint","nuint","value","yield"
        };
    }
}
