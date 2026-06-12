#nullable enable
using System;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldNameHelper
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
    }
}
