#nullable enable
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldNameHelper
    {
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
    }
}
