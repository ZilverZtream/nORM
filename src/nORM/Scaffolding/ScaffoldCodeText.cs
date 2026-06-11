#nullable enable
using System.Text;

namespace nORM.Scaffolding
{
    internal static class ScaffoldCodeText
    {
        public static string EscapeStringLiteral(string value)
            => value
                .Replace("\\", "\\\\")
                .Replace("\"", "\\\"")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        public static string EscapeXmlDocumentation(string value)
            => value
                .Replace("&", "&amp;")
                .Replace("<", "&lt;")
                .Replace(">", "&gt;")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        public static void AppendXmlSummary(StringBuilder sb, string indent, string value)
        {
            sb.AppendLine(indent + "/// <summary>");
            sb.AppendLine(indent + "/// " + EscapeXmlDocumentation(value));
            sb.AppendLine(indent + "/// </summary>");
        }
    }
}
