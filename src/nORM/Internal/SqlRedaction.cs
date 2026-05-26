using System.Text.RegularExpressions;

namespace nORM.Internal
{
    /// <summary>
    /// Pure string-transformation helpers for redacting SQL literals before logging.
    /// This class contains no reflection or dynamic-code paths and may be called freely
    /// from code that is not annotated with RequiresDynamicCode or RequiresUnreferencedCode.
    /// </summary>
    internal static class SqlRedaction
    {
        private static readonly Regex SingleQuoteLiteralRegex = new(@"N?'(?:[^']|'')*'", RegexOptions.Compiled);
        private static readonly Regex DollarQuoteLiteralRegex = new(@"\$(\w*)\$.*?\$\1\$", RegexOptions.Compiled | RegexOptions.Singleline);

        /// <summary>
        /// Replaces SQL string literals with <c>'[redacted]'</c> to prevent credentials,
        /// PII, or other sensitive data from appearing in logs.
        /// Handles single-quoted strings (ANSI SQL and SQL Server N'...' form) and
        /// PostgreSQL dollar-quoted blocks (bare <c>$$...$$</c> and tagged <c>$tag$...$tag$</c>).
        /// </summary>
        internal static string RedactForLogging(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;
            var step1 = SingleQuoteLiteralRegex.Replace(sql, "'[redacted]'");
            return DollarQuoteLiteralRegex.Replace(step1, "'[redacted]'");
        }
    }
}
