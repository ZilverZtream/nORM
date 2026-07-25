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
        // Bounded MatchTimeout as defense-in-depth: inputs are MaxSqlLength-capped so there is no practical
        // ReDoS, but the dollar-quote pattern uses a backreference (\1) with .*? and could backtrack, so a
        // pathological input fails closed (a timeout surfaces) rather than spinning.
        private static readonly System.TimeSpan RedactionRegexTimeout = System.TimeSpan.FromSeconds(1);
        private static readonly Regex SingleQuoteLiteralRegex = new(@"N?'(?:[^']|'')*'", RegexOptions.Compiled, RedactionRegexTimeout);
        private static readonly Regex DollarQuoteLiteralRegex = new(@"\$(\w*)\$.*?\$\1\$", RegexOptions.Compiled | RegexOptions.Singleline, RedactionRegexTimeout);

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
