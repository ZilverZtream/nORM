using System;
using System.Text.RegularExpressions;

namespace nORM.Migration
{
    /// <summary>
    /// Validates that a <c>DefaultValue</c> string is a safe SQL literal or well-known SQL function
    /// before it is interpolated verbatim into DDL by migration SQL generators.
    ///
    /// M1 root cause: migration generators interpolated <c>ColumnSchema.DefaultValue</c> directly
    /// into DDL strings with no validation, allowing an attacker-controlled value to inject
    /// arbitrary SQL into migration scripts (e.g., <c>0; DROP TABLE Users--</c>).
    ///
    /// Fix: this allowlist permits only:
    /// <list type="bullet">
    ///   <item>SQL NULL literal</item>
    ///   <item>Boolean keywords: TRUE, FALSE</item>
    ///   <item>Integer and decimal numeric literals (optional leading minus)</item>
    ///   <item>Hex/binary literals: 0xDEADBEEF and X'DEADBEEF'</item>
    ///   <item>Single-quoted ANSI/Unicode string literals with SQL-escaped interior quotes</item>
    ///   <item>Literal-only string normalization defaults: LOWER('value') and UPPER('value')</item>
    ///   <item>Standard SQL no-argument functions: CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME,
    ///         CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(6), CURRENT_DATE(),
    ///         CURRENT_TIME(), CURRENT_TIME(6), LOCALTIME, LOCALTIME(6),
    ///         LOCALTIMESTAMP, LOCALTIMESTAMP(6), CURRENT_USER,
    ///         NOW(), NOW(6), GETDATE(), GETUTCDATE(), NEWID(), NEWSEQUENTIALID(), UUID(),
    ///         GEN_RANDOM_UUID(), UUID_GENERATE_V4(), SYSDATE(), SYSDATE(6), SYSDATETIME(), SYSUTCDATETIME(),
    ///         SYSDATETIMEOFFSET(), UTC_TIMESTAMP(), UTC_TIMESTAMP(6), RANDOM(), LAST_INSERT_ID(),
    ///         CLOCK_TIMESTAMP(), TRANSACTION_TIMESTAMP(), NEXTVAL('sequence') and
    ///         NEXTVAL('schema.sequence'::regclass)</item>
    ///   <item>Strict PostgreSQL UTC timestamp defaults:
    ///         NOW() AT TIME ZONE 'utc', CURRENT_TIMESTAMP AT TIME ZONE 'utc',
    ///         and TIMEZONE('utc', NOW())</item>
    ///   <item>Safe PostgreSQL cast suffixes after one of those literals/functions,
    ///         such as 'draft'::text, 42::integer, or now()::timestamp without time zone</item>
    /// </list>
    /// All other values (including any string containing semicolons, comments, unbalanced quotes,
    /// or keywords such as DROP/SELECT/INSERT) are rejected with <see cref="ArgumentException"/>.
    /// </summary>
    internal static class DefaultValueValidator
    {
        // Anchored allowlist: the entire value must match one of the permitted forms.
        // NOTE: \z is used (not $) because $ in .NET matches before a trailing \n; \z is absolute end-of-string.
        private static readonly Regex _safe = new(
            @"^(?:" +
            @"null" +                                                   // SQL NULL
            @"|true|false" +                                            // boolean keywords
            @"|-?[0-9]+(?:\.[0-9]+)?" +                                 // numeric literal (int or decimal)
            @"|0x[0-9a-f]+|x'(?:[0-9a-f]{2})*'" +                       // provider binary/hex literals
            @"|n?'(?:[^']|'')*'" +                                       // single-quoted ANSI/Unicode string literal
            @"|(?:lower|upper)\s*\(\s*n?'(?:[^']|'')*'\s*\)" +           // literal-only string normalization functions
            @"|current_timestamp(?:\([0-6]?\))?|current_date(?:\(\))?|current_time(?:\([0-6]?\))?" + // ANSI standard date/time functions
            @"|localtime(?:\([0-6]?\))?|localtimestamp(?:\([0-6]?\))?" + // H: ANSI local date/time keywords
            @"|current_user" +                                          // H: ANSI current user keyword
            @"|now\([0-6]?\)|getdate\(\)|getutcdate\(\)" +              // common date functions
            @"|newid\(\)|newsequentialid\(\)" +                         // SQL Server UUID generators
            @"|uuid\(\)|gen_random_uuid\(\)|uuid_generate_v4\(\)" +       // PostgreSQL / MySQL UUID generators
            @"|sysdate\([0-6]?\)|sysdatetime\(\)|sysutcdatetime\(\)|sysdatetimeoffset\(\)" + // Oracle / SQL Server variants
            @"|utc_timestamp\([0-6]?\)" +                                // MySQL UTC timestamp
            @"|random\(\)" +                                            // H: SQLite / PostgreSQL random value
            @"|last_insert_id\(\)" +                                    // H: MySQL last inserted row ID
            @"|clock_timestamp\(\)|transaction_timestamp\(\)" +          // H: PostgreSQL clock functions
            @"|(?:now\(\)|current_timestamp(?:\([0-6]?\))?)\s+at\s+time\s+zone\s+'utc'(?:\s*::\s*text)?" + // H: PostgreSQL UTC timestamp defaults
            @"|timezone\s*\(\s*'utc'(?:\s*::\s*text)?\s*,\s*(?:now\(\)|current_timestamp(?:\([0-6]?\))?)\s*\)" +
            // H: PostgreSQL/MySQL NEXTVAL — allow only simple optional schema-qualified identifiers.
            @"|nextval\s*\(\s*'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?'\s*(?:::regclass)?\s*\)" +
            @")(?:\s*::\s*(?:" +
            @"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?(?:\s*\(\s*[0-9]+(?:\s*,\s*[0-9]+)?\s*\))?(?:\[\])?" +
            @"|character\s+varying(?:\s*\(\s*[0-9]+\s*\))?" +
            @"|timestamp\s+(?:with|without)\s+time\s+zone" +
            @"|time\s+(?:with|without)\s+time\s+zone" +
            @"|double\s+precision" +
            @"))?\z",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Returns the trimmed form of <paramref name="value"/> if it is a safe SQL default literal or
        /// well-known SQL function call. Surrounding whitespace is stripped before validation and the
        /// trimmed value is returned. Passes <c>null</c> through unchanged (null means
        /// no DEFAULT clause will be emitted).
        /// </summary>
        /// <param name="value">The raw DefaultValue string from <see cref="ColumnSchema"/>.</param>
        /// <returns>The validated, whitespace-trimmed value, or <c>null</c> when <paramref name="value"/> is <c>null</c>.</returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="value"/> is not null and does not match the allowlist.
        /// </exception>
        public static string? Validate(string? value)
        {
            if (value is null)
                return null;

            var trimmed = value.Trim();
            if (!_safe.IsMatch(trimmed))
                throw new ArgumentException(
                    $"DefaultValue '{value}' is not a permitted SQL literal. " +
                    "Only numeric literals, single-quoted ANSI/Unicode strings, boolean literals (TRUE/FALSE), NULL, " +
                    "safe hex/binary literals, literal-only LOWER/UPPER string normalization defaults, " +
                    "standard SQL functions (CURRENT_TIMESTAMP, NOW(), GETDATE(), SYSUTCDATETIME(), NEWID(), UUID(), etc.), " +
                    "and safe PostgreSQL cast suffixes on those values are allowed. " +
                    "Values containing semicolons, comments, or DML keywords are rejected.");

            return trimmed;
        }
    }
}
