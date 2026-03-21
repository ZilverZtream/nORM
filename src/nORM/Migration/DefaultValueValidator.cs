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
    ///   <item>Single-quoted ANSI string literals with SQL-escaped interior quotes</item>
    ///   <item>Standard SQL no-argument functions: CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME,
    ///         LOCALTIME, LOCALTIMESTAMP, CURRENT_USER,
    ///         NOW(), GETDATE(), GETUTCDATE(), NEWID(), NEWSEQUENTIALID(), UUID(),
    ///         GEN_RANDOM_UUID(), SYSDATE(), SYSDATETIME(), RANDOM(), LAST_INSERT_ID(),
    ///         CLOCK_TIMESTAMP(), NEXTVAL</item>
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
            @"|'(?:[^']|'')*'" +                                        // single-quoted ANSI string literal
            @"|current_timestamp|current_date|current_time" +           // ANSI standard date/time functions
            @"|localtime|localtimestamp" +                              // H: ANSI local date/time keywords
            @"|current_user" +                                          // H: ANSI current user keyword
            @"|now\(\)|getdate\(\)|getutcdate\(\)" +                    // common date functions
            @"|newid\(\)|newsequentialid\(\)" +                         // SQL Server UUID generators
            @"|uuid\(\)|gen_random_uuid\(\)" +                          // PostgreSQL / MySQL UUID generators
            @"|sysdate\(\)|sysdatetime\(\)" +                           // Oracle / SQL Server variants
            @"|random\(\)" +                                            // H: SQLite / PostgreSQL random value
            @"|last_insert_id\(\)" +                                    // H: MySQL last inserted row ID
            @"|clock_timestamp\(\)" +                                   // H: PostgreSQL wall-clock timestamp
            // H: PostgreSQL/MySQL NEXTVAL — handles both NEXTVAL('seq') and NEXTVAL( 'seq' ) with optional spaces
            @"|nextval\s*\(\s*'[^']*'\s*\)" +                          // sequence nextval (Postgres + MySQL)
            @")\z",
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
                    "Only numeric literals, single-quoted strings, boolean literals (TRUE/FALSE), NULL, " +
                    "and standard SQL functions (CURRENT_TIMESTAMP, NOW(), GETDATE(), NEWID(), UUID(), etc.) are allowed. " +
                    "Values containing semicolons, comments, or DML keywords are rejected.");

            return trimmed;
        }
    }
}
