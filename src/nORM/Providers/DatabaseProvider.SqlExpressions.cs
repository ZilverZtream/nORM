using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Configuration;

#nullable enable

namespace nORM.Providers
{
    public abstract partial class DatabaseProvider
    {
        /// <summary>
        /// Character used to escape wildcards in patterns passed to SQL <c>LIKE</c> clauses.
        /// Defaults to a backslash but can be overridden by providers with different
        /// escaping semantics.
        /// </summary>
        public virtual char LikeEscapeChar => '\\';

        /// <summary>
        /// Escapes occurrences of wildcard characters (<c>%</c> and <c>_</c>) in a
        /// pattern so that they are treated as literals in <c>LIKE</c> expressions.
        /// </summary>
        /// <param name="value">The raw pattern value supplied by the user.</param>
        /// <returns>The escaped pattern safe for inclusion in a <c>LIKE</c> clause.</returns>
        public virtual string EscapeLikePattern(string value)
        {
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar).ToString();
            return value
                .Replace(esc, esc + esc)
                .Replace("%", esc + "%")
                .Replace("_", esc + "_");
        }

        /// <summary>
        /// Generates SQL that escapes wildcard characters in a SQL expression for safe use in LIKE patterns.
        /// This is used when the LIKE pattern value comes from a runtime variable (not a constant).
        /// </summary>
        /// <param name="sqlExpression">The SQL expression (parameter reference or column) to escape.</param>
        /// <returns>SQL that escapes the expression for safe use in LIKE patterns.</returns>
        public virtual string GetLikeEscapeSql(string sqlExpression)
        {
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar).ToString();
            // Generate nested REPLACE calls to escape the escape char, %, and _
            // Example: REPLACE(REPLACE(REPLACE(value, '\', '\\'), '%', '\%'), '_', '\_')
            return $"REPLACE(REPLACE(REPLACE({sqlExpression}, '{esc}', '{esc}{esc}'), '%', '{esc}%'), '_', '{esc}_')";
        }

        /// <summary>
        /// Generates SQL that concatenates two SQL expressions. Defaults to ANSI CONCAT function.
        /// Providers that do not support CONCAT (e.g. SQLite) override this method.
        /// </summary>
        /// <param name="left">Left SQL expression.</param>
        /// <param name="right">Right SQL expression.</param>
        /// <returns>SQL fragment that concatenates the two expressions.</returns>
        public virtual string GetConcatSql(string left, string right) => $"CONCAT({left}, {right})";

        /// <summary>
        /// Returns SQL that converts <paramref name="innerSql"/> to its textual representation -
        /// used by the translator for LINQ <c>x.ToString()</c> calls on non-string columns.
        /// Default uses ANSI <c>CAST(x AS VARCHAR)</c>; providers override with their native
        /// text type (NVARCHAR(MAX) on SQL Server, TEXT on SQLite/Postgres, CHAR on MySQL).
        /// </summary>
        public virtual string GetToStringSql(string innerSql) => $"CAST({innerSql} AS VARCHAR)";

        /// <summary>
        /// Wraps a string operand so equality/IN comparisons use ordinal, case-sensitive
        /// semantics even on databases whose default column collation is case-insensitive.
        /// PostgreSQL is already case-sensitive by default, so the base implementation is
        /// identity.
        /// </summary>
        public virtual string ForceCaseSensitiveStringComparison(string sql) => sql;

        /// <summary>
        /// Returns SQL that XORs two integer expressions. SQL Server and MySQL accept the
        /// `^` operator; PostgreSQL uses `#`; SQLite has no XOR operator and falls back to
        /// `(a | b) - (a &amp; b)` - algebraically equivalent on integers.
        /// </summary>
        public virtual string GetBitwiseXorSql(string left, string right) => $"({left} ^ {right})";

        /// <summary>
        /// Wraps a SQL operand for chronological DateTime comparison. SQL Server / Postgres /
        /// MySQL all have native DATETIME types whose comparison operators are timezone- and
        /// offset-aware, so the default is identity. SQLite stores DateTime as TEXT and
        /// comparison is lex-based, which silently mis-orders rows with mixed timezone
        /// offsets (e.g. '+02:00' suffix vs 'Z') -- SqliteProvider overrides to wrap the
        /// operand with <c>datetime(...)</c> for chronological semantics.
        /// </summary>
        public virtual string NormalizeDateTimeForCompare(string sql) => sql;

        /// <summary>
        /// Wraps a SQL operand for numeric decimal comparison / arithmetic / aggregation /
        /// dedup / sort. SQL Server / Postgres / MySQL all have native DECIMAL types whose
        /// operators preserve full decimal precision -- the default is identity. SQLite
        /// stores decimal as TEXT and lex-compares ('10.5' &lt; '2.0' because '1' &lt; '2'),
        /// so SqliteProvider overrides to wrap with <c>CAST({sql} AS REAL)</c>. The REAL
        /// coercion forces numeric semantics but with IEEE-754 binary precision loss --
        /// the standard tradeoff documented across the decimal-cluster fixes.
        /// </summary>
        public virtual string NormalizeDecimalForCompare(string sql) => sql;

        /// <summary>
        /// Wraps a SQL operand for numeric TimeSpan comparison. SQL Server / Postgres /
        /// MySQL store TimeSpan in native TIME / INTERVAL types whose comparison operators
        /// already use numeric ordering - the default is identity. SQLite stores TimeSpan
        /// as canonical 'c' TEXT (<c>"d.hh:mm:ss.fffffff"</c>) and lex-compares, which
        /// silently mis-orders multi-day durations ("10.00:00:00" &lt; "9.23:59:59"
        /// lexicographically but 10 days &gt; 9 days 23 hours). SqliteProvider overrides
        /// to convert the column to fractional seconds via
        /// <see cref="SqliteProvider.TimeSpanColumnTotalSecondsSql"/>.
        /// </summary>
        public virtual string NormalizeTimeSpanForCompare(string sql) => sql;

        /// <summary>
        /// Wraps a SQL operand (a DateTimeOffset column/expression) so ORDER BY / DISTINCT / GROUP BY
        /// sort by the UTC instant, matching <see cref="DateTimeOffset"/>'s instant-based comparison
        /// in LINQ. SQL Server (native <c>datetimeoffset</c>), PostgreSQL (<c>timestamptz</c>) and
        /// MySQL (nORM stores a <c>DATETIME(6)</c> pre-normalised to UTC) already order by instant,
        /// so the default is identity — which also keeps any index on the column usable. SQLite
        /// stores DateTimeOffset as offset-suffixed TEXT, whose lexical order is NOT instant order
        /// when offsets differ, so SqliteProvider overrides to convert to the UTC epoch. The WHERE
        /// path already normalises DateTimeOffset comparisons; without this the ORDER BY silently
        /// disagreed with both WHERE and LINQ.
        /// </summary>
        public virtual string NormalizeDateTimeOffsetForCompare(string sql) => sql;

        /// <summary>
        /// Builds a SQL predicate fragment that is TRUE when <paramref name="colSql"/> is
        /// NULL or an empty string. The default <c>(col IS NULL OR col = '')</c> works on
        /// every provider except SQL Server, where trailing spaces are insignificant in
        /// equality comparisons so <c>'   ' = ''</c> evaluates to TRUE. SQL Server must use
        /// <c>DATALENGTH(col) = 0</c> for a byte-exact empty-string test.
        /// </summary>
        public virtual string IsNullOrEmptySql(string colSql) =>
            $"({colSql} IS NULL OR {colSql} = '')";

        /// <summary>
        /// Formats a numeric SQL expression as a fixed-decimal text with exactly
        /// <paramref name="digits"/> fractional digits, matching .NET's
        /// <c>ToString("F{digits}")</c>. Each provider has a different primitive:
        /// SQLite <c>printf('%.Nf', x)</c>; SQL Server <c>FORMAT(x, 'FN', 'en-US')</c>;
        /// PostgreSQL <c>to_char(x, 'FM999999990.{N zeros}')</c>; MySQL strips the
        /// thousand-separators from <c>FORMAT(x, N)</c>.
        ///
        /// The default base implementation routes to <c>CAST({sql} AS VARCHAR)</c>
        /// which loses the fixed-decimal padding but produces valid SQL on any
        /// provider that doesn't override -- callers should override for
        /// correctness, not depend on the default for production formatting.
        /// </summary>
        public virtual string FormatFixedDecimalSql(string sql, int digits)
            => $"CAST({sql} AS VARCHAR)";

        /// <summary>
        /// Formats a DateTime/DateTimeOffset/DateOnly/TimeOnly SQL expression as
        /// text using a .NET custom date format string (e.g. "yyyy-MM-dd").
        /// SQLite uses <c>strftime('%Y-%m-%d', x)</c>; SQL Server's <c>FORMAT(x,
        /// 'yyyy-MM-dd', 'en-US')</c> accepts .NET-style patterns directly;
        /// PostgreSQL uses <c>to_char(x, 'YYYY-MM-DD')</c>; MySQL uses
        /// <c>DATE_FORMAT(x, '%Y-%m-%d')</c>. Returns null when the format is
        /// not supported by the provider (so callers can fall through).
        /// </summary>
        public virtual string? FormatDateUsingDotNetPattern(string sql, string dotNetFormat)
            => null;

        /// <summary>
        /// Adds a number of seconds (expressed as a SQL fragment, e.g. "3600"
        /// or "(CAST(substr(ts,1,2) AS INTEGER) * 3600 + ...)") to a DateTime
        /// SQL expression. Used by ETSV/SCV for `Stamp + TimeSpan.FromHours(1)`
        /// and constant-TimeSpan shift translation.
        ///
        /// SQLite: <c>RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', col, '+N seconds'), '0'), '.')</c>
        /// SQL Server: <c>DATEADD(SECOND, N, col)</c>
        /// PostgreSQL: <c>(col + (N || ' seconds')::interval)</c>
        /// MySQL: <c>DATE_ADD(col, INTERVAL N SECOND)</c>
        ///
        /// Default returns null so callers can fall through to client-eval.
        /// </summary>
        public virtual string? AddSecondsToDateTimeSql(string dateTimeSql, string secondsSqlFragment)
            => null;

        /// <summary>
        /// Returns SQL that converts a single-character expression to its
        /// integer code point. Used by ETSV's <c>char.IsPunctuation/IsSymbol/
        /// IsControl/GetNumericValue</c> handlers.
        /// SQLite: <c>unicode(c)</c>; SQL Server: <c>UNICODE(c)</c>;
        /// PostgreSQL: <c>ascii(c)</c>; MySQL: <c>ORD(c)</c>.
        /// </summary>
        public virtual string GetCharCodeSql(string charSql) => $"unicode({charSql})";

        /// <summary>
        /// Inverse of <see cref="GetCharCodeSql"/>: converts an integer code point
        /// to a single-character SQL expression. SQLite / SQL Server / MySQL use
        /// <c>CHAR(N)</c>; PostgreSQL uses <c>chr(N)</c>.
        /// </summary>
        public virtual string GetCharFromCodeSql(string codePointSql) => $"CHAR({codePointSql})";

        /// <summary>
        /// Adds N days (a SQL fragment / integer literal) to a DateOnly SQL
        /// expression. Default returns null so callers can fall through to
        /// the existing NormUnsupportedFeatureException pathway.
        ///
        /// SQLite: <c>strftime('%Y-%m-%d', col, '+N days')</c>
        /// SQL Server: <c>DATEADD(DAY, N, col)</c>
        /// PostgreSQL: <c>(col + N)</c> (date + int is native)
        /// MySQL: <c>DATE(DATE_ADD(col, INTERVAL N DAY))</c>
        ///   (DATE_ADD returns DATETIME; DATE() casts back to DATE so the
        ///   materializer reads a DateOnly-compatible value.)
        /// </summary>
        public virtual string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment) => null;

        /// <summary>
        /// Adds N months (a SQL fragment / integer literal) to a DateOnly SQL
        /// expression. Default returns null so callers can fall through.
        ///
        /// SQLite: <c>strftime('%Y-%m-%d', col, '+N months')</c>
        /// SQL Server: <c>DATEADD(MONTH, N, col)</c>
        /// PostgreSQL: <c>(col + N * INTERVAL '1 month')::date</c>
        /// MySQL: <c>DATE(DATE_ADD(col, INTERVAL N MONTH))</c>
        /// </summary>
        public virtual string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment) => null;

        /// <summary>
        /// Adds N years (a SQL fragment / integer literal) to a DateOnly SQL
        /// expression. Default returns null so callers can fall through.
        ///
        /// SQLite: <c>strftime('%Y-%m-%d', col, '+N years')</c>
        /// SQL Server: <c>DATEADD(YEAR, N, col)</c>
        /// PostgreSQL: <c>(col + N * INTERVAL '1 year')::date</c>
        /// MySQL: <c>DATE(DATE_ADD(col, INTERVAL N YEAR))</c>
        /// </summary>
        public virtual string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment) => null;

        /// <summary>
        /// Emits the per-provider aggregate that concatenates the elements of
        /// a group's string-typed column with a separator. Used to translate
        /// <c>g =&gt; string.Join(sep, g.Select(x =&gt; x.Member))</c> inside a
        /// <c>GroupBy</c> projection.
        ///
        /// SQLite: <c>GROUP_CONCAT(expr, sep)</c>
        /// SQL Server (2017+): <c>STRING_AGG(expr, sep)</c>
        /// PostgreSQL: <c>STRING_AGG(expr, sep)</c>
        /// MySQL: <c>GROUP_CONCAT(expr SEPARATOR sep)</c>
        ///
        /// Default routes to STRING_AGG which is portable across SqlServer and
        /// PostgreSQL; SQLite and MySQL override.
        /// </summary>
        public virtual string GetStringAggregateSql(string expr, string sepLiteral)
            => $"STRING_AGG({expr}, {sepLiteral})";

        /// <summary>
        /// Ordered variant: STRING_AGG with an ORDER BY clause inside the aggregate.
        /// SQL Server uses <c>WITHIN GROUP (ORDER BY expression)</c>; Postgres puts ORDER BY
        /// inside the function; MySQL differs - those providers override.
        /// </summary>
        public virtual string GetStringAggregateSql(string expr, string sepLiteral, string orderBySql)
            => $"STRING_AGG({expr}, {sepLiteral}) WITHIN GROUP (ORDER BY {orderBySql})";

        /// <summary>
        /// When true, the translator routes an ordered string-concat aggregate fold
        /// through <see cref="GetStringAggregateSql(string,string,string)"/> and
        /// removes the ORDER BY from the outer SELECT. When false (SQLite &lt; 3.44),
        /// the unordered aggregate form is used and the ORDER BY stays on the outer
        /// SELECT so the query planner's index selection determines GROUP_CONCAT order.
        /// </summary>
        public virtual bool SupportsNativeOrderedStringAggregate => true;

        /// <summary>
        /// Returns SQL that casts <paramref name="innerSql"/> to a boolean.
        /// Default emits <c>CAST(x AS BOOLEAN)</c> which works on Postgres and SQLite.
        /// SQL Server overrides to <c>BIT</c>; MySQL overrides to a comparison.
        /// </summary>
        public virtual string GetBoolCastSql(string innerSql)
            => $"CAST({innerSql} AS BOOLEAN)";

        /// <summary>
        /// Returns SQL that evaluates the .NET <c>Regex.IsMatch(input, pattern)</c>
        /// where <paramref name="patternLiteral"/> is the already-emitted SQL
        /// fragment for the pattern (typically a single-quoted literal or @-param).
        /// PostgreSQL: <c>input ~ pattern</c>.
        /// MySQL: <c>input REGEXP pattern</c>.
        /// SQLite: <c>input REGEXP pattern</c> (requires a user-registered REGEXP
        ///   function on the connection -- Microsoft.Data.Sqlite doesn't supply
        ///   one by default; emitting the operator is still the correct shape).
        /// SQL Server: throws -- T-SQL has no native regex primitive. A CLR
        ///   function with the same surface is the user-supplied workaround.
        /// </summary>
        public virtual string GetRegexMatchSql(string inputSql, string patternLiteral)
            => throw new NormUnsupportedFeatureException(
                $"Regex.IsMatch is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Returns SQL evaluating <c>Regex.Replace(input, pattern, replacement)</c>.
        /// PostgreSQL: <c>regexp_replace(input, pattern, replacement)</c>.
        /// MySQL (8.0+): <c>REGEXP_REPLACE(input, pattern, replacement)</c>.
        /// SQLite: <c>regexp_replace(input, pattern, replacement)</c> (requires a
        ///   user-registered function; Microsoft.Data.Sqlite has no built-in).
        /// SQL Server: throws -- no native regex primitive.
        /// </summary>
        public virtual string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => throw new NormUnsupportedFeatureException(
                $"Regex.Replace is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Case-insensitive variant of <see cref="GetRegexMatchSql"/>. Default
        /// falls back to wrapping both sides in LOWER() which is portable but
        /// loses Unicode case-folding nuance; providers with a native case-
        /// insensitive primitive override (e.g. PostgreSQL <c>~*</c>).
        /// </summary>
        public virtual string GetRegexMatchIgnoreCaseSql(string inputSql, string patternLiteral)
            => GetRegexMatchSql($"LOWER({inputSql})", $"LOWER({patternLiteral})");

        /// <summary>
        /// Case-insensitive variant of <see cref="GetRegexReplaceSql"/>. Default
        /// lowers both input and pattern; the replacement passes through
        /// unchanged (case is preserved on the replacement text). Providers
        /// with a flag-style override (PostgreSQL <c>regexp_replace(... 'gi')</c>,
        /// MySQL <c>REGEXP_REPLACE(... 'i')</c>) can supersede.
        /// </summary>
        public virtual string GetRegexReplaceIgnoreCaseSql(string inputSql, string patternLiteral, string replacementLiteral)
            => GetRegexReplaceSql($"LOWER({inputSql})", $"LOWER({patternLiteral})", replacementLiteral);

        /// <summary>
        /// Portable Gregorian leap-year predicate. Year divisible by 4 but
        /// not by 100, OR divisible by 400. Pure arithmetic; works on every
        /// provider.
        /// </summary>
        protected static string BuildIsLeapYearSql(string yearSql)
            => $"((({yearSql}) % 4 = 0 AND ({yearSql}) % 100 != 0) OR ({yearSql}) % 400 = 0)";

        /// <summary>
        /// Portable DateTime.DaysInMonth lookup. Month-length CASE with the
        /// leap-year exception on February. Pure SQL CASE; works on every
        /// provider.
        /// </summary>
        protected static string BuildDaysInMonthSql(string yearSql, string monthSql)
            => $"(CASE ({monthSql}) " +
               $"WHEN 1 THEN 31 WHEN 3 THEN 31 WHEN 5 THEN 31 WHEN 7 THEN 31 " +
               $"WHEN 8 THEN 31 WHEN 10 THEN 31 WHEN 12 THEN 31 " +
               $"WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30 " +
               $"WHEN 2 THEN (CASE WHEN ({yearSql}) % 4 = 0 AND (({yearSql}) % 100 != 0 OR ({yearSql}) % 400 = 0) THEN 29 ELSE 28 END) " +
               $"END)";

        /// <summary>
        /// Emits the per-provider DATETIME-from-parts primitive used to
        /// translate <c>new DateTime(year, month, day)</c> on column args.
        /// Default throws so each provider opts in with its native function.
        /// Hours / minutes / seconds default to 0 -- the 7-arg DateTime
        /// ctor variants are handled by the same hook with more SQL args.
        /// </summary>
        public virtual string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime(year, month, day) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// 6-arg DATETIME-from-parts variant covering
        /// <c>new DateTime(year, month, day, hour, minute, second)</c>. Default
        /// throws; each provider overrides with its native primitive.
        /// </summary>
        public virtual string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime(year, month, day, hour, minute, second) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// 7-arg DATETIME-from-parts variant covering
        /// <c>new DateTime(year, month, day, hour, minute, second, millisecond)</c>.
        /// Default throws; each provider overrides with its native primitive.
        /// </summary>
        public virtual string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime(year, month, day, hour, minute, second, millisecond) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Per-provider DATE-from-parts primitive for translating
        /// <c>new DateOnly(year, month, day)</c> with column args.
        /// </summary>
        public virtual string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => throw new NormUnsupportedFeatureException(
                $"DateOnly(year, month, day) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Per-provider TIME-from-parts primitive for translating
        /// <c>new TimeOnly(hour, minute, second)</c> with column args.
        /// </summary>
        public virtual string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeOnly(hour, minute, second) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// 4-arg TIME-from-parts variant covering
        /// <c>new TimeOnly(hour, minute, second, millisecond)</c>. Default
        /// throws; each provider overrides with its native primitive.
        /// </summary>
        public virtual string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeOnly(hour, minute, second, millisecond) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Adds N seconds (a SQL fragment) to a TimeOnly SQL expression.
        /// Default returns null so callers can fall through.
        ///
        /// SQLite: strftime over a fake-date-prefixed time text + 'N seconds'
        ///   modifier; result re-formatted to 'HH:mm:ss'.
        /// SQL Server: CAST(DATEADD(SECOND, N, col) AS TIME) -- DATEADD on
        ///   TIME promotes to DATETIME, the CAST brings it back to TIME.
        /// PostgreSQL: (col + (N || ' seconds')::interval) -- TIME + INTERVAL
        ///   = TIME natively.
        /// MySQL: ADDTIME(col, SEC_TO_TIME(N)) -- stays TIME.
        ///
        /// Sub-day arithmetic only; wrap-around past 24h is provider-specific
        /// (SQLite drops, SqlServer wraps via TIME range, etc.).
        /// </summary>
        public virtual string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment) => null;

        /// <summary>
        /// Adds (or subtracts) a TimeSpan-typed column to a TimeOnly SQL
        /// expression. Sister of AddTimeSpanColumnToDateTimeSql. Each provider
        /// uses its TIME storage primitive without parsing text.
        ///
        /// SQLite: extract seconds from TimeSpan-as-text via substr/CAST and
        ///   feed to AddSecondsToTimeOnlySql.
        /// SQL Server: CAST(DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', dur), time) AS TIME).
        /// PostgreSQL: (time +/- dur) -- TIME + INTERVAL is native.
        /// MySQL: ADDTIME / SUBTIME(time, dur) -- both stay TIME.
        /// </summary>
        public virtual string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract) => null;

        /// <summary>
        /// Adds (or subtracts when <paramref name="subtract"/>) a TimeSpan-typed
        /// column expression to a DateTime SQL expression. Differs from
        /// <see cref="AddSecondsToDateTimeSql"/> which takes a numeric seconds
        /// fragment; this one takes the TimeSpan column directly and the
        /// implementation handles the storage format (SQLite stores 'HH:mm:ss'
        /// text and parses; SqlServer / Postgres / MySQL have native
        /// TIME/INTERVAL types).
        ///
        /// SQLite: parse 'HH:mm:ss' substring via substr/CAST and feed to
        /// strftime modifier.
        /// SQL Server: <c>DATEADD(SECOND, [+/-]DATEDIFF(SECOND, '00:00:00', col), dt)</c>
        /// PostgreSQL: <c>(dt + col)</c> / <c>(dt - col)</c> (native interval)
        /// MySQL: <c>DATE_ADD(dt, INTERVAL [+/-]TIME_TO_SEC(col) SECOND)</c>
        ///
        /// Default returns null so callers can fall through.
        /// </summary>
        public virtual string? AddTimeSpanColumnToDateTimeSql(string dateTimeSql, string timeSpanColumnSql, bool subtract)
            => null;

        /// <summary>
        /// Sister of <see cref="AddTimeSpanColumnToDateTimeSql"/> for
        /// DateTimeOffset operands. Default delegates to the DateTime hook;
        /// providers that store DTOs as offset-suffixed text (SQLite) override
        /// to preserve the original offset on the result so the materialiser
        /// round-trips with the same wall-clock rendering rather than re-
        /// interpreting the suffixless result as Local.
        /// </summary>
        public virtual string? AddTimeSpanColumnToDateTimeOffsetSql(string dtoSql, string timeSpanColumnSql, bool subtract)
            => AddTimeSpanColumnToDateTimeSql(dtoSql, timeSpanColumnSql, subtract);

        /// <summary>
        /// Returns SQL that evaluates a TimeSpan column as fractional seconds (double).
        /// Used by the LINQ projection-side translator to lower
        /// <c>col1 + col2</c> / <c>col1 - col2</c> between two TimeSpan columns into
        /// a portable scalar; the materialiser reads the resulting double and
        /// reconstructs the TimeSpan via <c>TimeSpan.FromSeconds</c>.
        ///
        /// Default throws because providers store TimeSpan in dialect-specific
        /// formats (SQLite TEXT, SqlServer TIME(7), Postgres INTERVAL, MySQL TIME);
        /// each provider supplies its own emit.
        /// </summary>
        public virtual string GetTimeSpanColumnSecondsSql(string timeSpanColumnSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeSpan column arithmetic is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Returns SQL evaluating <paramref name="dtoSql"/> (a DateTimeOffset column or
        /// expression) re-rendered at <paramref name="offset"/>. Implements
        /// <see cref="DateTimeOffset.ToOffset(TimeSpan)"/>: the UTC instant is invariant,
        /// only the wall-clock representation and trailing offset suffix change.
        /// Default throws; each provider overrides with a path tailored to its
        /// storage shape (native DATETIMEOFFSET on SqlServer, ISO-8601 text
        /// elsewhere).
        /// </summary>
        public virtual string GetDateTimeOffsetWithOffsetSql(string dtoSql, TimeSpan offset)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset.ToOffset is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as the wall-clock DateTime at
        /// <paramref name="localOffset"/> - i.e. the value of
        /// <see cref="DateTimeOffset.LocalDateTime"/> for one concrete local
        /// offset. Query translation composes this provider hook inside a
        /// generated timezone-offset range CASE expression so each row uses the
        /// correct per-instant local offset where nORM owns the full expression.
        /// Result is the date+time portion only (no offset suffix), readable as
        /// a <see cref="DateTime"/> by the materialiser.
        /// </summary>
        public virtual string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset.LocalDateTime is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as the integer count of seconds
        /// since the Unix epoch (UTC). Kept as a provider extension point and as a
        /// fallback for custom providers; built-in DateTimeOffset equality and
        /// subtraction use the microsecond hook below.
        /// </summary>
        public virtual string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset UTC-instant comparison is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as milliseconds since the Unix
        /// epoch (UTC). Used internally by timezone-range lowering where transition
        /// boundaries do not need sub-millisecond precision.
        /// </summary>
        internal virtual string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"(({GetDateTimeOffsetUtcEpochSecondsSql(dtoSql)}) * 1000)";

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as microseconds since the Unix
        /// epoch (UTC). Built-in DateTimeOffset equality and subtraction use this
        /// as the common precision floor; 100ns .NET ticks are still not portable
        /// across all providers.
        /// </summary>
        internal virtual string GetDateTimeOffsetUtcEpochMicrosecondsSql(string dtoSql)
            => $"(({GetDateTimeOffsetUtcEpochMillisecondsSql(dtoSql)}) * 1000)";

        /// <summary>
        /// SQL evaluating the UTC-instant difference between two DateTimeOffset
        /// expressions as fractional seconds. Providers may override to force
        /// floating arithmetic where native decimal division would round away
        /// microsecond precision.
        /// </summary>
        internal virtual string GetDateTimeOffsetDifferenceSecondsSql(string leftSql, string rightSql)
            => $"(({GetDateTimeOffsetUtcEpochMicrosecondsSql(leftSql)} - {GetDateTimeOffsetUtcEpochMicrosecondsSql(rightSql)}) / 1000000.0)";

        /// <summary>
        /// Returns SQL that parses <paramref name="innerSql"/> (a textual expression) as a
        /// 32- or 64-bit signed integer. Used to translate <c>int.Parse(col)</c> /
        /// <c>long.Parse(col)</c>. Most providers accept ANSI <c>CAST(x AS INTEGER)</c>;
        /// MySQL requires <c>CAST(x AS SIGNED)</c> instead - override on the MySQL provider.
        /// </summary>
        public virtual string GetIntCastSql(string innerSql, bool asLong = false)
            => $"CAST({innerSql} AS {(asLong ? "BIGINT" : "INTEGER")})";

        /// <summary>
        /// Returns SQL that truncates a numeric expression toward zero and returns
        /// an integer. Used for TimeSpan component extraction where .NET truncates
        /// fractional totals before modulo arithmetic.
        /// </summary>
        public virtual string GetTruncateToIntSql(string numericSql)
            => $"CAST({numericSql} AS INTEGER)";

        /// <summary>
        /// Returns SQL that parses <paramref name="innerSql"/> as a floating-point /
        /// fixed-precision number. Used to translate <c>double.Parse(col)</c> and
        /// <c>decimal.Parse(col)</c>. Defaults to ANSI <c>CAST(x AS DOUBLE PRECISION)</c>
        /// for <paramref name="asDecimal"/>=false and <c>CAST(x AS DECIMAL(38, 10))</c>
        /// otherwise. Providers without DOUBLE PRECISION or with stricter DECIMAL
        /// syntax override.
        /// </summary>
        public virtual string GetRealCastSql(string innerSql, bool asDecimal = false)
            => asDecimal
                ? $"CAST({innerSql} AS DECIMAL(38, 10))"
                : $"CAST({innerSql} AS DOUBLE PRECISION)";

        /// <summary>
        /// Returns an INSERT statement for a join table row that does nothing (ignores) on duplicate key.
        /// Providers override this for their native upsert/ignore syntax.
        /// </summary>
        /// <param name="escTable">Escaped join table name.</param>
        /// <param name="escC1">Escaped left FK column name.</param>
        /// <param name="escC2">Escaped right FK column name.</param>
        /// <param name="p1">Parameter placeholder for the left FK value.</param>
        /// <param name="p2">Parameter placeholder for the right FK value.</param>
        public virtual string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT INTO {escTable} ({escC1}, {escC2}) SELECT {p1}, {p2} WHERE NOT EXISTS (SELECT 1 FROM {escTable} WHERE {escC1} = {p1} AND {escC2} = {p2})";

    }
}
