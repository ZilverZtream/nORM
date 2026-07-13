using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class PostgresProvider
    {
        /// <summary>PostgreSQL TEXT is the natural target for numeric/Guid/DateTime ToString().</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS TEXT)";

        /// <summary>PostgreSQL numeric-to-integer casts round; TRUNC preserves .NET TimeSpan component semantics.</summary>
        public override string GetTruncateToIntSql(string numericSql)
            => $"CAST(TRUNC({numericSql}) AS INTEGER)";

        /// <summary>Postgres puts ORDER BY inside the aggregate function arguments.</summary>
        public override string GetStringAggregateSql(string expr, string sepLiteral, string orderBySql)
            => $"STRING_AGG({expr}, {sepLiteral} ORDER BY {orderBySql})";

        /// <summary>
        /// PostgreSQL uses <c>to_char(x, 'FM999999999990.{N zeros}')</c> for fixed-
        /// decimal text. 'FM' strips leading whitespace; '9' is optional digit, '0'
        /// is mandatory digit (forces trailing zero padding for the fractional part).
        /// For digits=0 the mask is just integer-only.
        /// </summary>
        public override string FormatFixedDecimalSql(string sql, int digits)
        {
            if (digits <= 0)
                return $"to_char({sql}, 'FM999999999990')";
            var fracMask = new string('0', digits);
            return $"to_char({sql}, 'FM999999999990.{fracMask}')";
        }

        /// <summary>
        /// PostgreSQL uses <c>to_char(x, 'YYYY-MM-DD')</c> with its own token set
        /// (different from strftime / .NET): YYYY year, MM month, DD day, HH24
        /// hour, MI minute, SS second. Converts the .NET pattern to PostgreSQL
        /// tokens; returns null if any unsupported token appears.
        /// </summary>
        public override string? FormatDateUsingDotNetPattern(string sql, string dotNetFormat)
        {
            var sb = new System.Text.StringBuilder(dotNetFormat.Length + 4);
            int i = 0;
            while (i < dotNetFormat.Length)
            {
                if (i + 4 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 4).SequenceEqual("yyyy")) { sb.Append("YYYY"); i += 4; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("yy")) { sb.Append("YY"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("MM")) { sb.Append("MM"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("dd")) { sb.Append("DD"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("HH")) { sb.Append("HH24"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("mm")) { sb.Append("MI"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("ss")) { sb.Append("SS"); i += 2; continue; }
                char c = dotNetFormat[i];
                if (c == 'M' || c == 'd' || c == 'H' || c == 'h' || c == 'm' || c == 's' || c == 'y'
                    || c == 'f' || c == 'F' || c == 'z' || c == 'K' || c == 't')
                    return null;
                // Literal segment -- to_char interprets 'Y'/'M'/'D' as tokens
                // even outside its known patterns, so wrap literals in quotes.
                if (c == '\'') sb.Append("''");
                else if (char.IsLetter(c)) sb.Append('"').Append(c).Append('"');
                else sb.Append(c);
                i++;
            }
            return $"to_char({sql}, '{sb}')";
        }

        /// <summary>
        /// PostgreSQL uses interval arithmetic: <c>(col + (N || ' seconds')::interval)</c>.
        /// The double-pipe-cast form lets the seconds count be a SQL fragment
        /// (constant literal or expression) rather than embedded in the literal.
        /// </summary>
        public override string? AddSecondsToDateTimeSql(string dateTimeSql, string secondsSqlFragment)
            => $"({dateTimeSql} + ({secondsSqlFragment} || ' seconds')::interval)";

        /// <summary>PostgreSQL uses ascii() (returns codepoint of first char).</summary>
        public override string GetCharCodeSql(string charSql) => $"ascii({charSql})";

        /// <summary>PostgreSQL uses chr() (lowercase, single-argument int).</summary>
        public override string GetCharFromCodeSql(string codePointSql) => $"chr({codePointSql})";

        /// <summary>
        /// PostgreSQL TimeSpan maps to INTERVAL; native +/- operators on
        /// (timestamp, interval) preserve precision without text parsing.
        /// </summary>
        public override string? AddTimeSpanColumnToDateTimeSql(string dateTimeSql, string timeSpanColumnSql, bool subtract)
        {
            var op = subtract ? "-" : "+";
            return $"({dateTimeSql} {op} {timeSpanColumnSql})";
        }

        /// <summary>PostgreSQL stores TimeSpan as INTERVAL; EXTRACT(EPOCH FROM col) returns fractional seconds.</summary>
        public override string GetTimeSpanColumnSecondsSql(string timeSpanColumnSql)
            => $"EXTRACT(EPOCH FROM {timeSpanColumnSql})";

        /// <summary>
        /// PostgreSQL stores DateTimeOffset as TIMESTAMPTZ (UTC instant). The
        /// <c>AT TIME ZONE</c> operator's string form uses POSIX semantics where
        /// the sign is inverted vs ISO-8601 — `'+02:00'` means UTC-2, not UTC+2.
        /// Using <c>INTERVAL '...'</c> instead gives ISO-8601-compatible semantics
        /// (positive interval = east of UTC). Format the wall clock result and
        /// append the canonical offset suffix the materialiser parses.
        /// </summary>
        public override string GetDateTimeOffsetWithOffsetSql(string dtoSql, TimeSpan offset)
        {
            var suffix = FormatOffsetSuffix(offset);
            var totalSec = (long)offset.TotalSeconds;
            return $"(to_char({dtoSql} AT TIME ZONE INTERVAL '{totalSec} seconds', 'YYYY-MM-DD\"T\"HH24:MI:SS') || '{suffix}')";
        }

        /// <inheritdoc/>
        public override string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
        {
            var totalSec = (long)localOffset.TotalSeconds;
            return $"(({dtoSql} AT TIME ZONE INTERVAL '{totalSec} seconds')::timestamp)";
        }

        /// <inheritdoc/>
        public override string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
            => $"CAST(EXTRACT(EPOCH FROM {dtoSql}) AS BIGINT)";

        internal override string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"FLOOR(EXTRACT(EPOCH FROM {dtoSql}) * 1000.0)::bigint";

        internal override string GetDateTimeOffsetUtcEpochMicrosecondsSql(string dtoSql)
            => $"FLOOR(EXTRACT(EPOCH FROM {dtoSql}) * 1000000.0)::bigint";

        /// <summary>PostgreSQL: date + int is native (`(date + 7)`).</summary>
        public override string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment)
            => $"({dateOnlySql} + {daysSqlFragment})";

        /// <summary>
        /// PostgreSQL: date + INTERVAL returns timestamp; cast back to date so the
        /// materializer reads a DateOnly-compatible value.
        /// </summary>
        public override string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment)
            => $"({dateOnlySql} + ({monthsSqlFragment}) * INTERVAL '1 month')::date";

        /// <summary>PostgreSQL: date + N * INTERVAL '1 year', cast back to date.</summary>
        public override string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment)
            => $"({dateOnlySql} + ({yearsSqlFragment}) * INTERVAL '1 year')::date";

        /// <summary>PostgreSQL TIME + INTERVAL returns TIME natively.</summary>
        public override string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment)
            => $"({timeOnlySql} + ({secondsSqlFragment} || ' seconds')::interval)";

        /// <summary>PostgreSQL: TIME + INTERVAL returns TIME natively, no text parsing.</summary>
        public override string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract)
        {
            var op = subtract ? "-" : "+";
            return $"({timeOnlySql} {op} {timeSpanColumnSql})";
        }

        /// <summary>PostgreSQL uses `#` (not `^`) for integer XOR — `^` would be exponentiation.</summary>
        public override string GetBitwiseXorSql(string left, string right) => $"({left} # {right})";

        /// <summary>PostgreSQL supports ON CONFLICT DO NOTHING for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2}) ON CONFLICT DO NOTHING";

        /// <summary>
        /// Creates a <see cref="DbParameter"/> instance for use with Npgsql commands.
        /// </summary>
        /// <param name="name">Parameter name including prefix.</param>
        /// <param name="value">Parameter value; <c>null</c> is passed through to the factory.</param>
        /// <returns>A configured <see cref="DbParameter"/> from the Npgsql factory.</returns>
        public override DbParameter CreateParameter(string name, object? value) =>
            _parameterFactory.CreateParameter(name, value);

        /// <summary>
        /// PostgreSQL subtracts timestamps to produce an interval; EXTRACT(EPOCH FROM ...)
        /// returns fractional seconds, matching .NET TimeSpan.TotalSeconds precision.
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"EXTRACT(EPOCH FROM ({endSql} - {startSql}))";

        /// <summary>PostgreSQL INTERVAL to fractional seconds via EPOCH extraction.</summary>
        internal override string TimeSpanOperandToSecondsSql(string sql)
            => $"EXTRACT(EPOCH FROM {sql})";

        /// <summary>PostgreSQL's ::int rounds half to even; TRUNC restores C#'s toward-zero cast.</summary>
        internal override string FloatingToIntegralTruncatingSql(string sql, bool asLong)
            => GetIntCastSql($"TRUNC({sql})", asLong);

        /// <summary>PostgreSQL MAKE_TIMESTAMP builds a TIMESTAMP from int parts.</summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"MAKE_TIMESTAMP({yearSql}, {monthSql}, {daySql}, 0, 0, 0)";

        /// <summary>6-arg PostgreSQL MAKE_TIMESTAMP with hour / minute / second columns.</summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql)
            => $"MAKE_TIMESTAMP({yearSql}, {monthSql}, {daySql}, {hourSql}, {minuteSql}, {secondSql})";

        /// <summary>
        /// 7-arg variant: PostgreSQL MAKE_TIMESTAMP accepts seconds as double,
        /// so combine second + millisecond/1000.0 into the seconds arg.
        /// </summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"MAKE_TIMESTAMP({yearSql}, {monthSql}, {daySql}, {hourSql}, {minuteSql}, ({secondSql}) + ({millisecondSql}) / 1000.0)";

        /// <summary>PostgreSQL MAKE_DATE builds a DATE from int parts.</summary>
        public override string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"MAKE_DATE({yearSql}, {monthSql}, {daySql})";

        /// <summary>PostgreSQL MAKE_TIME builds a TIME from int parts; seconds accept double.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => $"MAKE_TIME({hourSql}, {minuteSql}, {secondSql})";

        /// <summary>4-arg variant: fold seconds + millisecond/1000.0 into the double seconds arg.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"MAKE_TIME({hourSql}, {minuteSql}, ({secondSql}) + ({millisecondSql}) / 1000.0)";

        /// <summary>PostgreSQL's `~` operator evaluates a POSIX regex match against a text column.</summary>
        public override string GetRegexMatchSql(string inputSql, string patternLiteral)
            => $"({inputSql} ~ {patternLiteral})";

        /// <summary>PostgreSQL native regexp_replace(input, pattern, replacement).</summary>
        public override string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => $"regexp_replace({inputSql}, {patternLiteral}, {replacementLiteral})";

        /// <summary>PostgreSQL's `~*` is the case-insensitive POSIX regex match operator.</summary>
        public override string GetRegexMatchIgnoreCaseSql(string inputSql, string patternLiteral)
            => $"({inputSql} ~* {patternLiteral})";

        /// <summary>
        /// PostgreSQL regexp_replace supports a flags argument; 'gi' = global + case-insensitive.
        /// Matches .NET Regex.Replace(input, pattern, replacement, RegexOptions.IgnoreCase)
        /// semantics: pattern case-folded, non-matched portions of the input preserved.
        /// </summary>
        public override string GetRegexReplaceIgnoreCaseSql(string inputSql, string patternLiteral, string replacementLiteral)
            => $"regexp_replace({inputSql}, {patternLiteral}, {replacementLiteral}, 'gi')";

        /// <summary>
        /// PostgreSQL TIME - TIME yields INTERVAL natively. EXTRACT(EPOCH FROM ...)
        /// returns fractional seconds preserving sub-second precision. Wrap with
        /// +86400 then mod 86400 to match TimeOnly's [0, 24h) semantics.
        /// </summary>
        public override string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
            => $"((EXTRACT(EPOCH FROM ({endSql} - {startSql})) + 86400)::numeric % 86400)";
    }
}
