using System;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {
        /// <summary>
        /// Adds an integral 100ns-tick delta to a DateTime TEXT value with full tick
        /// precision. SQLite's datetime() truncates its output to whole seconds and
        /// strftime's %f keeps only milliseconds, so both drop sub-second precision --
        /// of the shift AND of the original value. This form works in integer epoch
        /// ticks instead: floor-epoch seconds from strftime('%s') plus the 7-digit
        /// fractional tail parsed from the canonical text
        /// ('yyyy-MM-dd HH:mm:ss[.fffffff]', fraction at position 21), then re-renders
        /// the seconds part via 'unixepoch' and re-appends the fraction trimmed the way
        /// Microsoft.Data.Sqlite serializes DateTime (trailing zeros and bare '.'
        /// removed). Division/modulo are floored explicitly so negative epochs
        /// (pre-1970) keep the correct fraction.
        /// </summary>
        private static string AddTicksToDateTimeText(string dateTimeSql, string ticksDeltaSql)
        {
            var frac = $"(CASE WHEN length({dateTimeSql}) > 20 THEN CAST(substr({dateTimeSql} || '0000000', 21, 7) AS INTEGER) ELSE 0 END)";
            var tot = $"(strftime('%s', {dateTimeSql}) * 10000000 + {frac} + ({ticksDeltaSql}))";
            var secs = $"(({tot} / 10000000) - (CASE WHEN {tot} % 10000000 < 0 THEN 1 ELSE 0 END))";
            var fracPos = $"((({tot} % 10000000) + 10000000) % 10000000)";
            return $"(strftime('%Y-%m-%d %H:%M:%S', {secs}, 'unixepoch') || RTRIM(RTRIM('.' || printf('%07d', {fracPos}), '0'), '.'))";
        }

        /// <summary>
        /// Calendar-unit add (months/years) that keeps the original sub-second
        /// fraction: datetime() renders whole seconds only, so re-append the
        /// input's fractional tail (unchanged by calendar arithmetic), trimmed
        /// to the Microsoft.Data.Sqlite serialization.
        /// </summary>
        private static string CalendarAddPreservingFraction(string dateTimeSql, string modifierSql)
            => $"(datetime({dateTimeSql}, {modifierSql}) || RTRIM(RTRIM('.' || printf('%07d', " +
               $"(CASE WHEN length({dateTimeSql}) > 20 THEN CAST(substr({dateTimeSql} || '0000000', 21, 7) AS INTEGER) ELSE 0 END)), '0'), '.'))";

        private static string? TryTranslateDateTimeFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(DateTime) && declaringType != typeof(DateTimeOffset))
            {
                return null;
            }

            return name switch
            {
                nameof(DateTime.Year) => $"CAST(strftime('%Y', {args[0]}) AS INTEGER)",
                nameof(DateTime.Month) => $"CAST(strftime('%m', {args[0]}) AS INTEGER)",
                nameof(DateTime.Day) => $"CAST(strftime('%d', {args[0]}) AS INTEGER)",
                nameof(DateTime.Hour) => $"CAST(strftime('%H', {args[0]}) AS INTEGER)",
                nameof(DateTime.Minute) => $"CAST(strftime('%M', {args[0]}) AS INTEGER)",
                nameof(DateTime.Second) => $"CAST(strftime('%S', {args[0]}) AS INTEGER)",
                nameof(DateTime.DayOfYear) => $"CAST(strftime('%j', {args[0]}) AS INTEGER)",
                // SQLite's date() returns 'YYYY-MM-DD' but ParameterManager
                // serializes DateTime params as 'yyyy-MM-dd HH:mm:ss.fffffff'.
                // A text comparison between those two formats never matches,
                // so column.Date == constantDate silently returns zero rows.
                // Emit the matching long format so Where round-trips; the
                // materializer parses either form back to DateTime.
                nameof(DateTime.Date) => $"strftime('%Y-%m-%d 00:00:00', {args[0]})",
                // DateTime.Ticks: (julianday(col) - julianday('0001-01-01')) *
                // 86400 * 1e7 -- ticks since DateTime.MinValue. IEEE-754 double
                // gives ~15 significant digits which is enough for comparison
                // ranges within a few hundred years (precision loss starts at
                // sub-microsecond). Projection round-trip back to long ticks is
                // out of scope for this initial implementation; comparison is
                // the dominant use case.
                nameof(DateTime.Ticks) when declaringType == typeof(DateTime) =>
                    $"((julianday({args[0]}) - 1721425.5) * 864000000000.0)",
                // DateTimeOffset.UtcDateTime -- normalize to UTC instant. The
                // stored format is 'yyyy-MM-dd HH:mm:ss[.FFFFFFF]zzz' where
                // zzz is the trailing 6-char offset (+HH:MM / -HH:MM).
                // strftime accepts multi-modifier syntax: pass the timestamp
                // sub-substring then negate the parsed offset by hours and
                // minutes. Sign-flip uses CASE on the leading char of zzz.
                // Materializer reads result as DateTime (Kind=Unspecified)
                // -- DateTime.Equals compares ticks not Kind, so round-trip
                // works for instant comparisons.
                // DateTimeOffset.DateTime -- the wall-clock DateTime portion,
                // IGNORING the offset (.NET returns Kind=Unspecified).
                // Strip the trailing 6-char zzz substring to leave the
                // canonical timestamp text the materializer parses as
                // DateTime.
                nameof(DateTimeOffset.DateTime) when declaringType == typeof(DateTimeOffset) =>
                    $"substr({args[0]}, 1, length({args[0]}) - 6)",
                // DateTimeOffset.Offset (TimeSpan) -- parse the trailing 6-char
                // zzz substring into the canonical TimeSpan 'c' format
                // ('[-]HH:mm:ss') the materializer parses via TimeSpan.Parse.
                // The sign char is at position length-5; if '-' we prefix
                // a '-' on the output, otherwise the unsigned form.
                nameof(DateTimeOffset.Offset) when declaringType == typeof(DateTimeOffset) =>
                    $"((CASE WHEN substr({args[0]}, length({args[0]}) - 5, 1) = '-' THEN '-' ELSE '' END) " +
                    $"|| substr({args[0]}, length({args[0]}) - 4, 2) || ':' " +
                    $"|| substr({args[0]}, length({args[0]}) - 1, 2) || ':00')",
                nameof(DateTimeOffset.UtcDateTime) when declaringType == typeof(DateTimeOffset) =>
                    $"strftime('%Y-%m-%d %H:%M:%S', substr({args[0]}, 1, length({args[0]}) - 6), " +
                    $"(CASE WHEN substr({args[0]}, length({args[0]}) - 5, 1) = '+' THEN '-' ELSE '+' END) " +
                    $"|| substr({args[0]}, length({args[0]}) - 4, 2) || ' hours', " +
                    $"(CASE WHEN substr({args[0]}, length({args[0]}) - 5, 1) = '+' THEN '-' ELSE '+' END) " +
                    $"|| substr({args[0]}, length({args[0]}) - 1, 2) || ' minutes')",
                // TimeOfDay returns the time portion (TimeSpan). Microsoft.Data.Sqlite
                // binds TimeSpan params as canonical 'HH:mm:ss' text (TimeSpan.ToString
                // 'c' format for sub-day spans), so emitting strftime('%H:%M:%S', col)
                // gives a string-comparable form that matches the param shape and
                // round-trips back to TimeSpan via the materializer.
                nameof(DateTime.TimeOfDay) => $"strftime('%H:%M:%S', {args[0]})",
                // strftime('%f', col) returns 'SS.SSS' (seconds with millisecond
                // precision). Multiplying by 1000 yields the integer ms portion of
                // the minute; modulo 1000 strips the seconds component. ROUND
                // guards against FP truncation (e.g. 45.456 * 1000 = 45455.99...
                // would truncate to 455 instead of 456).
                nameof(DateTime.Millisecond) => $"(CAST(ROUND(strftime('%f', {args[0]}) * 1000) AS INTEGER) % 1000)",
                // DateTime text format includes 'yyyy-MM-dd HH:mm:ss[.fffffff]'.
                // The fractional 7-digit tail starts at position 21 (' ' at 11,
                // hh at 12-13, ':' at 14, mm at 15-16, ':' at 17, ss at 18-19,
                // '.' at 20, fffffff starts at 21). Digits 4..6 of the tail
                // are the microsecond-within-millisecond.
                nameof(DateTime.Microsecond) =>
                    $"(CASE WHEN length({args[0]}) > 23 THEN CAST(substr({args[0]}, 24, 3) AS INTEGER) ELSE 0 END)",
                // Digit 7 of the 7-digit fractional tail (position 27 of
                // 'yyyy-MM-dd HH:mm:ss.fffffff') is the 100ns tick offset;
                // multiply by 100 to get the .NET 0..900 Nanosecond.
                nameof(DateTime.Nanosecond) =>
                    $"(CASE WHEN length({args[0]}) > 26 THEN CAST(substr({args[0]}, 27, 1) AS INTEGER) * 100 ELSE 0 END)",
                // AddDays/AddMonths/AddYears accept a delta in the second argument.
                // SQLite's date modifier syntax accepts an unsigned-positive form
                // ('7 days') and an explicitly-signed negative form ('-3 days'); the
                // previous '+' || ({delta}) prefix produced '+-3 days' for negative
                // deltas which SQLite parses as invalid and returns NULL, silently
                // dropping every comparison. Letting the bound value carry its own
                // sign works for positive (no prefix), negative (leading -), and
                // zero (no shift).
                // The double-argument Add* family is tick-exact in .NET (AddDays(1.5) is
                // +36h; the delta truncates toward zero at the 100ns tick). CAST(x AS
                // INTEGER) truncates toward zero, matching .NET. Gated to DateTime:
                // DateTimeOffset TEXT carries a trailing '+HH:MM' offset the epoch-tick
                // parse does not understand, so DTO keeps the previous datetime() form.
                nameof(DateTime.AddDays) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    AddTicksToDateTimeText(args[0], $"CAST(({args[1]}) * 864000000000 AS INTEGER)"),
                nameof(DateTime.AddHours) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    AddTicksToDateTimeText(args[0], $"CAST(({args[1]}) * 36000000000 AS INTEGER)"),
                nameof(DateTime.AddMinutes) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    AddTicksToDateTimeText(args[0], $"CAST(({args[1]}) * 600000000 AS INTEGER)"),
                nameof(DateTime.AddSeconds) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    AddTicksToDateTimeText(args[0], $"CAST(({args[1]}) * 10000000 AS INTEGER)"),
                nameof(DateTime.AddMonths) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    CalendarAddPreservingFraction(args[0], $"(({args[1]}) || ' months')"),
                nameof(DateTime.AddYears) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    CalendarAddPreservingFraction(args[0], $"(({args[1]}) || ' years')"),
                nameof(DateTime.AddDays) when args.Length == 2 => $"datetime({args[0]}, ({args[1]}) || ' days')",
                nameof(DateTime.AddMonths) when args.Length == 2 => $"datetime({args[0]}, ({args[1]}) || ' months')",
                nameof(DateTime.AddYears) when args.Length == 2 => $"datetime({args[0]}, ({args[1]}) || ' years')",
                nameof(DateTime.AddHours) when args.Length == 2 => $"datetime({args[0]}, ({args[1]}) || ' hours')",
                nameof(DateTime.AddMinutes) when args.Length == 2 => $"datetime({args[0]}, ({args[1]}) || ' minutes')",
                nameof(DateTime.AddSeconds) when args.Length == 2 => $"datetime({args[0]}, ({args[1]}) || ' seconds')",
                // AddMilliseconds needs sub-second precision. SQLite's modifier
                // syntax accepts fractional seconds ('+0.5 seconds'), so scale
                // the int delta with /1000.0. Default datetime() drops fractional
                // output -- use strftime('%Y-%m-%d %H:%M:%f', ...) which keeps
                // 'SS.SSS'. Then RTRIM('0') + RTRIM('.') trims trailing zeros
                // (and the literal '.' when no fractional remains) so the column
                // text matches Microsoft.Data.Sqlite's DateTime serialization
                // ('yyyy-MM-dd HH:mm:ss.FFFFFFF' which trims trailing zeros);
                // without this, '.500' lexically != param-bound '.5' and Where
                // round-trip silently mis-matches.
                nameof(DateTime.AddMilliseconds) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    AddTicksToDateTimeText(args[0], $"CAST(({args[1]}) * 10000 AS INTEGER)"),
                nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', {args[0]}, (({args[1]}) / 1000.0) || ' seconds'), '0'), '.')",
                // AddTicks is the finest-grained Add* (1 tick = 100ns). Same
                // strftime + RTRIM trim shape as AddMilliseconds; the divisor
                // is 1e7 (10_000_000 ticks per second). SQLite's modifier
                // syntax accepts the fractional value directly so very small
                // tick deltas (e.g. 7500 ticks = 0.00075 seconds) get applied
                // correctly while still trimming the trailing-zero / dot to
                // match Microsoft.Data.Sqlite's FFFFFFF DateTime binding.
                nameof(DateTime.AddTicks) when args.Length == 2 && declaringType == typeof(DateTime) =>
                    AddTicksToDateTimeText(args[0], args[1]),
                nameof(DateTime.AddTicks) when args.Length == 2 => $"RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', {args[0]}, (({args[1]}) / 10000000.0) || ' seconds'), '0'), '.')",
                // SQLite strftime %w returns 0..6 (Sun..Sat); .NET DayOfWeek enum matches.
                nameof(DateTime.DayOfWeek) => $"CAST(strftime('%w', {args[0]}) AS INTEGER)",
                // DateTime/DateTimeOffset.Parse(string) -- SQLite stores DateTime
                // as TEXT and Microsoft.Data.Sqlite's GetDateTime parses the
                // canonical text directly. Identity emission; the materializer
                // converts text -> DateTime/DateTimeOffset via the column type
                // affinity. Sister to the numeric Parse handler.
                "Parse" when args.Length == 1 => args[0],
                // Compare(a, b) returns -1/0/1 indicating less/equal/greater.
                // Lift both to julianday so the subtraction is numeric and
                // SIGN yields the canonical triple. CAST settles the result
                // to INTEGER so the materializer hits int affinity.
                nameof(DateTime.Compare) when args.Length == 2 =>
                    $"CAST(SIGN(julianday({args[0]}) - julianday({args[1]})) AS INTEGER)",
                // IsLeapYear(y) -- Gregorian rule: div by 4, but not
                // centuries unless also div by 400.
                nameof(DateTime.IsLeapYear) when args.Length == 1 =>
                    $"((({args[0]}) % 4 = 0 AND ({args[0]}) % 100 != 0) OR ({args[0]}) % 400 = 0)",
                // DaysInMonth(year, month) -- month-length table with the
                // leap-year exception for February. Pure CASE expression.
                nameof(DateTime.DaysInMonth) when args.Length == 2 =>
                    $"(CASE ({args[1]}) " +
                    $"WHEN 1 THEN 31 WHEN 3 THEN 31 WHEN 5 THEN 31 WHEN 7 THEN 31 " +
                    $"WHEN 8 THEN 31 WHEN 10 THEN 31 WHEN 12 THEN 31 " +
                    $"WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30 " +
                    $"WHEN 2 THEN (CASE WHEN ({args[0]}) % 4 = 0 AND (({args[0]}) % 100 != 0 OR ({args[0]}) % 400 = 0) THEN 29 ELSE 28 END) " +
                    $"END)",
                // Instance CompareTo -- same emit pattern, args[0] is the
                // receiver instance pushed through TranslateFunction.
                nameof(DateTime.CompareTo) when args.Length == 2 =>
                    $"CAST(SIGN(julianday({args[0]}) - julianday({args[1]})) AS INTEGER)",
                _ => null
            };
        }

        private static string? TryTranslateDateOnlyFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(DateOnly))
            {
                return null;
            }

            return name switch
            {
                nameof(DateOnly.Year) => $"CAST(strftime('%Y', {args[0]}) AS INTEGER)",
                nameof(DateOnly.Month) => $"CAST(strftime('%m', {args[0]}) AS INTEGER)",
                nameof(DateOnly.Day) => $"CAST(strftime('%d', {args[0]}) AS INTEGER)",
                nameof(DateOnly.DayOfYear) => $"CAST(strftime('%j', {args[0]}) AS INTEGER)",
                // SQLite strftime %w returns 0..6 (Sun..Sat), matching .NET DayOfWeek.
                nameof(DateOnly.DayOfWeek) => $"CAST(strftime('%w', {args[0]}) AS INTEGER)",
                // DayNumber: days since DateOnly.MinValue (0001-01-01).
                // .NET stores DayNumber as a 0-based int -- subtract the
                // Julian Day of 0001-01-01 (1721425.5; the .5 is the JD
                // half-day offset since julianday('0001-01-01') returns
                // 1721425.5 for that date at 00:00 UTC).
                nameof(DateOnly.DayNumber) => $"CAST((julianday({args[0]}) - 1721425.5) AS INTEGER)",
                // FromDayNumber(n): inverse of DayNumber. SQLite's
                // julianday baseline differs from .NET's proleptic-Gregorian
                // DayNumber, so we use the date('0001-01-01', '+N days')
                // modifier which operates day-by-day on the symbolic date
                // and yields the expected proleptic-Gregorian result.
                nameof(DateOnly.CompareTo) when args.Length == 2 =>
                    $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                nameof(DateOnly.FromDayNumber) when args.Length == 1 =>
                    $"date('0001-01-01', '+' || CAST({args[0]} AS TEXT) || ' days')",
                // FromDateTime(dt) drops the time portion. SQLite's date()
                // emits 'YYYY-MM-DD' which the materializer parses to DateOnly.
                nameof(DateOnly.FromDateTime) when args.Length == 1 =>
                    $"date({args[0]})",
                // ToDateTime(timeOnly) combines a 'YYYY-MM-DD' DateOnly
                // text with an 'HH:mm:ss[.fffffff]' TimeOnly text via
                // string concat. The materializer parses the resulting
                // canonical 'YYYY-MM-DD HH:mm:ss' to DateTime.
                nameof(DateOnly.ToDateTime) when args.Length == 2 =>
                    $"({args[0]} || ' ' || {args[1]})",
                // Parse(string) -- Microsoft.Data.Sqlite stores DateOnly
                // as canonical 'yyyy-MM-dd' text; source TEXT column
                // already holds compatible text so SQL emission is
                // identity and GetFieldValue<DateOnly> round-trips.
                "Parse" when args.Length == 1 => args[0],
                _ => null
            };
        }

        private static string? TryTranslateTimeSpanFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(TimeSpan))
            {
                return null;
            }

            // Microsoft.Data.Sqlite binds TimeSpan as canonical
            // <c>[-][d.]HH:mm:ss[.fffffff]</c> text (TimeSpan.ToString 'c'
            // format). The TimeSpanColumnTotalSecondsSql helper handles
            // all four shape combinations of sign x day-prefix; the
            // component accessors below derive from it via integer
            // arithmetic that preserves .NET's truncate-toward-zero
            // semantics for negative spans.
            var totalSecondsSql = TimeSpanColumnTotalSecondsSql(args[0]);
            return name switch
            {
                nameof(TimeSpan.Hours) => $"(CAST({totalSecondsSql} / 3600 AS INTEGER) % 24)",
                nameof(TimeSpan.Minutes) => $"(CAST({totalSecondsSql} / 60 AS INTEGER) % 60)",
                nameof(TimeSpan.Seconds) => $"(CAST({totalSecondsSql} AS INTEGER) % 60)",
                nameof(TimeSpan.Days) => $"CAST({totalSecondsSql} / 86400 AS INTEGER)",
                nameof(TimeSpan.TotalSeconds) => totalSecondsSql,
                nameof(TimeSpan.TotalMinutes) => $"({totalSecondsSql} / 60.0)",
                nameof(TimeSpan.TotalHours) => $"({totalSecondsSql} / 3600.0)",
                nameof(TimeSpan.TotalDays) => $"({totalSecondsSql} / 86400.0)",
                nameof(TimeSpan.TotalMilliseconds) => $"({totalSecondsSql} * 1000.0)",
                // Parse(string) -- Microsoft.Data.Sqlite round-trips
                // TimeSpan via canonical 'HH:mm:ss[.fffffff]' text. The
                // source column already holds compatible text, so SQL
                // emission is identity and GetFieldValue<TimeSpan> parses.
                "Parse" when args.Length == 1 => args[0],
                // Compare(a, b) -- TimeSpan binds as canonical 'HH:mm:ss
                // [.fffffff]' text which is lexicographically sortable
                // within a single day (sub-day spans). CASE on < > = on
                // the text yields the standard -1/0/1 sign. Multi-day
                // 'd.HH:mm:ss' prefixes are documented as out-of-scope
                // for the component getters above and the same caveat
                // applies here.
                nameof(TimeSpan.Compare) when args.Length == 2 =>
                    $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                _ => null
            };
        }

        private static string? TryTranslateTimeOnlyFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(TimeOnly))
            {
                return null;
            }

            return name switch
            {
                nameof(TimeOnly.Hour) => $"CAST(strftime('%H', {args[0]}) AS INTEGER)",
                nameof(TimeOnly.Minute) => $"CAST(strftime('%M', {args[0]}) AS INTEGER)",
                nameof(TimeOnly.Second) => $"CAST(strftime('%S', {args[0]}) AS INTEGER)",
                // TimeOnly text format is 'HH:mm:ss[.fffffff]'. When the
                // fractional tail is present (length > 9) parse the first
                // 3 digits past the '.' as the millisecond component;
                // when absent return 0. The 3-digit substring is the
                // ms portion of .NET's 7-digit ticks suffix.
                nameof(TimeOnly.Millisecond) =>
                    $"(CASE WHEN length({args[0]}) > 9 THEN CAST(substr({args[0]}, 10, 3) AS INTEGER) ELSE 0 END)",
                // TimeOnly text 'HH:mm:ss[.fffffff]'. The 7-digit fractional
                // starts at position 10. Digits 4..6 (positions 13..15) are
                // the microsecond-within-millisecond.
                nameof(TimeOnly.Microsecond) =>
                    $"(CASE WHEN length({args[0]}) > 12 THEN CAST(substr({args[0]}, 13, 3) AS INTEGER) ELSE 0 END)",
                // TimeOnly text 'HH:mm:ss[.fffffff]'. Digit 7 is at position 16;
                // value times 100 yields the .NET Nanosecond 0..900.
                nameof(TimeOnly.Nanosecond) =>
                    $"(CASE WHEN length({args[0]}) > 15 THEN CAST(substr({args[0]}, 16, 1) AS INTEGER) * 100 ELSE 0 END)",
                // Ticks = H*36e9 + M*6e8 + S*1e7 + 7-digit fractional tail.
                // Text format 'HH:mm:ss[.fffffff]'; fractional starts at
                // position 10 (length-check gates the optional tail).
                nameof(TimeOnly.Ticks) =>
                    $"(CAST(substr({args[0]}, 1, 2) AS INTEGER) * 36000000000 + " +
                    $"CAST(substr({args[0]}, 4, 2) AS INTEGER) * 600000000 + " +
                    $"CAST(substr({args[0]}, 7, 2) AS INTEGER) * 10000000 + " +
                    $"CASE WHEN length({args[0]}) > 9 THEN CAST(substr({args[0]}, 10, 7) AS INTEGER) ELSE 0 END)",
                // FromDateTime(dt) / FromTimeSpan(ts) drop everything but
                // the time portion. SQLite's time() emits 'HH:mm:ss'.
                nameof(TimeOnly.FromDateTime) when args.Length == 1 => $"time({args[0]})",
                nameof(TimeOnly.FromTimeSpan) when args.Length == 1 => args[0],
                // IsBetween(start, end): .NET defines this as
                //   if (start <= end) start <= this < end
                //   else (wraps midnight) this >= start OR this < end
                // Both cases unified with a CASE on the comparison. Args
                // are TEXT 'HH:mm:ss[.fffffff]' which sorts lex-correctly.
                nameof(TimeOnly.IsBetween) when args.Length == 3 =>
                    $"(CASE WHEN {args[1]} <= {args[2]} THEN ({args[0]} >= {args[1]} AND {args[0]} < {args[2]}) " +
                    $"ELSE ({args[0]} >= {args[1]} OR {args[0]} < {args[2]}) END)",
                nameof(TimeOnly.CompareTo) when args.Length == 2 =>
                    $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                // Parse(string) -- Microsoft.Data.Sqlite stores TimeOnly
                // as canonical 'HH:mm:ss[.fffffff]' text; source TEXT
                // column already holds compatible text so SQL emission
                // is identity and GetFieldValue<TimeOnly> round-trips.
                "Parse" when args.Length == 1 => args[0],
                _ => null
            };
        }
    }
}
