using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class SqlServerProvider
    {
        /// <summary>
        /// SQL Server stores TimeOnly as TIME(7). DATEDIFF(SECOND, t1, t2) on
        /// two TIMEs returns the signed second diff in (-86400, 86400). Wrap
        /// with +86400 then % 86400 to match TimeOnly's [0, 24h) semantics.
        /// </summary>
        public override string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
            => $"CAST(((DATEDIFF(SECOND, {startSql}, {endSql}) + 86400) % 86400) AS FLOAT)";

        /// <summary>
        /// Overload-aware Math.Round / decimal.Round handling so MidpointRounding
        /// arguments dispatch to the correct emit instead of being silently
        /// coerced to a digit-count integer (the int value of the enum).
        /// SqlServer-native ROUND(x [, n]) is AwayFromZero; ROUND(x, n, 1) is
        /// the truncate-toward-zero form via the truncate flag.
        /// </summary>
        public override string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args)
            => TryTranslateMathRoundWithMode(node, args,
                awayFromZero: (x, digits) => digits == null ? $"ROUND({x}, 0)" : $"ROUND({x}, {digits})",
                truncateTowardZero: (x, digits) => digits == null ? $"ROUND({x}, 0, 1)" : $"ROUND({x}, {digits}, 1)")
            ?? TryTranslateIeee754Predicate(node, args)
            ?? TryTranslateTimeSpanFactory(node, args);

        /// <summary>
        /// double / float IEEE 754 predicates (IsNaN / IsInfinity / IsFinite /
        /// IsPositiveInfinity / IsNegativeInfinity). T-SQL has no native
        /// primitives; emit the algebraic form (x != x for IsNaN, CAST('Infinity'
        /// AS FLOAT) comparison for IsInfinity) so the predicate works on
        /// computed float expressions (e.g. division results) even though SQL
        /// Server's FLOAT type itself usually rejects NaN/Infinity at insert.
        /// </summary>
        private static string? TryTranslateIeee754Predicate(System.Linq.Expressions.MethodCallExpression node, string[] args)
        {
            var dt = node.Method.DeclaringType;
            if ((dt != typeof(double) && dt != typeof(float)) || args.Length != 1) return null;
            const string pInf = "CAST('Infinity' AS FLOAT)";
            const string nInf = "CAST('-Infinity' AS FLOAT)";
            return node.Method.Name switch
            {
                "IsNaN" => $"({args[0]} != {args[0]})",
                "IsInfinity" => $"({args[0]} = {pInf} OR {args[0]} = {nInf})",
                "IsFinite" => $"({args[0]} = {args[0]} AND {args[0]} != {pInf} AND {args[0]} != {nInf})",
                "IsPositiveInfinity" => $"({args[0]} = {pInf})",
                "IsNegativeInfinity" => $"({args[0]} = {nInf})",
                // Normal: finite, non-zero, |x| >= min normal positive double.
                "IsNormal" => $"({args[0]} = {args[0]} AND {args[0]} != {pInf} AND {args[0]} != {nInf} " +
                              $"AND {args[0]} != 0 AND ABS({args[0]}) >= 2.2250738585072014E-308)",
                // Subnormal: non-zero AND |x| < min normal positive.
                "IsSubnormal" => $"({args[0]} != 0 AND ABS({args[0]}) < 2.2250738585072014E-308)",
                _ => null
            };
        }

        /// <summary>
        /// Translates a subset of .NET methods into their SQL Server equivalents.
        /// </summary>
        /// <param name="name">Name of the method being translated.</param>
        /// <param name="declaringType">Type that defines the method.</param>
        /// <param name="args">SQL representations of the method arguments.</param>
        /// <returns>The SQL translation or <c>null</c> if unsupported.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="args"/> is null.</exception>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);

            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(char.ToUpperInvariant) => $"UPPER({args[0]})",
                    nameof(char.ToLowerInvariant) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"LEN({args[0]})",
                    nameof(string.Trim) when args.Length == 1 => $"LTRIM(RTRIM({args[0]}))",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // T-SQL has no LPAD/RPAD primitive. Build via REPLICATE
                    // + concatenation, with a CASE gate so a longer-than-
                    // target input passes through unchanged (matching .NET
                    // PadLeft/PadRight's "never truncates" contract).
                    nameof(string.PadLeft) when args.Length == 2 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE REPLICATE(' ', {args[1]} - LEN({args[0]})) + {args[0]} END)",
                    nameof(string.PadLeft) when args.Length == 3 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE REPLICATE({args[2]}, {args[1]} - LEN({args[0]})) + {args[0]} END)",
                    nameof(string.PadRight) when args.Length == 2 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE {args[0]} + REPLICATE(' ', {args[1]} - LEN({args[0]})) END)",
                    nameof(string.PadRight) when args.Length == 3 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE {args[0]} + REPLICATE({args[2]}, {args[1]} - LEN({args[0]})) END)",
                    // SQL Server SUBSTRING is 1-indexed; .NET Substring is 0-indexed, add 1.
                    // The 2-arg form needs an explicit large length because SUBSTRING requires
                    // a length parameter; LEN of the source is always >= what we need.
                    nameof(string.Substring) when args.Length == 2 => $"SUBSTRING({args[0]}, {args[1]} + 1, LEN({args[0]}))",
                    nameof(string.Substring) when args.Length == 3 => $"SUBSTRING({args[0]}, {args[1]} + 1, {args[2]})",
                    // .NET Replace matches ordinally, but T-SQL REPLACE follows the (case-
                    // insensitive by default) collation and would rewrite case variants too.
                    // An explicit binary collation on the input makes the match ordinal.
                    nameof(string.Replace) when args.Length == 3 =>
                        $"REPLACE({OrdinalComparableStringProjection(args[0])}, {args[1]}, {args[2]})",
                    // CHARINDEX is 1-based and returns 0 if not found; .NET IndexOf is 0-based
                    // and returns -1, so subtract 1 from CHARINDEX. The binary collation on the
                    // needle makes the match ordinal (CHARINDEX otherwise follows the case-
                    // insensitive default collation).
                    nameof(string.IndexOf) when args.Length == 2 =>
                        $"(CHARINDEX({OrdinalComparableStringProjection(args[1])}, {args[0]}) - 1)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTimeOffset))
            {
                // DateTimeOffset members specific to the offset-aware type. Handle
                // these BEFORE the shared DateTime branch so DateTime.X never
                // accidentally swallows them via name collision.
                var dtoMatch = name switch
                {
                    // SWITCHOFFSET(col, 0) shifts the wall-clock to UTC; CAST AS
                    // DATETIME2 drops the offset suffix so the materializer reads
                    // a plain DATETIME2 -> DateTime(Kind=Unspecified).
                    nameof(DateTimeOffset.UtcDateTime) => $"CAST(SWITCHOFFSET({args[0]}, 0) AS DATETIME2)",
                    // The wall-clock portion (ignoring the offset) is just the
                    // DATETIME2 cast of the value -- matches .NET DateTimeOffset.
                    // DateTime returning Kind=Unspecified.
                    nameof(DateTimeOffset.DateTime) => $"CAST({args[0]} AS DATETIME2)",
                    // DATEPART(TZoffset, x) returns the offset in MINUTES as int;
                    // multiply by 60 to get seconds and CAST to FLOAT so the
                    // materializer's "double -> TimeSpan.FromSeconds" path picks
                    // it up correctly.
                    nameof(DateTimeOffset.Offset) => $"CAST((DATEPART(TZoffset, {args[0]}) * 60) AS FLOAT)",
                    _ => null
                };
                if (dtoMatch != null) return dtoMatch;
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
                return name switch
                {
                    nameof(DateTime.Millisecond) => $"DATEPART(millisecond, {args[0]})",
                    // Microsecond returns the microsecond within the current
                    // millisecond (0..999), not the whole-second microsecond.
                    // T-SQL DATEPART(microsecond) returns 0..999999; modulo
                    // 1000 yields the microsecond-within-millisecond.
                    nameof(DateTime.Microsecond) => $"(DATEPART(microsecond, {args[0]}) % 1000)",
                    // SQL Server DATEPART(nanosecond) returns 0..999999900 with
                    // 100ns precision; modulo 1000 yields the 100ns-within-
                    // microsecond, matching .NET's 0..900 in 100 steps.
                    nameof(DateTime.Nanosecond) => $"(DATEPART(nanosecond, {args[0]}) % 1000)",
                    nameof(DateTime.Year) => $"YEAR({args[0]})",
                    nameof(DateTime.Month) => $"MONTH({args[0]})",
                    nameof(DateTime.Day) => $"DAY({args[0]})",
                    nameof(DateTime.Hour) => $"DATEPART(hour, {args[0]})",
                    nameof(DateTime.Minute) => $"DATEPART(minute, {args[0]})",
                    nameof(DateTime.Second) => $"DATEPART(second, {args[0]})",
                    nameof(DateTime.DayOfYear) => $"DATEPART(dayofyear, {args[0]})",
                    nameof(DateTime.Date) => $"CAST({args[0]} AS DATE)",
                    nameof(DateTime.AddDays) when args.Length == 2 => $"DATEADD(day, {args[1]}, {args[0]})",
                    nameof(DateTime.AddMonths) when args.Length == 2 => $"DATEADD(month, {args[1]}, {args[0]})",
                    nameof(DateTime.AddYears) when args.Length == 2 => $"DATEADD(year, {args[1]}, {args[0]})",
                    nameof(DateTime.AddHours) when args.Length == 2 => $"DATEADD(hour, {args[1]}, {args[0]})",
                    nameof(DateTime.AddMinutes) when args.Length == 2 => $"DATEADD(minute, {args[1]}, {args[0]})",
                    nameof(DateTime.AddSeconds) when args.Length == 2 => $"DATEADD(second, {args[1]}, {args[0]})",
                    // AddMilliseconds: DATEADD supports the `millisecond` datepart.
                    nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"DATEADD(millisecond, {args[1]}, {args[0]})",
                    // AddTicks: a tick = 100ns. SqlServer DATEADD(NANOSECOND, ...) exists
                    // in 2008+ but the underlying DATETIME2 has 100ns precision so sub-tick
                    // precision is dropped. The argument range matters: ticks * 100 can
                    // overflow long. Split into seconds (whole ticks/1e7) + nanosecond
                    // remainder ((ticks % 1e7) * 100) so each component stays in int32 range.
                    nameof(DateTime.AddTicks) when args.Length == 2 =>
                        $"DATEADD(nanosecond, (({args[1]}) % 10000000) * 100, DATEADD(second, ({args[1]}) / 10000000, {args[0]}))",
                    // T-SQL DATEPART(weekday) depends on @@DATEFIRST; subtract @@DATEFIRST so that
                    // Sunday=0..Saturday=6 always, matching System.DayOfWeek.
                    nameof(DateTime.DayOfWeek) => $"((DATEPART(weekday, {args[0]}) + @@DATEFIRST - 1) % 7)",
                    // Compare / CompareTo: return the signed -1/0/1 sentinel
                    // matching .NET IComparable. Static Compare(a, b) gets two
                    // arg slots (both pre-translated to SQL fragments);
                    // instance CompareTo collapses to the same shape since
                    // node.Object is passed as args[0].
                    nameof(DateTime.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.IsLeapYear) when args.Length == 1 => BuildIsLeapYearSql(args[0]),
                    nameof(DateTime.DaysInMonth) when args.Length == 2 => BuildDaysInMonthSql(args[0], args[1]),
                    _ => null
                };
            }

            if (declaringType == typeof(TimeSpan))
            {
                return name switch
                {
                    nameof(TimeSpan.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(TimeSpan.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateOnly))
            {
                return name switch
                {
                    nameof(DateOnly.Year) => $"YEAR({args[0]})",
                    nameof(DateOnly.Month) => $"MONTH({args[0]})",
                    nameof(DateOnly.Day) => $"DAY({args[0]})",
                    nameof(DateOnly.DayOfYear) => $"DATEPART(dayofyear, {args[0]})",
                    // T-SQL DATEPART(weekday) depends on @@DATEFIRST; subtract @@DATEFIRST
                    // so Sunday=0..Saturday=6 always, matching System.DayOfWeek.
                    nameof(DateOnly.DayOfWeek) => $"((DATEPART(weekday, {args[0]}) + @@DATEFIRST - 1) % 7)",
                    nameof(DateOnly.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    // DateOnly.DayNumber: days since DateOnly.MinValue (0001-01-01).
                    // SQL Server DATEDIFF(DAY, '0001-01-01', x) returns int days
                    // matching .NET's day-count semantics across the full range.
                    nameof(DateOnly.DayNumber) => $"DATEDIFF(DAY, CAST('0001-01-01' AS DATE), {args[0]})",
                    nameof(DateOnly.FromDayNumber) when args.Length == 1 =>
                        $"DATEADD(DAY, {args[0]}, CAST('0001-01-01' AS DATE))",
                    nameof(DateOnly.FromDateTime) when args.Length == 1 =>
                        $"CAST({args[0]} AS DATE)",
                    // DateOnly.ToDateTime(TimeOnly) combines the two parts into a
                    // wall-clock DATETIME2. Add the time-as-seconds to a midnight
                    // datetime built from the date portion. SQL Server doesn't
                    // accept native (DATE + TIME) addition, so use DATEADD.
                    nameof(DateOnly.ToDateTime) when args.Length == 2 =>
                        $"DATEADD(SECOND, DATEDIFF(SECOND, CAST('00:00:00' AS TIME), {args[1]}), CAST({args[0]} AS DATETIME2))",
                    _ => null
                };
            }

            if (declaringType == typeof(TimeOnly))
            {
                return name switch
                {
                    nameof(TimeOnly.Hour) => $"DATEPART(hour, {args[0]})",
                    nameof(TimeOnly.Minute) => $"DATEPART(minute, {args[0]})",
                    nameof(TimeOnly.Second) => $"DATEPART(second, {args[0]})",
                    nameof(TimeOnly.Millisecond) => $"DATEPART(millisecond, {args[0]})",
                    // CAST DATETIME2 -> TIME extracts the wall-clock TIME portion.
                    nameof(TimeOnly.FromDateTime) when args.Length == 1 => $"CAST({args[0]} AS TIME)",
                    // TimeSpan is bound as TIME on SqlServer (column-side); the
                    // CAST is a no-op for TIME but explicit for clarity.
                    nameof(TimeOnly.FromTimeSpan) when args.Length == 1 => $"CAST({args[0]} AS TIME)",
                    nameof(TimeOnly.Microsecond) => $"(DATEPART(microsecond, {args[0]}) % 1000)",
                    nameof(TimeOnly.Nanosecond) => $"(DATEPART(nanosecond, {args[0]}) % 1000)",
                    // Ticks = 100ns since midnight; DATEDIFF_BIG(NANOSECOND)
                    // returns ns count, /100 yields ticks.
                    nameof(TimeOnly.Ticks) => $"(DATEDIFF_BIG(NANOSECOND, CAST('00:00:00' AS TIME), {args[0]}) / 100)",
                    // IsBetween(start, end) wraps around midnight when start > end.
                    // Matches .NET's TimeOnly.IsBetween semantics.
                    nameof(TimeOnly.IsBetween) when args.Length == 3 =>
                        $"(CASE WHEN {args[1]} <= {args[2]} THEN ({args[0]} >= {args[1]} AND {args[0]} < {args[2]}) " +
                        $"ELSE ({args[0]} >= {args[1]} OR {args[0]} < {args[2]}) END)",
                    nameof(TimeOnly.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    _ => null
                };
            }

            if (declaringType == typeof(NormFunctions))
            {
                return name switch
                {
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
                    // Server-side primitives matching .NET semantics: GETUTCDATE
                    // returns DATETIME (UTC); NEWID is a Guid-shaped uniqueidentifier;
                    // RAND() returns float in [0, 1).
                    nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "GETUTCDATE()",
                    nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => "NEWID()",
                    nameof(NormFunctions.ServerRandom) when args.Length == 0 => "RAND()",
                    _ => null
                };
            }

            if (declaringType == typeof(Guid) && name == nameof(Guid.NewGuid) && args.Length == 0)
                return "NEWID()";

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEILING({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]}, 0)",
                    nameof(Math.Sqrt) when args.Length == 1 => $"SQRT(CASE WHEN {args[0]} < 0 THEN NULL ELSE {args[0]} END)",
                    nameof(Math.Pow) when args.Length == 2 => $"POWER({args[0]}, {args[1]})",
                    nameof(Math.Exp) when args.Length == 1 => $"EXP({args[0]})",
                    nameof(Math.Log) when args.Length == 1 => $"LOG({args[0]})",
                    nameof(Math.Log) when args.Length == 2 => $"LOG({args[0]}, {args[1]})",
                    nameof(Math.Log10) when args.Length == 1 => $"LOG10({args[0]})",
                    nameof(Math.Sign) when args.Length == 1 => $"SIGN({args[0]})",
                    // T-SQL ROUND with truncate flag (1) drops fractional digits instead of rounding.
                    nameof(Math.Truncate) when args.Length == 1 => $"ROUND({args[0]}, 0, 1)",
                    // T-SQL has no scalar Min/Max, only the aggregate. Emit a CASE.
                    nameof(Math.Min) when args.Length == 2 => $"(CASE WHEN {args[0]} < {args[1]} THEN {args[0]} ELSE {args[1]} END)",
                    nameof(Math.Max) when args.Length == 2 => $"(CASE WHEN {args[0]} > {args[1]} THEN {args[0]} ELSE {args[1]} END)",
                    // Basic trig + inverse trig. T-SQL spells two-argument
                    // arctangent ATN2 (legacy from SQL Server's Sybase
                    // ancestry), NOT ATAN2 -- this is the only per-provider
                    // wrinkle in the trig set.
                    nameof(Math.Sin) when args.Length == 1 => $"SIN({args[0]})",
                    nameof(Math.Cos) when args.Length == 1 => $"COS({args[0]})",
                    nameof(Math.Tan) when args.Length == 1 => $"TAN({args[0]})",
                    nameof(Math.Asin) when args.Length == 1 => $"ASIN({args[0]})",
                    nameof(Math.Acos) when args.Length == 1 => $"ACOS({args[0]})",
                    nameof(Math.Atan) when args.Length == 1 => $"ATAN({args[0]})",
                    nameof(Math.Atan2) when args.Length == 2 => $"ATN2({args[0]}, {args[1]})",
                    // Hyperbolic + inverse hyperbolic: T-SQL has no native
                    // functions for these. Emit the algebraic identities via
                    // EXP / LOG (natural log) / SQRT. Identical math to .NET's
                    // managed implementation; precision matches double for the
                    // safe input domain.
                    //   sinh(x) = (exp(x) - exp(-x)) / 2
                    //   cosh(x) = (exp(x) + exp(-x)) / 2
                    //   tanh(x) = (exp(2x) - 1) / (exp(2x) + 1)
                    //     (cleaner than sinh/cosh ratio; avoids cosh-divide
                    //     numerical instability for large |x|).
                    //   asinh(x) = ln(x + sqrt(x^2 + 1))
                    //   acosh(x) = ln(x + sqrt(x^2 - 1))  (domain x >= 1)
                    //   atanh(x) = 0.5 * ln((1+x) / (1-x))  (domain |x| < 1)
                    nameof(Math.Sinh) when args.Length == 1 =>
                        $"((EXP({args[0]}) - EXP(-({args[0]}))) / 2)",
                    nameof(Math.Cosh) when args.Length == 1 =>
                        $"((EXP({args[0]}) + EXP(-({args[0]}))) / 2)",
                    nameof(Math.Tanh) when args.Length == 1 =>
                        $"((EXP(2 * ({args[0]})) - 1) / (EXP(2 * ({args[0]})) + 1))",
                    nameof(Math.Asinh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POWER({args[0]}, 2) + 1))",
                    nameof(Math.Acosh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POWER({args[0]}, 2) - 1))",
                    nameof(Math.Atanh) when args.Length == 1 =>
                        $"(0.5 * LOG((1 + ({args[0]})) / (1 - ({args[0]}))))",
                    // Extended Math methods present on SQLite but not in T-SQL.
                    // Cbrt: POWER(x, 1.0/3.0); for x >= 0 matches Math.Cbrt.
                    nameof(Math.Cbrt) when args.Length == 1 => $"POWER({args[0]}, 1.0/3.0)",
                    // T-SQL LOG(value, base): second arg is the base.
                    nameof(Math.Log2) when args.Length == 1 => $"LOG({args[0]}, 2)",
                    nameof(Math.MaxMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) >= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.MinMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) <= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.ScaleB) when args.Length == 2 =>
                        $"({args[0]} * POWER(CAST(2 AS FLOAT), {args[1]}))",
                    // BigMul(int, int) -> long. Widen one operand to BIGINT
                    // before multiplying or the product overflows when
                    // |a * b| > 2^31 - 1.
                    nameof(Math.BigMul) when args.Length == 2 =>
                        $"(CAST({args[0]} AS BIGINT) * {args[1]})",
                    nameof(Math.CopySign) when args.Length == 2 =>
                        $"(ABS({args[0]}) * SIGN({args[1]}))",
                    // Math.Clamp(value, min, max) = MIN(MAX(value, min), max).
                    // T-SQL has no scalar Min/Max so emit nested CASE.
                    nameof(Math.Clamp) when args.Length == 3 =>
                        $"(CASE WHEN (CASE WHEN {args[0]} > {args[1]} THEN {args[0]} ELSE {args[1]} END) < {args[2]} " +
                        $"THEN (CASE WHEN {args[0]} > {args[1]} THEN {args[0]} ELSE {args[1]} END) ELSE {args[2]} END)",
                    // IEEERemainder(x, y) = x - y * round(x/y, ToEven). Same banker's
                    // CASE algebra as SQLite's emit since T-SQL's ROUND is also
                    // half-away-from-zero, not ToEven.
                    nameof(Math.IEEERemainder) when args.Length == 2 =>
                        $"({args[0]} - {args[1]} * ((CASE WHEN ({args[0]})/({args[1]}) >= 0 THEN 1 ELSE -1 END) * " +
                        $"(CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) + " +
                        $"CASE " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) > 0.5 THEN 1 " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) < 0.5 THEN 0 " +
                        $"ELSE (CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) % 2) END)))",
                    _ => null
                };
            }

            if (declaringType == typeof(decimal))
            {
                return name switch
                {
                    // T-SQL ROUND with truncate flag (1) drops fractional digits.
                    nameof(decimal.Truncate) when args.Length == 1 => $"ROUND({args[0]}, 0, 1)",
                    nameof(decimal.Floor) when args.Length == 1 => $"FLOOR({args[0]})",
                    nameof(decimal.Ceiling) when args.Length == 1 => $"CEILING({args[0]})",
                    nameof(decimal.Abs) when args.Length == 1 => $"ABS({args[0]})",
                    nameof(decimal.Negate) when args.Length == 1 => $"(-({args[0]}))",
                    nameof(decimal.Add) when args.Length == 2 => $"({args[0]} + {args[1]})",
                    nameof(decimal.Subtract) when args.Length == 2 => $"({args[0]} - {args[1]})",
                    nameof(decimal.Multiply) when args.Length == 2 => $"({args[0]} * {args[1]})",
                    nameof(decimal.Divide) when args.Length == 2 => $"({args[0]} / {args[1]})",
                    nameof(decimal.Remainder) when args.Length == 2 => $"({args[0]} % {args[1]})",
                    _ => null
                };
            }

            return null;
        }

        /// <summary>
        /// Translates a JSON value access expression using SQL Server's <c>JSON_VALUE</c> function.
        /// </summary>
        /// <param name="columnName">The JSON column to access.</param>
        /// <param name="jsonPath">JSON path pointing to the desired element.</param>
        /// <returns>SQL fragment that retrieves the JSON value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="columnName"/> or <paramref name="jsonPath"/> is null.</exception>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);
            return $"JSON_VALUE({columnName}, '{jsonPath}')";
        }

        /// <summary>
        /// SQL Server ignores trailing spaces in equality comparisons, so
        /// <c>'   ' = ''</c> is TRUE. <c>DATALENGTH</c> counts raw bytes and is
        /// not affected by padding, making it the reliable empty-string test.
        /// For NVARCHAR an empty string has DATALENGTH = 0; a whitespace-only
        /// string has DATALENGTH > 0.
        /// </summary>
        public override string IsNullOrEmptySql(string colSql) =>
            $"({colSql} IS NULL OR DATALENGTH({colSql}) = 0)";
    }
}
