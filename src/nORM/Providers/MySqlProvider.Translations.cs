using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class MySqlProvider
    {
        /// <summary>
        /// Overload-aware Math.Round / decimal.Round handling. MySQL ROUND(x [, n])
        /// is AwayFromZero; TRUNCATE(x, n) truncates toward zero.
        /// </summary>
        public override string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args)
            => TryTranslateMathRoundWithMode(node, args,
                awayFromZero: (x, digits) => digits == null ? $"ROUND({x})" : $"ROUND({x}, {digits})",
                truncateTowardZero: (x, digits) => digits == null ? $"TRUNCATE({x}, 0)" : $"TRUNCATE({x}, {digits})",
                integerCastType: "SIGNED")
            ?? TryTranslateIeee754Predicate(node, args)
            ?? TryTranslateTimeSpanFactory(node, args);

        /// <summary>
        /// MySQL's <c>CAST(x AS VARCHAR(N))</c> is a syntax error - its CAST
        /// target type is <c>CHAR(N)</c>. Used by the canonical-text
        /// <see cref="DatabaseProvider.GetDateTimeOffsetFromPartsSql"/> emit.
        /// </summary>
        protected override string CastToVarchar(string sql, int width)
            => $"CAST(({sql}) AS CHAR({width}))";

        /// <summary>
        /// MySQL DOUBLE rejects NaN / Infinity at insert by default; the
        /// predicate is typically false for stored values. The (x != x)
        /// algebraic form remains correct for computed float expressions
        /// such as division-by-zero columns.
        /// </summary>
        private static string? TryTranslateIeee754Predicate(System.Linq.Expressions.MethodCallExpression node, string[] args)
        {
            var dt = node.Method.DeclaringType;
            if ((dt != typeof(double) && dt != typeof(float)) || args.Length != 1) return null;
            return node.Method.Name switch
            {
                "IsNaN" => $"({args[0]} != {args[0]})",
                // POW(10, 400) overflows to a DOUBLE that MySQL treats as the
                // out-of-range marker -- emit the algebraic form that doesn't
                // require a portable +Infinity literal.
                "IsInfinity" => $"({args[0]} = {args[0]} AND ABS({args[0]}) > 1.7976931348623157E+307)",
                "IsFinite" => $"({args[0]} = {args[0]} AND ABS({args[0]}) <= 1.7976931348623157E+307)",
                "IsPositiveInfinity" => $"({args[0]} = {args[0]} AND {args[0]} > 1.7976931348623157E+307)",
                "IsNegativeInfinity" => $"({args[0]} = {args[0]} AND {args[0]} < -1.7976931348623157E+307)",
                "IsNormal" => $"({args[0]} = {args[0]} AND ABS({args[0]}) <= 1.7976931348623157E+307 " +
                              $"AND {args[0]} != 0 AND ABS({args[0]}) >= 2.2250738585072014E-308)",
                "IsSubnormal" => $"({args[0]} != 0 AND ABS({args[0]}) < 2.2250738585072014E-308)",
                _ => null
            };
        }

        /// <summary>
        /// Translates selected .NET methods to their MySQL SQL equivalents.
        /// </summary>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(char.ToUpperInvariant) => $"UPPER({args[0]})",
                    nameof(char.ToLowerInvariant) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"CHAR_LENGTH({args[0]})",
                    nameof(string.Trim) when args.Length == 1 => $"TRIM({args[0]})",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // MySQL LPAD/RPAD truncates when input length exceeds the
                    // target width; gate with CASE so longer inputs pass
                    // through unchanged, matching .NET PadLeft/PadRight's
                    // never-truncate contract.
                    nameof(string.PadLeft) when args.Length == 2 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE LPAD({args[0]}, {args[1]}, ' ') END)",
                    nameof(string.PadLeft) when args.Length == 3 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE LPAD({args[0]}, {args[1]}, {args[2]}) END)",
                    nameof(string.PadRight) when args.Length == 2 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE RPAD({args[0]}, {args[1]}, ' ') END)",
                    nameof(string.PadRight) when args.Length == 3 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE RPAD({args[0]}, {args[1]}, {args[2]}) END)",
                    // MySQL SUBSTRING is 1-indexed; .NET Substring is 0-indexed, add 1.
                    nameof(string.Substring) when args.Length == 2 => $"SUBSTRING({args[0]}, ({args[1]}) + 1)",
                    nameof(string.Substring) when args.Length == 3 => $"SUBSTRING({args[0]}, ({args[1]}) + 1, {args[2]})",
                    nameof(string.Replace) when args.Length == 3 => $"REPLACE({args[0]}, {args[1]}, {args[2]})",
                    // LOCATE returns 1-based position or 0 if not found; .NET IndexOf is 0-based
                    // returning -1, so subtract 1.
                    nameof(string.IndexOf) when args.Length == 2 => $"(LOCATE({args[1]}, {args[0]}) - 1)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTimeOffset))
            {
                // MySQL has no native DATETIMEOFFSET type. MySqlConnector
                // stores DateTimeOffset as DATETIME / TIMESTAMP normalized
                // to UTC, so the column represents a UTC instant and the
                // offset portion is always TimeSpan.Zero from a query
                // perspective.
                var dtoMatch = name switch
                {
                    nameof(DateTimeOffset.UtcDateTime) => args[0],
                    nameof(DateTimeOffset.DateTime) => args[0],
                    // REAL-seconds emit for the materializer's double ->
                    // TimeSpan.FromSeconds path, yielding TimeSpan.Zero.
                    nameof(DateTimeOffset.Offset) => "CAST(0 AS DOUBLE)",
                    _ => null
                };
                if (dtoMatch != null) return dtoMatch;
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"YEAR({args[0]})",
                    nameof(DateTime.Month) => $"MONTH({args[0]})",
                    nameof(DateTime.Day) => $"DAY({args[0]})",
                    nameof(DateTime.Hour) => $"HOUR({args[0]})",
                    nameof(DateTime.Minute) => $"MINUTE({args[0]})",
                    nameof(DateTime.Second) => $"SECOND({args[0]})",
                    nameof(DateTime.DayOfYear) => $"DAYOFYEAR({args[0]})",
                    nameof(DateTime.Date) => $"DATE({args[0]})",
                    nameof(DateTime.AddDays) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) DAY)",
                    nameof(DateTime.AddMonths) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) MONTH)",
                    nameof(DateTime.AddYears) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) YEAR)",
                    nameof(DateTime.AddHours) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) HOUR)",
                    nameof(DateTime.AddMinutes) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) MINUTE)",
                    nameof(DateTime.AddSeconds) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) SECOND)",
                    // MySQL DATETIME(6) supports microsecond precision; ms*1000 = microseconds.
                    nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL (({args[1]}) * 1000) MICROSECOND)",
                    // MySQL DOES NOT expose MILLISECOND() but MICROSECOND returns
                    // 0..999999; integer-divide by 1000 to get the ms component.
                    nameof(DateTime.Millisecond) => $"(MICROSECOND({args[0]}) DIV 1000)",
                    nameof(DateTime.Microsecond) => $"(MICROSECOND({args[0]}) % 1000)",
                    // MySQL DATETIME(6) has microsecond precision; the sub-
                    // microsecond Nanosecond is always zero.
                    nameof(DateTime.Nanosecond) => "0",
                    // 1 tick = 100ns. MySQL's smallest interval unit is MICROSECOND so
                    // ticks/10 gives microseconds (sub-microsecond truncates).
                    nameof(DateTime.AddTicks) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL (({args[1]}) / 10) MICROSECOND)",
                    // MySQL DAYOFWEEK returns 1=Sun..7=Sat; .NET DayOfWeek is 0=Sun..6=Sat.
                    nameof(DateTime.DayOfWeek) => $"(DAYOFWEEK({args[0]}) - 1)",
                    // Compare / CompareTo: signed -1/0/1 sentinel via CASE.
                    nameof(DateTime.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.IsLeapYear) when args.Length == 1 => BuildIsLeapYearSql(args[0]),
                    nameof(DateTime.DaysInMonth) when args.Length == 2 => BuildDaysInMonthSql(args[0], args[1]),
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
                    nameof(DateOnly.DayOfYear) => $"DAYOFYEAR({args[0]})",
                    // MySQL DAYOFWEEK returns 1=Sun..7=Sat; .NET DayOfWeek is 0=Sun..6=Sat.
                    nameof(DateOnly.DayOfWeek) => $"(DAYOFWEEK({args[0]}) - 1)",
                    nameof(DateOnly.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    // MySQL TO_DAYS uses proleptic Gregorian anchored at year 0;
                    // TO_DAYS('0001-01-01') == 366 (year 0 was 366 days). Subtract
                    // 366 so the result matches .NET DateOnly.MinValue == day 0.
                    nameof(DateOnly.DayNumber) => $"(TO_DAYS({args[0]}) - 366)",
                    nameof(DateOnly.FromDayNumber) when args.Length == 1 =>
                        $"FROM_DAYS(({args[0]}) + 366)",
                    nameof(DateOnly.FromDateTime) when args.Length == 1 =>
                        $"DATE({args[0]})",
                    // MySQL TIMESTAMP(date, time) combines into a DATETIME.
                    nameof(DateOnly.ToDateTime) when args.Length == 2 =>
                        $"TIMESTAMP({args[0]}, {args[1]})",
                    _ => null
                };
            }

            if (declaringType == typeof(TimeOnly))
            {
                return name switch
                {
                    nameof(TimeOnly.Hour) => $"HOUR({args[0]})",
                    nameof(TimeOnly.Minute) => $"MINUTE({args[0]})",
                    nameof(TimeOnly.Second) => $"SECOND({args[0]})",
                    nameof(TimeOnly.Millisecond) => $"(MICROSECOND({args[0]}) DIV 1000)",
                    // MySQL TIME() extracts the TIME portion from a DATETIME
                    // or coerces a TIME-typed expression.
                    nameof(TimeOnly.FromDateTime) when args.Length == 1 => $"TIME({args[0]})",
                    nameof(TimeOnly.FromTimeSpan) when args.Length == 1 => $"TIME({args[0]})",
                    nameof(TimeOnly.Microsecond) => $"(MICROSECOND({args[0]}) % 1000)",
                    nameof(TimeOnly.Nanosecond) => "0",
                    // TIME_TO_SEC returns the whole-second count; multiply by
                    // 10_000_000 ticks/sec. MICROSECOND yields 0..999999;
                    // multiply by 10 to get ticks (MySQL has microsecond precision).
                    nameof(TimeOnly.Ticks) => $"(TIME_TO_SEC({args[0]}) * 10000000 + MICROSECOND({args[0]}) * 10)",
                    // IsBetween(start, end) wraps around midnight when start > end.
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
                    // MySQL LIKE is case-insensitive on case-insensitive collations (the
                    // default for utf8mb4_general_ci) but case-sensitive on _bin / _cs
                    // collations. Forcing LOWER on both sides guarantees consistent
                    // ILIKE semantics regardless of column collation.
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
                    // MySQL UTC_TIMESTAMP returns DATETIME in UTC; UUID() yields
                    // a 36-char hex-with-dashes string compatible with .NET Guid
                    // via Guid.Parse on the reader; RAND() is double in [0, 1).
                    nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "UTC_TIMESTAMP()",
                    nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => "UUID()",
                    nameof(NormFunctions.ServerRandom) when args.Length == 0 => "RAND()",
                    _ => null
                };
            }

            if (declaringType == typeof(Guid) && name == nameof(Guid.NewGuid) && args.Length == 0)
                return "UUID()";

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEILING({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    nameof(Math.Sqrt) when args.Length == 1 => $"SQRT({args[0]})",
                    nameof(Math.Pow) when args.Length == 2 => $"POW({args[0]}, {args[1]})",
                    nameof(Math.Exp) when args.Length == 1 => $"EXP({args[0]})",
                    nameof(Math.Log) when args.Length == 1 => $"LN({args[0]})",
                    nameof(Math.Log) when args.Length == 2 => $"LOG({args[1]}, {args[0]})",
                    nameof(Math.Log10) when args.Length == 1 => $"LOG10({args[0]})",
                    nameof(Math.Sign) when args.Length == 1 => $"SIGN({args[0]})",
                    nameof(Math.Truncate) when args.Length == 1 => $"TRUNCATE({args[0]}, 0)",
                    nameof(Math.Min) when args.Length == 2 => $"LEAST({args[0]}, {args[1]})",
                    nameof(Math.Max) when args.Length == 2 => $"GREATEST({args[0]}, {args[1]})",
                    // Basic trig + inverse trig + 2-arg arctangent. All native
                    // in MySQL with .NET-matching names.
                    nameof(Math.Sin) when args.Length == 1 => $"SIN({args[0]})",
                    nameof(Math.Cos) when args.Length == 1 => $"COS({args[0]})",
                    nameof(Math.Tan) when args.Length == 1 => $"TAN({args[0]})",
                    nameof(Math.Asin) when args.Length == 1 => $"ASIN({args[0]})",
                    nameof(Math.Acos) when args.Length == 1 => $"ACOS({args[0]})",
                    nameof(Math.Atan) when args.Length == 1 => $"ATAN({args[0]})",
                    nameof(Math.Atan2) when args.Length == 2 => $"ATAN2({args[0]}, {args[1]})",
                    // Hyperbolic + inverse hyperbolic: MySQL has no native
                    // hyperbolic functions. Emit via algebraic identities
                    // using EXP / LOG (single-arg LOG is natural log in MySQL,
                    // matching SQL Server's LOG) / SQRT / POW. Same identities
                    // used in the SQL Server branch -- see that comment for
                    // the formula derivation and domain notes.
                    nameof(Math.Sinh) when args.Length == 1 =>
                        $"((EXP({args[0]}) - EXP(-({args[0]}))) / 2)",
                    nameof(Math.Cosh) when args.Length == 1 =>
                        $"((EXP({args[0]}) + EXP(-({args[0]}))) / 2)",
                    nameof(Math.Tanh) when args.Length == 1 =>
                        $"((EXP(2 * ({args[0]})) - 1) / (EXP(2 * ({args[0]})) + 1))",
                    nameof(Math.Asinh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POW({args[0]}, 2) + 1))",
                    nameof(Math.Acosh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POW({args[0]}, 2) - 1))",
                    nameof(Math.Atanh) when args.Length == 1 =>
                        $"(0.5 * LOG((1 + ({args[0]})) / (1 - ({args[0]}))))",
                    // Extended Math methods. MySQL has POW and LOG(B, X).
                    nameof(Math.Cbrt) when args.Length == 1 => $"POW({args[0]}, 1.0/3.0)",
                    // MySQL LOG(B, X): base first.
                    nameof(Math.Log2) when args.Length == 1 => $"LOG(2, {args[0]})",
                    nameof(Math.MaxMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) >= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.MinMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) <= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.ScaleB) when args.Length == 2 =>
                        $"({args[0]} * POW(2.0, {args[1]}))",
                    // MySQL SIGNED is 64-bit (BIGINT range); cast widens
                    // the int*int product so |a*b| > 2^31 doesn't overflow.
                    nameof(Math.BigMul) when args.Length == 2 =>
                        $"(CAST({args[0]} AS SIGNED) * {args[1]})",
                    nameof(Math.CopySign) when args.Length == 2 =>
                        $"(ABS({args[0]}) * SIGN({args[1]}))",
                    // Math.Clamp = LEAST(GREATEST(v, min), max). MySQL has both.
                    nameof(Math.Clamp) when args.Length == 3 =>
                        $"LEAST(GREATEST({args[0]}, {args[1]}), {args[2]})",
                    // IEEERemainder via banker's-rounding CASE. MySQL ROUND is
                    // half-away-from-zero by default, so we inline the same
                    // algebra used by SQLite/SqlServer/Postgres.
                    nameof(Math.IEEERemainder) when args.Length == 2 =>
                        $"({args[0]} - {args[1]} * ((CASE WHEN ({args[0]})/({args[1]}) >= 0 THEN 1 ELSE -1 END) * " +
                        $"(CAST(ABS(({args[0]})/({args[1]})) AS SIGNED) + " +
                        $"CASE " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS SIGNED) > 0.5 THEN 1 " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS SIGNED) < 0.5 THEN 0 " +
                        $"ELSE (CAST(ABS(({args[0]})/({args[1]})) AS SIGNED) % 2) END)))",
                    _ => null
                };
            }

            if (declaringType == typeof(decimal))
            {
                return name switch
                {
                    nameof(decimal.Truncate) when args.Length == 1 => $"TRUNCATE({args[0]}, 0)",
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

            return null;
        }

        /// <summary>
        /// Translates access to a JSON value using MySQL's <c>JSON_EXTRACT</c> function.
        /// </summary>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);
            return $"JSON_UNQUOTE(JSON_EXTRACT({columnName}, '{jsonPath}'))";
        }
    }
}
