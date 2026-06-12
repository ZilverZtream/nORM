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
        /// <summary>
        /// Overload-aware Math.Round / decimal.Round handling. PostgreSQL's
        /// ROUND(double, int) doesn't exist -- ROUND with two args requires
        /// numeric. For AwayFromZero on doubles, cast to numeric, round,
        /// then back to double precision. TRUNC handles ToZero.
        /// </summary>
        public override string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args)
            => TryTranslateMathRoundWithMode(node, args,
                awayFromZero: (x, digits) => digits == null
                    ? $"ROUND(({x})::numeric)"
                    : $"ROUND(({x})::numeric, {digits})",
                truncateTowardZero: (x, digits) => digits == null
                    ? $"TRUNC(({x})::numeric)"
                    : $"TRUNC(({x})::numeric, {digits})")
            ?? TryTranslateIeee754Predicate(node, args)
            ?? TryTranslateTimeSpanFactory(node, args);

        /// <summary>
        /// IEEE 754 predicates -- PostgreSQL stores Infinity / NaN in DOUBLE
        /// PRECISION natively. Use native isnan() for IsNaN and the
        /// 'Infinity'::float8 literal for IsInfinity.
        /// </summary>
        private static string? TryTranslateIeee754Predicate(System.Linq.Expressions.MethodCallExpression node, string[] args)
        {
            var dt = node.Method.DeclaringType;
            if ((dt != typeof(double) && dt != typeof(float)) || args.Length != 1) return null;
            return node.Method.Name switch
            {
                "IsNaN" => $"isnan({args[0]})",
                "IsInfinity" => $"({args[0]} = 'Infinity'::float8 OR {args[0]} = '-Infinity'::float8)",
                "IsFinite" => $"(NOT isnan({args[0]}) AND {args[0]} != 'Infinity'::float8 AND {args[0]} != '-Infinity'::float8)",
                "IsPositiveInfinity" => $"({args[0]} = 'Infinity'::float8)",
                "IsNegativeInfinity" => $"({args[0]} = '-Infinity'::float8)",
                "IsNormal" => $"(NOT isnan({args[0]}) AND {args[0]} != 'Infinity'::float8 AND {args[0]} != '-Infinity'::float8 " +
                              $"AND {args[0]} != 0 AND ABS({args[0]}) >= 2.2250738585072014E-308)",
                "IsSubnormal" => $"({args[0]} != 0 AND ABS({args[0]}) < 2.2250738585072014E-308)",
                _ => null
            };
        }

        /// <summary>
        /// Attempts to translate a .NET method invocation into its PostgreSQL equivalent.
        /// </summary>
        /// <param name="name">Name of the .NET method being translated.</param>
        /// <param name="declaringType">Type that declares the method.</param>
        /// <param name="args">SQL fragments representing the method arguments.</param>
        /// <returns>The translated SQL expression or <c>null</c> if the method is not supported.</returns>

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
                    nameof(string.Length) when args.Length == 1 => $"LENGTH({args[0]})",
                    nameof(string.Trim) when args.Length == 1 => $"BTRIM({args[0]})",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // PostgreSQL LPAD/RPAD: when the input is already longer
                    // than the target width LPAD truncates from the LEFT
                    // (returning the right N chars), which DIVERGES from
                    // .NET PadLeft's never-truncate contract. Gate with CASE
                    // so longer inputs pass through unchanged.
                    nameof(string.PadLeft) when args.Length == 2 =>
                        $"(CASE WHEN LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE LPAD({args[0]}, {args[1]}, ' ') END)",
                    nameof(string.PadLeft) when args.Length == 3 =>
                        $"(CASE WHEN LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE LPAD({args[0]}, {args[1]}, {args[2]}) END)",
                    nameof(string.PadRight) when args.Length == 2 =>
                        $"(CASE WHEN LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE RPAD({args[0]}, {args[1]}, ' ') END)",
                    nameof(string.PadRight) when args.Length == 3 =>
                        $"(CASE WHEN LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE RPAD({args[0]}, {args[1]}, {args[2]}) END)",
                    // PostgreSQL SUBSTRING is 1-indexed; .NET Substring is 0-indexed, add 1.
                    nameof(string.Substring) when args.Length == 2 => $"SUBSTRING({args[0]} FROM ({args[1]}) + 1)",
                    nameof(string.Substring) when args.Length == 3 => $"SUBSTRING({args[0]} FROM ({args[1]}) + 1 FOR {args[2]})",
                    nameof(string.Replace) when args.Length == 3 => $"REPLACE({args[0]}, {args[1]}, {args[2]})",
                    // STRPOS returns 1-based position or 0 if not found; .NET IndexOf is 0-based
                    // returning -1, so subtract 1.
                    nameof(string.IndexOf) when args.Length == 2 => $"(STRPOS({args[0]}, {args[1]}) - 1)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTimeOffset))
            {
                // TIMESTAMPTZ stores a UTC instant -- Npgsql normalizes any
                // stored offset to the session's UTC representation, so the
                // "offset" portion is effectively always 0 from the LINQ-side
                // perspective. UtcDateTime / DateTime both unwrap to a
                // session-independent TIMESTAMP via AT TIME ZONE 'UTC'.
                var dtoMatch = name switch
                {
                    nameof(DateTimeOffset.UtcDateTime) => $"({args[0]} AT TIME ZONE 'UTC')",
                    nameof(DateTimeOffset.DateTime) => $"({args[0]} AT TIME ZONE 'UTC')",
                    // Always TimeSpan.Zero: emit as REAL seconds so the
                    // materializer's double -> TimeSpan.FromSeconds path
                    // reconstructs TimeSpan.Zero.
                    nameof(DateTimeOffset.Offset) => "CAST(0 AS DOUBLE PRECISION)",
                    _ => null
                };
                if (dtoMatch != null) return dtoMatch;
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
                var source = declaringType == typeof(DateTimeOffset)
                    ? $"({args[0]} AT TIME ZONE 'UTC')"
                    : args[0];
                return name switch
                {
                    nameof(DateTime.Year) => $"EXTRACT(YEAR FROM {source})",
                    nameof(DateTime.Month) => $"EXTRACT(MONTH FROM {source})",
                    nameof(DateTime.Day) => $"EXTRACT(DAY FROM {source})",
                    nameof(DateTime.Hour) => $"EXTRACT(HOUR FROM {source})",
                    nameof(DateTime.Minute) => $"EXTRACT(MINUTE FROM {source})",
                    nameof(DateTime.Second) => $"EXTRACT(SECOND FROM {source})",
                    nameof(DateTime.DayOfYear) => $"EXTRACT(DOY FROM {source})",
                    nameof(DateTime.Date) => $"DATE_TRUNC('day', {source})",
                    nameof(DateTime.AddDays) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 day')",
                    nameof(DateTime.AddMonths) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 month')",
                    nameof(DateTime.AddYears) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 year')",
                    nameof(DateTime.AddHours) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 hour')",
                    nameof(DateTime.AddMinutes) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 minute')",
                    nameof(DateTime.AddSeconds) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 second')",
                    // PostgreSQL supports millisecond interval natively.
                    nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"({args[0]} + ({args[1]}) * INTERVAL '1 millisecond')",
                    // Postgres EXTRACT(MILLISECONDS FROM ts) returns SS*1000+ms;
                    // modulo 1000 yields the millisecond component matching .NET.
                    nameof(DateTime.Millisecond) => $"(EXTRACT(MILLISECONDS FROM {source})::int % 1000)",
                    // EXTRACT(MICROSECONDS FROM ts) returns SS*1e6+us; %1000
                    // yields the microsecond within the current millisecond.
                    nameof(DateTime.Microsecond) => $"((EXTRACT(MICROSECONDS FROM {source}))::bigint % 1000)",
                    // PostgreSQL timestamps have microsecond precision; the
                    // sub-microsecond Nanosecond is always zero.
                    nameof(DateTime.Nanosecond) => "0",
                    // PostgreSQL TIMESTAMP precision is microseconds; 1 tick = 100ns, so
                    // ticks/10 gives microseconds (truncating sub-microsecond precision).
                    nameof(DateTime.AddTicks) when args.Length == 2 => $"({args[0]} + (({args[1]}) / 10) * INTERVAL '1 microsecond')",
                    // PostgreSQL EXTRACT(DOW) returns 0=Sunday..6=Saturday — matches System.DayOfWeek.
                    nameof(DateTime.DayOfWeek) => $"EXTRACT(DOW FROM {source})",
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
                    nameof(DateOnly.Year) => $"EXTRACT(YEAR FROM {args[0]})",
                    nameof(DateOnly.Month) => $"EXTRACT(MONTH FROM {args[0]})",
                    nameof(DateOnly.Day) => $"EXTRACT(DAY FROM {args[0]})",
                    nameof(DateOnly.DayOfYear) => $"EXTRACT(DOY FROM {args[0]})",
                    // PostgreSQL EXTRACT(DOW) returns 0=Sunday..6=Saturday — matches System.DayOfWeek.
                    nameof(DateOnly.DayOfWeek) => $"EXTRACT(DOW FROM {args[0]})",
                    nameof(DateOnly.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    // PostgreSQL `date - date` returns int (days). Anchor on
                    // DATE '0001-01-01' to match .NET DateOnly.MinValue == day 0.
                    nameof(DateOnly.DayNumber) => $"({args[0]} - DATE '0001-01-01')",
                    nameof(DateOnly.FromDayNumber) when args.Length == 1 =>
                        $"(DATE '0001-01-01' + ({args[0]}))",
                    nameof(DateOnly.FromDateTime) when args.Length == 1 =>
                        $"({args[0]})::date",
                    // PostgreSQL date + time = timestamp natively (no cast needed).
                    nameof(DateOnly.ToDateTime) when args.Length == 2 =>
                        $"({args[0]} + {args[1]})",
                    _ => null
                };
            }

            if (declaringType == typeof(TimeOnly))
            {
                return name switch
                {
                    nameof(TimeOnly.Hour) => $"EXTRACT(HOUR FROM {args[0]})",
                    nameof(TimeOnly.Minute) => $"EXTRACT(MINUTE FROM {args[0]})",
                    nameof(TimeOnly.Second) => $"EXTRACT(SECOND FROM {args[0]})",
                    nameof(TimeOnly.Millisecond) => $"(EXTRACT(MILLISECONDS FROM {args[0]})::int % 1000)",
                    // PostgreSQL cast TIMESTAMP -> TIME and INTERVAL -> TIME.
                    nameof(TimeOnly.FromDateTime) when args.Length == 1 => $"({args[0]})::time",
                    nameof(TimeOnly.FromTimeSpan) when args.Length == 1 => $"({args[0]})::time",
                    nameof(TimeOnly.Microsecond) => $"((EXTRACT(MICROSECONDS FROM {args[0]}))::bigint % 1000)",
                    nameof(TimeOnly.Nanosecond) => "0",
                    // EXTRACT(EPOCH FROM time) returns seconds-since-midnight as
                    // numeric (with sub-second fraction). x 10_000_000 yields
                    // 100ns ticks; truncate via ::bigint for the long return.
                    nameof(TimeOnly.Ticks) => $"((EXTRACT(EPOCH FROM {args[0]}) * 10000000)::bigint)",
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
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"({args[0]} ILIKE {args[1]})",
                    // gen_random_uuid requires the pgcrypto extension (or the
                    // pg-built-in available since v13). RANDOM() returns
                    // double in [0, 1).
                    nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "(NOW() AT TIME ZONE 'UTC')",
                    nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => "gen_random_uuid()",
                    nameof(NormFunctions.ServerRandom) when args.Length == 0 => "RANDOM()",
                    _ => null
                };
            }

            if (declaringType == typeof(Guid) && name == nameof(Guid.NewGuid) && args.Length == 0)
                return "gen_random_uuid()";

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEILING({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    nameof(Math.Sqrt) when args.Length == 1 => $"SQRT(CASE WHEN {args[0]} < 0 THEN NULL ELSE {args[0]} END)",
                    nameof(Math.Pow) when args.Length == 2 => $"POWER({args[0]}, {args[1]})",
                    nameof(Math.Exp) when args.Length == 1 => $"EXP({args[0]})",
                    nameof(Math.Log) when args.Length == 1 => $"LN({args[0]})",
                    nameof(Math.Log) when args.Length == 2 => $"LOG({args[1]}, {args[0]})",
                    nameof(Math.Log10) when args.Length == 1 => $"LOG({args[0]})",
                    nameof(Math.Sign) when args.Length == 1 => $"SIGN({args[0]})",
                    nameof(Math.Truncate) when args.Length == 1 => $"TRUNC({args[0]})",
                    nameof(Math.Min) when args.Length == 2 => $"LEAST({args[0]}, {args[1]})",
                    nameof(Math.Max) when args.Length == 2 => $"GREATEST({args[0]}, {args[1]})",
                    // Basic trig + inverse trig + 2-arg arctangent. All native
                    // in PostgreSQL with .NET-matching names.
                    nameof(Math.Sin) when args.Length == 1 => $"SIN({args[0]})",
                    nameof(Math.Cos) when args.Length == 1 => $"COS({args[0]})",
                    nameof(Math.Tan) when args.Length == 1 => $"TAN({args[0]})",
                    nameof(Math.Asin) when args.Length == 1 => $"ASIN({args[0]})",
                    nameof(Math.Acos) when args.Length == 1 => $"ACOS({args[0]})",
                    nameof(Math.Atan) when args.Length == 1 => $"ATAN({args[0]})",
                    nameof(Math.Atan2) when args.Length == 2 => $"ATAN2({args[0]}, {args[1]})",
                    // Hyperbolic + inverse hyperbolic: PostgreSQL exposes
                    // them all as built-ins with .NET-matching names.
                    nameof(Math.Sinh) when args.Length == 1 => $"SINH({args[0]})",
                    nameof(Math.Cosh) when args.Length == 1 => $"COSH({args[0]})",
                    nameof(Math.Tanh) when args.Length == 1 => $"TANH({args[0]})",
                    nameof(Math.Asinh) when args.Length == 1 => $"ASINH({args[0]})",
                    nameof(Math.Acosh) when args.Length == 1 => $"ACOSH({args[0]})",
                    nameof(Math.Atanh) when args.Length == 1 => $"ATANH({args[0]})",
                    // Extended Math methods. PostgreSQL has CBRT natively;
                    // wrap with POW for parity with the test anchor while
                    // matching the algebraic value.
                    nameof(Math.Cbrt) when args.Length == 1 => $"POW({args[0]}, 1.0/3.0)",
                    // PostgreSQL LOG(base, num): base first.
                    nameof(Math.Log2) when args.Length == 1 => $"LOG(2, {args[0]})",
                    nameof(Math.MaxMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) >= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.MinMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) <= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.ScaleB) when args.Length == 2 =>
                        $"({args[0]} * POW(2.0, {args[1]}))",
                    nameof(Math.BigMul) when args.Length == 2 =>
                        $"(CAST({args[0]} AS BIGINT) * {args[1]})",
                    nameof(Math.CopySign) when args.Length == 2 =>
                        $"(ABS({args[0]}) * SIGN({args[1]}))",
                    // Math.Clamp = LEAST(GREATEST(v, min), max). Postgres has both
                    // GREATEST and LEAST as variadic functions.
                    nameof(Math.Clamp) when args.Length == 3 =>
                        $"LEAST(GREATEST({args[0]}, {args[1]}), {args[2]})",
                    // IEEERemainder via banker's-rounding CASE. Postgres ROUND on
                    // double-precision is half-away-from-zero, not ToEven, so we
                    // inline the same algebra used by SQLite/SqlServer.
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
                    nameof(decimal.Truncate) when args.Length == 1 => $"TRUNC({args[0]})",
                    nameof(decimal.Floor) when args.Length == 1 => $"FLOOR({args[0]})",
                    nameof(decimal.Ceiling) when args.Length == 1 => $"CEILING({args[0]})",
                    nameof(decimal.Abs) when args.Length == 1 => $"ABS({args[0]})",
                    nameof(decimal.Negate) when args.Length == 1 => $"(-({args[0]}))",
                    nameof(decimal.Add) when args.Length == 2 => $"({args[0]} + {args[1]})",
                    nameof(decimal.Subtract) when args.Length == 2 => $"({args[0]} - {args[1]})",
                    nameof(decimal.Multiply) when args.Length == 2 => $"({args[0]} * {args[1]})",
                    nameof(decimal.Divide) when args.Length == 2 => $"({args[0]} / {args[1]})",
                    // PostgreSQL `numeric % numeric` returns the remainder.
                    nameof(decimal.Remainder) when args.Length == 2 => $"({args[0]} % {args[1]})",
                    _ => null
                };
            }

            return null;
        }

    }
}
