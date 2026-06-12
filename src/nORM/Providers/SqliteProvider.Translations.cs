using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {        /// <summary>
        /// Overload-aware hook -- called by the visitors BEFORE
        /// <see cref="TranslateFunction"/>. Used here to distinguish Math.Round
        /// overloads that share arity but differ in semantics:
        ///   * 1-arg  Math.Round(x)                   -> .NET default ToEven (banker's)
        ///   * 2-arg  Math.Round(x, int)              -> default ToEven, rounded to N digits
        ///   * 2-arg  Math.Round(x, MidpointRounding) -> mode-driven
        ///   * 3-arg  Math.Round(x, int, MidpointRounding) -> mode-driven, N digits
        /// SQLite's native ROUND uses AwayFromZero, so the default-mode paths
        /// emit a banker's-rounding CASE (sister to the IEEERemainder emit).
        /// </summary>
        public override string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args)
        {
            var declType = node.Method.DeclaringType;

            // TimeSpan factory static methods (FromDays/FromHours/FromMinutes/
            // FromSeconds/FromMilliseconds/FromTicks) -- emit printf producing the
            // canonical 'd.HH:mm:ss.fffffff' text the TimeSpan materializer parses.
            // SQLite has no native TimeSpan type, so Microsoft.Data.Sqlite reads
            // TEXT columns via TimeSpan.Parse for TimeSpan-typed projection slots.
            //
            // Day-prefix `d.` is REQUIRED for hour fields >= 24 (e.g. FromHours(48)).
            // TimeSpan.Parse("48:00:00") throws OverflowException because the first
            // segment is constrained to [0,23] when no day prefix is present. Always
            // emitting `0.` for sub-day spans is parser-compatible: Parse accepts
            // both "0.02:30:00" and "02:30:00" as 2h30m.
            //
            // Unified total-ticks reduction: every factory has an integer multiplier
            // to ticks (1 sec = 10_000_000 ticks). Computing the integer tick count
            // once and splitting into d/hh/mm/ss/frac via integer arithmetic
            // preserves sub-second precision for FromMilliseconds / FromTicks. For
            // float-arg factories the multiplier-times-double may lose 1 tick of
            // precision for non-representable values (e.g. FromHours(1.0/3.0)) --
            // documented limitation matching .NET's IEEE-754 semantics.
            //
            // Negative spans are handled via abs(ticks) + '-' prefix per the
            // canonical 'c' format ("-01:00:00" for TimeSpan.FromHours(-1)).
            if (declType == typeof(TimeSpan)
                && node.Object == null
                && args.Length == 1)
            {
                // Tick multiplier per factory:
                //   FromDays         = arg * 864_000_000_000
                //   FromHours        = arg *  36_000_000_000
                //   FromMinutes      = arg *     600_000_000
                //   FromSeconds      = arg *      10_000_000
                //   FromMilliseconds = arg *          10_000
                //   FromTicks        = arg
                string? totalTicksSql = node.Method.Name switch
                {
                    nameof(TimeSpan.FromDays)         => $"CAST(({args[0]}) * 864000000000.0 AS INTEGER)",
                    nameof(TimeSpan.FromHours)        => $"CAST(({args[0]}) * 36000000000.0 AS INTEGER)",
                    nameof(TimeSpan.FromMinutes)      => $"CAST(({args[0]}) * 600000000.0 AS INTEGER)",
                    nameof(TimeSpan.FromSeconds)      => $"CAST(({args[0]}) * 10000000.0 AS INTEGER)",
                    nameof(TimeSpan.FromMilliseconds) => $"CAST(({args[0]}) * 10000.0 AS INTEGER)",
                    nameof(TimeSpan.FromTicks)        => $"CAST(({args[0]}) AS INTEGER)",
                    _ => null
                };
                if (totalTicksSql != null)
                {
                    // 4-way CASE matching TimeSpan.ToString("c") exactly so the
                    // column-side text lex-compares against parameter-side binding
                    // (Microsoft.Data.Sqlite uses 'c' format). The 'c' format omits
                    // the day prefix for sub-day spans and the fractional suffix
                    // when ticks are an integer-second boundary -- four shapes:
                    //   0 days, 0 frac:  HH:mm:ss
                    //   0 days, frac:    HH:mm:ss.fffffff
                    //   >0 days, 0 frac: d.HH:mm:ss
                    //   >0 days, frac:   d.HH:mm:ss.fffffff
                    // For negative spans, 'c' prefixes the whole span with '-' on
                    // top of the same field layout: e.g. TimeSpan.FromHours(-0.5)
                    // -> "-00:30:00". SQLite's signed modulo embeds the sign in
                    // individual fields ("00:-30:00") -- WRONG, TimeSpan.Parse
                    // rejects it. Compute on abs(ticks) and prepend '-' via a
                    // sign-aware outer CASE.
                    // Without exact format match, WHERE comparisons of
                    // TimeSpan.FromHours(col) > parameter fail lexically because
                    // '0.02:00:00.0000000' (column with day prefix) sorts before
                    // '01:00:00' (parameter without).
                    var absTicks = $"abs({totalTicksSql})";
                    var daysExpr = $"(({absTicks}) / 864000000000)";
                    var hExpr    = $"((({absTicks}) / 36000000000) % 24)";
                    var mExpr    = $"((({absTicks}) / 600000000) % 60)";
                    var sExpr    = $"((({absTicks}) / 10000000) % 60)";
                    var fExpr    = $"(({absTicks}) % 10000000)";
                    var signExpr = $"(CASE WHEN ({totalTicksSql}) < 0 THEN '-' ELSE '' END)";
                    var bodyExpr =
                        "(CASE " +
                           $"WHEN ({absTicks}) >= 864000000000 AND ({absTicks}) % 10000000 != 0 " +
                                $"THEN printf('%d.%02d:%02d:%02d.%07d', {daysExpr}, {hExpr}, {mExpr}, {sExpr}, {fExpr}) " +
                           $"WHEN ({absTicks}) >= 864000000000 " +
                                $"THEN printf('%d.%02d:%02d:%02d', {daysExpr}, {hExpr}, {mExpr}, {sExpr}) " +
                           $"WHEN ({absTicks}) % 10000000 != 0 " +
                                $"THEN printf('%02d:%02d:%02d.%07d', {hExpr}, {mExpr}, {sExpr}, {fExpr}) " +
                           $"ELSE printf('%02d:%02d:%02d', {hExpr}, {mExpr}, {sExpr}) END)";
                    return $"({signExpr} || {bodyExpr})";
                }
            }

            // DateTime/DateTimeOffset/DateOnly/TimeOnly.ParseExact(s, format
            // [, provider[, style]]) -- restricted to a small set of constant
            // format strings we can losslessly rewrite into the canonical
            // 'yyyy-MM-dd HH:MM:SS' the materializer reads. The constant-
            // format guard keeps the rewrite scope honest -- arbitrary format
            // strings can't be re-implemented in pure SQL.
            if ((declType == typeof(DateTime) || declType == typeof(DateTimeOffset)
                 || declType == typeof(DateOnly) || declType == typeof(TimeOnly))
                && node.Method.Name == "ParseExact"
                && node.Arguments.Count >= 2
                && node.Arguments[1] is System.Linq.Expressions.ConstantExpression fmtArg
                && fmtArg.Value is string fmt)
            {
                // args[0] is the receiver (none for static); ParseExact is static
                // so node.Object is null and args[0] is the source string SQL.
                var src = args[0];
                switch (fmt)
                {
                    case "yyyyMMdd":
                        return $"(SUBSTR({src},1,4) || '-' || SUBSTR({src},5,2) || '-' || SUBSTR({src},7,2))";
                    case "yyyy-MM-dd":
                        return src;
                    case "yyyyMMddHHmmss":
                        return $"(SUBSTR({src},1,4) || '-' || SUBSTR({src},5,2) || '-' || SUBSTR({src},7,2) || ' ' || SUBSTR({src},9,2) || ':' || SUBSTR({src},11,2) || ':' || SUBSTR({src},13,2))";
                    case "yyyy-MM-dd HH:mm:ss":
                        return src;
                    default:
                        throw new NormUnsupportedFeatureException(
                            $"DateTime.ParseExact format \"{fmt}\" is not supported in SQL translation. " +
                            "Supported formats: yyyyMMdd, yyyy-MM-dd, yyyyMMddHHmmss, yyyy-MM-dd HH:mm:ss. " +
                            "Other formats require materializing the column and parsing client-side.");
                }
            }

            // 3-arg string.Replace(old, new, StringComparison) -- SQLite
            // REPLACE is case-sensitive. Honour the case-sensitive modes
            // (Ordinal/CurrentCulture/InvariantCulture) by emitting plain
            // REPLACE; ignore-case modes require a substring-detection
            // scheme SQLite doesn't have natively -- surface that as an
            // explicit unsupported-feature rather than silently emitting
            // a wrong-answer case-sensitive REPLACE.
            if (declType == typeof(string)
                && node.Method.Name == nameof(string.Replace)
                && node.Object != null
                && args.Length == 4   // receiver + old + new + comparison
                && node.Arguments.Count == 3
                && node.Arguments[2].Type == typeof(StringComparison)
                && node.Arguments[2] is System.Linq.Expressions.ConstantExpression repCmpArg
                && repCmpArg.Value is StringComparison repCmp)
            {
                bool ignoreCase = repCmp is StringComparison.OrdinalIgnoreCase
                    or StringComparison.CurrentCultureIgnoreCase
                    or StringComparison.InvariantCultureIgnoreCase;
                if (ignoreCase)
                {
                    throw new NormUnsupportedFeatureException(
                        "string.Replace(old, new, StringComparison) with an IgnoreCase mode is not supported -- " +
                        "SQLite REPLACE is case-sensitive and there's no portable case-insensitive substring " +
                        "rewrite. Use a case-sensitive mode or post-materialize the column and Replace client-side.");
                }
                return $"REPLACE({args[0]}, {args[1]}, {args[2]})";
            }

            // 2-arg string.IndexOf with a StringComparison enum tail arg.
            // SQLite INSTR is BINARY by default; lower both sides for an
            // ignore-case variant. INSTR returns 1-based or 0-when-missing;
            // subtract 1 for .NET's 0-based-or--1 contract. LastIndexOf is
            // intentionally NOT handled here -- SQLite has no last-occurrence
            // primitive and INSTR's first-occurrence position would be a
            // silently-wrong answer for a LastIndexOf caller.
            if (declType == typeof(string)
                && node.Method.Name == nameof(string.IndexOf)
                && node.Object != null
                && args.Length == 3   // receiver + needle + comparison
                && node.Arguments.Count == 2
                && node.Arguments[1].Type == typeof(StringComparison)
                && node.Arguments[1] is System.Linq.Expressions.ConstantExpression idxCmpArg
                && idxCmpArg.Value is StringComparison idxCmp)
            {
                bool ignoreCase = idxCmp is StringComparison.OrdinalIgnoreCase
                    or StringComparison.CurrentCultureIgnoreCase
                    or StringComparison.InvariantCultureIgnoreCase;
                var hay = ignoreCase ? $"LOWER({args[0]})" : args[0];
                var needle = ignoreCase ? $"LOWER({args[1]})" : args[1];
                return $"(INSTR({hay}, {needle}) - 1)";
            }

            // 3-arg string.Compare/CompareTo with a StringComparison enum tail
            // arg. SQLite's BINARY collation matches Ordinal (byte-wise); NOCASE
            // matches OrdinalIgnoreCase well for ASCII data. The 2-arg overload
            // already lives in the typeof(string) switch.
            if (declType == typeof(string)
                && (node.Method.Name == nameof(string.Compare) || node.Method.Name == nameof(string.CompareTo))
                && args.Length == 3
                && node.Arguments[node.Arguments.Count - 1] is System.Linq.Expressions.ConstantExpression cmpArg
                && cmpArg.Value is StringComparison cmp)
            {
                string collation = cmp switch
                {
                    StringComparison.OrdinalIgnoreCase
                        or StringComparison.CurrentCultureIgnoreCase
                        or StringComparison.InvariantCultureIgnoreCase => "NOCASE",
                    _ => "BINARY"
                };
                // The instance-form CompareTo arrives with receiver as args[0],
                // peer as args[1], mode arg as args[2]. Compare static comes
                // through identically since node.Object is null and three
                // expression args present.
                return $"(CASE WHEN {args[0]} COLLATE {collation} < {args[1]} COLLATE {collation} THEN -1 " +
                       $"WHEN {args[0]} COLLATE {collation} > {args[1]} COLLATE {collation} THEN 1 ELSE 0 END)";
            }

            // Math.Round and decimal.Round share identical overload semantics
            // and identical .NET defaults (ToEven). Treat them uniformly.
            if (!((declType == typeof(Math) && node.Method.Name == nameof(Math.Round))
                  || (declType == typeof(decimal) && node.Method.Name == nameof(decimal.Round))))
                return null;

            var ps = node.Method.GetParameters();
            // Default is ToEven; AwayFromZero matches SQLite's native ROUND.
            MidpointRounding mode = MidpointRounding.ToEven;
            string? digitsArg = null;
            if (ps.Length == 2 && ps[1].ParameterType == typeof(MidpointRounding))
            {
                if (node.Arguments[1] is System.Linq.Expressions.ConstantExpression c1 && c1.Value is MidpointRounding m1) mode = m1;
            }
            else if (ps.Length == 2)
            {
                digitsArg = args[1];
            }
            else if (ps.Length == 3)
            {
                digitsArg = args[1];
                if (node.Arguments[2] is System.Linq.Expressions.ConstantExpression c2 && c2.Value is MidpointRounding m2) mode = m2;
            }

            var x = args[0];
            // Scaled value for digit-aware rounding: round(x * 10^n) / 10^n.
            // No POW10 in pure-SQL constants; use POW(10.0, n).
            string scaled = digitsArg == null ? x : $"({x} * POW(10.0, {digitsArg}))";
            string unscale(string s) => digitsArg == null ? s : $"({s} / POW(10.0, {digitsArg}))";

            string roundCore;
            switch (mode)
            {
                case MidpointRounding.AwayFromZero:
                    // SQLite's native ROUND is already AwayFromZero.
                    roundCore = digitsArg == null ? $"ROUND({x})" : $"ROUND({x}, {digitsArg})";
                    return roundCore;
                case MidpointRounding.ToZero:
                    // Truncate toward zero -- CAST to INTEGER drops the fraction.
                    roundCore = $"(CASE WHEN {scaled} >= 0 THEN 1 ELSE -1 END * CAST(ABS({scaled}) AS INTEGER))";
                    return unscale(roundCore);
                case MidpointRounding.ToNegativeInfinity:
                    roundCore = $"FLOOR({scaled})";
                    return unscale(roundCore);
                case MidpointRounding.ToPositiveInfinity:
                    roundCore = $"CEIL({scaled})";
                    return unscale(roundCore);
                case MidpointRounding.ToEven:
                default:
                    // Banker's rounding on |scaled|: integer part via CAST (truncates
                    // toward zero, so on a non-negative value that's FLOOR); fractional
                    // part > 0.5 -> +1, < 0.5 -> +0, == 0.5 -> add 1 only when the
                    // integer part is odd. Sign reapplied via the leading CASE.
                    roundCore =
                        $"((CASE WHEN {scaled} >= 0 THEN 1 ELSE -1 END) * " +
                        $"(CAST(ABS({scaled}) AS INTEGER) + " +
                        $"CASE " +
                        $"WHEN ABS({scaled}) - CAST(ABS({scaled}) AS INTEGER) > 0.5 THEN 1 " +
                        $"WHEN ABS({scaled}) - CAST(ABS({scaled}) AS INTEGER) < 0.5 THEN 0 " +
                        $"ELSE (CAST(ABS({scaled}) AS INTEGER) % 2) END))";
                    return unscale(roundCore);
            }
        }

        private static string NewGuidSql()
            => "(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))))";

        /// <summary>
        /// Attempts to translate a .NET method into its SQLite SQL equivalent.
        /// </summary>
        /// <param name="name">Name of the method being translated.</param>
        /// <param name="declaringType">Type that declares the method.</param>
        /// <param name="args">SQL fragments representing the arguments.</param>
        /// <returns>The translated SQL or <c>null</c> if unsupported.</returns>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            // Instance CompareTo on integer primitives (int/long/short/byte/sbyte/uint
            // /ulong/ushort) -- arrives with the receiver's primitive type as
            // declaringType. Same sign-based emit as decimal/double's switch entries.
            if (name == "CompareTo" && args.Length == 2
                && (declaringType == typeof(int) || declaringType == typeof(long)
                    || declaringType == typeof(short) || declaringType == typeof(byte)
                    || declaringType == typeof(sbyte) || declaringType == typeof(uint)
                    || declaringType == typeof(ulong) || declaringType == typeof(ushort)
                    || declaringType == typeof(double) || declaringType == typeof(float)))
            {
                return $"CAST(SIGN({args[0]} - {args[1]}) AS INTEGER)";
            }
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    // String indexer s[i] compiles to String.get_Chars(i). Mirror
                    // ExpressionToSqlVisitor's lowering to SUBSTR(s, i+1, 1) so the
                    // projection path -- which routes through TranslateFunction --
                    // gets the same one-char extraction the Where path gets.
                    "get_Chars" when args.Length == 2 => $"SUBSTR({args[0]}, ({args[1]}) + 1, 1)",
                    // Static IsNullOrEmpty / IsNullOrWhiteSpace. Mirror
                    // ExpressionToSqlVisitor's inline emission (~line 1166) so the
                    // projection path matches the Where path. Without this, SCV
                    // falls through to its generic function-name handler and emits
                    // raw "ISNULLOREMPTY(...)" -- a SQLite 'no such function' error.
                    nameof(string.IsNullOrEmpty) when args.Length == 1 => $"({args[0]} IS NULL OR {args[0]} = '')",
                    nameof(string.IsNullOrWhiteSpace) when args.Length == 1 => $"({args[0]} IS NULL OR LTRIM(RTRIM({args[0]})) = '')",
                    // StartsWith / EndsWith / Contains in projection -- mirror the Where
                    // path's simple-literal LIKE shape. The pattern arg is already a
                    // bound parameter or quoted literal, so concat with %-wildcards via
                    // SQLite's || operator. Wildcard-in-pattern escape (the GetLikeEscapeSql
                    // path the Where handler uses for variable patterns) is not duplicated
                    // here -- the projection translates the user-visible "does this row
                    // contain X" shape and matches the most-common 'literal substring' use.
                    // string.Concat static with 2+ args -- chain via SQLite's || operator.
                    // Mirror of ExpressionToSqlVisitor's ~line 1333 inline path so SCV
                    // doesn't fall through to its lambda-expecting Queryable fallback
                    // (which crashes with "Expected a lambda expression as argument 1").
                    nameof(string.Concat) when args.Length >= 2 => "(" + string.Join(" || ", args) + ")",
                    nameof(string.StartsWith) when args.Length == 2 => $"({args[0]} LIKE {args[1]} || '%')",
                    nameof(string.EndsWith) when args.Length == 2 => $"({args[0]} LIKE '%' || {args[1]})",
                    nameof(string.Contains) when args.Length == 2 => $"({args[0]} LIKE '%' || {args[1]} || '%')",
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.ToUpperInvariant) => $"UPPER({args[0]})",
                    nameof(string.ToLowerInvariant) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"LENGTH({args[0]})",
                    nameof(string.Trim) when args.Length == 1 => $"TRIM({args[0]})",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // SQLite SUBSTR is 1-indexed; .NET Substring is 0-indexed, so add 1 to the start.
                    nameof(string.Substring) when args.Length == 2 => $"SUBSTR({args[0]}, {args[1]} + 1)",
                    nameof(string.Substring) when args.Length == 3 => $"SUBSTR({args[0]}, {args[1]} + 1, {args[2]})",
                    nameof(string.Replace) when args.Length == 3 => $"REPLACE({args[0]}, {args[1]}, {args[2]})",
                    // PadLeft/PadRight: SQLite has no REPLICATE, so the classic
                    // hex(zeroblob(n)) + REPLACE idiom is the portable way to build
                    // N copies of a single char. zeroblob(k) creates k null bytes,
                    // hex() renders them as 2k hex chars (k copies of '00'); REPLACE
                    // swaps '00' for the desired fill char to get k copies. The CASE
                    // returns the input unchanged when length(col) >= width, matching
                    // .NET semantics (PadLeft never truncates).
                    nameof(string.PadLeft) when args.Length == 2 => $"(CASE WHEN length({args[0]}) >= {args[1]} THEN {args[0]} ELSE replace(hex(zeroblob({args[1]} - length({args[0]}))), '00', ' ') || {args[0]} END)",
                    nameof(string.PadLeft) when args.Length == 3 => $"(CASE WHEN length({args[0]}) >= {args[1]} THEN {args[0]} ELSE replace(hex(zeroblob({args[1]} - length({args[0]}))), '00', {args[2]}) || {args[0]} END)",
                    nameof(string.PadRight) when args.Length == 2 => $"(CASE WHEN length({args[0]}) >= {args[1]} THEN {args[0]} ELSE {args[0]} || replace(hex(zeroblob({args[1]} - length({args[0]}))), '00', ' ') END)",
                    nameof(string.PadRight) when args.Length == 3 => $"(CASE WHEN length({args[0]}) >= {args[1]} THEN {args[0]} ELSE {args[0]} || replace(hex(zeroblob({args[1]} - length({args[0]}))), '00', {args[2]}) END)",
                    // SQLite INSTR returns 1-based position or 0 if not found; .NET IndexOf returns
                    // 0-based position or -1, so subtract 1 unconditionally.
                    nameof(string.IndexOf) when args.Length == 2 => $"(INSTR({args[0]}, {args[1]}) - 1)",
                    // Compare(a, b) -- .NET only guarantees the sign of the
                    // result, so emit a CASE producing -1/0/1. SQLite's < > =
                    // on TEXT use BINARY collation by default which matches
                    // the ordinal comparison most callers expect.
                    nameof(string.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    // Instance form: receiver passes through as args[0] from
                    // node.Object, peer as args[1]. Identical emit to static
                    // Compare; same .NET sign-only contract.
                    nameof(string.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
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
                    nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', {args[0]}, (({args[1]}) / 1000.0) || ' seconds'), '0'), '.')",
                    // AddTicks is the finest-grained Add* (1 tick = 100ns). Same
                    // strftime + RTRIM trim shape as AddMilliseconds; the divisor
                    // is 1e7 (10_000_000 ticks per second). SQLite's modifier
                    // syntax accepts the fractional value directly so very small
                    // tick deltas (e.g. 7500 ticks = 0.00075 seconds) get applied
                    // correctly while still trimming the trailing-zero / dot to
                    // match Microsoft.Data.Sqlite's FFFFFFF DateTime binding.
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

            if (declaringType == typeof(DateOnly))
            {
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

            if (declaringType == typeof(TimeSpan))
            {
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

            if (declaringType == typeof(Enum))
            {
                // Mirror ExpressionToSqlVisitor's inline HasFlag emission so the
                // projection path matches the Where path. enumCol.HasFlag(flag)
                // -> (col & flag) = flag works for any [Flags] enum with non-
                // overlapping bit values; multi-bit flag arguments require ALL
                // bits set (the canonical .NET semantic).
                return name switch
                {
                    nameof(Enum.HasFlag) when args.Length == 2 => $"(({args[0]} & {args[1]}) = {args[1]})",
                    _ => null
                };
            }

            if (declaringType == typeof(char))
            {
                // Mirror ExpressionToSqlVisitor's BETWEEN-style emission for the
                // common char.IsX validators so projection (which routes through
                // TranslateFunction) gets identical SQL to the Where path. Without
                // this branch, SelectClauseVisitor falls through to its generic
                // function-name handler and emits raw "ISDIGIT(...)" -- a SQLite
                // 'no such function' error. ASCII-only ranges match the Where
                // implementation note (no portable Unicode L*).
                return name switch
                {
                    nameof(char.IsDigit) when args.Length == 1 => $"({args[0]} BETWEEN '0' AND '9')",
                    nameof(char.IsLetter) when args.Length == 1 => $"(({args[0]} BETWEEN 'A' AND 'Z') OR ({args[0]} BETWEEN 'a' AND 'z'))",
                    nameof(char.IsWhiteSpace) when args.Length == 1 => $"({args[0]} = ' ' OR {args[0]} = CHAR(9) OR {args[0]} = CHAR(10) OR {args[0]} = CHAR(13))",
                    // SQLite UPPER / LOWER work on single-char text the same way
                    // they work on strings, so the static char form maps cleanly.
                    // Invariant overloads share the same emit on SQLite because
                    // UPPER/LOWER are already ASCII-only (no locale awareness).
                    nameof(char.ToUpper) when args.Length == 1 => $"UPPER({args[0]})",
                    nameof(char.ToLower) when args.Length == 1 => $"LOWER({args[0]})",
                    nameof(char.ToUpperInvariant) when args.Length == 1 => $"UPPER({args[0]})",
                    nameof(char.ToLowerInvariant) when args.Length == 1 => $"LOWER({args[0]})",
                    // ASCII-range predicates matching the existing IsDigit/IsLetter shape.
                    nameof(char.IsUpper) when args.Length == 1 => $"({args[0]} BETWEEN 'A' AND 'Z')",
                    nameof(char.IsLower) when args.Length == 1 => $"({args[0]} BETWEEN 'a' AND 'z')",
                    // ASCII punctuation per .NET char.IsPunctuation: codepoints
                    // 33-35, 37-42, 44-47, 58-59, 63-64, 91-93, 95, 123, 125.
                    // (! " # / % & ' ( ) * / , - . / / : ; / ? @ / [ \ ] / _ /
                    //  { } -- excludes $, +, <, =, >, |, ~, ^, ` which .NET
                    // classifies as Symbols.) Sub-ASCII only; full Unicode P*
                    // category is not portable.
                    nameof(char.IsPunctuation) when args.Length == 1 =>
                        $"((unicode({args[0]}) BETWEEN 33 AND 35) OR " +
                        $"(unicode({args[0]}) BETWEEN 37 AND 42) OR " +
                        $"(unicode({args[0]}) BETWEEN 44 AND 47) OR " +
                        $"(unicode({args[0]}) BETWEEN 58 AND 59) OR " +
                        $"(unicode({args[0]}) BETWEEN 63 AND 64) OR " +
                        $"(unicode({args[0]}) BETWEEN 91 AND 93) OR " +
                        $"unicode({args[0]}) = 95 OR " +
                        $"unicode({args[0]}) = 123 OR " +
                        $"unicode({args[0]}) = 125)",
                    // ASCII symbols per .NET char.IsSymbol: $, +, <, =, >, ^,
                    // `, |, ~. Distinct from Punctuation (5bb7520).
                    nameof(char.IsSymbol) when args.Length == 1 =>
                        $"(unicode({args[0]}) = 36 OR " +
                        $"unicode({args[0]}) = 43 OR " +
                        $"(unicode({args[0]}) BETWEEN 60 AND 62) OR " +
                        $"unicode({args[0]}) = 94 OR " +
                        $"unicode({args[0]}) = 96 OR " +
                        $"unicode({args[0]}) = 124 OR " +
                        $"unicode({args[0]}) = 126)",
                    // ASCII control chars: codepoints 0-31 plus 127 (DEL).
                    nameof(char.IsControl) when args.Length == 1 =>
                        $"((unicode({args[0]}) BETWEEN 0 AND 31) OR unicode({args[0]}) = 127)",
                    // char.GetNumericValue: digit value (0..9) for '0'..'9',
                    // -1.0 otherwise. Cast result to REAL to match the double
                    // return type the materializer expects.
                    nameof(char.GetNumericValue) when args.Length == 1 =>
                        $"(CASE WHEN unicode({args[0]}) BETWEEN 48 AND 57 " +
                        $"THEN CAST(unicode({args[0]}) - 48 AS REAL) ELSE -1.0 END)",
                    _ => null
                };
            }

            if (declaringType == typeof(TimeOnly))
            {
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

            // Numeric Parse(string) -- common pattern where numeric values
            // are stored in a TEXT column and need integer/decimal semantics
            // for projection or downstream arithmetic. SQLite CAST AS INTEGER
            // / REAL handles the text->number conversion natively (returns 0
            // for non-numeric text, matching SQLite's coercion -- not .NET's
            // FormatException semantic but the closest SQL equivalent).
            if (declaringType == typeof(int)
                || declaringType == typeof(long)
                || declaringType == typeof(short)
                || declaringType == typeof(byte)
                || declaringType == typeof(double)
                || declaringType == typeof(float)
                || declaringType == typeof(decimal))
            {
                if (name == "Parse" && args.Length == 1)
                {
                    var sqlType = declaringType == typeof(double) || declaringType == typeof(float) || declaringType == typeof(decimal)
                        ? "REAL"
                        : "INTEGER";
                    return $"CAST({args[0]} AS {sqlType})";
                }
            }

            // bool.Parse(string) -- .NET semantics are case-insensitive
            // ("True"/"true"/"TRUE" -> true; "False"/"false"/"FALSE" ->
            // false). SQLite returns 0/1 INTEGER from a boolean expression
            // which the materializer converts to bool via the column type.
            if (declaringType == typeof(bool) && name == "Parse" && args.Length == 1)
            {
                return $"(LOWER({args[0]}) = 'true')";
            }

            // Guid.Parse(string) -- SQLite stores Guid as canonical
            // 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' text and Microsoft.Data
            // .Sqlite's GetGuid parses it directly. Identity emission.
            if (declaringType == typeof(Guid) && name == "Parse" && args.Length == 1)
            {
                return args[0];
            }

            if (declaringType == typeof(Guid) && name == nameof(Guid.NewGuid) && args.Length == 0)
            {
                return NewGuidSql();
            }


            if (declaringType == typeof(NormFunctions))
            {
                return name switch
                {
                    // SQLite LIKE is case-insensitive for ASCII by default. Force the
                    // case-fold explicitly so callers can rely on consistent semantics
                    // when collations or PRAGMA case_sensitive_like change.
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
                    // Server-side primitives. SQLite datetime('now') returns
                    // 'YYYY-MM-DD HH:MM:SS' in UTC; randomblob(16) returns a
                    // 16-byte blob compatible with .NET Guid via the
                    // GetFieldValue<Guid>() reader path; random()/9.22e18
                    // squeezes the 64-bit signed result into [0, 1).
                    nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "datetime('now')",
                    nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => NewGuidSql(),
                    nameof(NormFunctions.ServerRandom) when args.Length == 0 => "(ABS(random()) / 9223372036854775808.0)",
                    _ => null
                };
            }

            // IEEE 754 predicates on double/float. Same emit shape for all
            // numeric receivers so we match by name+arity regardless of which
            // primitive type owns the static. Notes on the SQL idioms:
            //   IsNaN(x):      (x != x)               -- NaN is the only IEEE
            //                                            value not equal to itself.
            //   IsInfinity(x): (ABS(x) = 1e999)       -- ABS strips sign; the
            //                                            literal 1e999 parses to +Inf.
            //   IsFinite(x):   (x = x AND ABS(x) != 1e999) -- not NaN AND not +/-Inf.
            //   IsNegativeInfinity(x): (x = -1e999)
            //   IsPositiveInfinity(x): (x =  1e999)
            if ((declaringType == typeof(double) || declaringType == typeof(float))
                && args.Length == 1)
            {
                switch (name)
                {
                    case "IsNaN": return $"({args[0]} != {args[0]})";
                    case "IsInfinity": return $"(ABS({args[0]}) = 1e999)";
                    case "IsFinite": return $"({args[0]} = {args[0]} AND ABS({args[0]}) != 1e999)";
                    case "IsNegativeInfinity": return $"({args[0]} = -1e999)";
                    case "IsPositiveInfinity": return $"({args[0]} = 1e999)";
                    // IsNormal: finite, non-zero, |x| >= smallest normal positive double.
                    // The IEEE 754 boundary is 2^-1022 = 2.2250738585072014E-308.
                    case "IsNormal":
                        return $"({args[0]} = {args[0]} AND ABS({args[0]}) != 1e999 " +
                               $"AND {args[0]} != 0 AND ABS({args[0]}) >= 2.2250738585072014E-308)";
                    // IsSubnormal: non-zero AND |x| < min normal positive.
                    case "IsSubnormal":
                        return $"({args[0]} != 0 AND ABS({args[0]}) < 2.2250738585072014E-308)";
                }
            }

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEIL({args[0]})",
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
                    // SQLite has no TRUNC; CAST drops the fractional part for finite reals and
                    // matches Math.Truncate semantics (truncate toward zero).
                    nameof(Math.Truncate) when args.Length == 1 => $"CAST({args[0]} AS INTEGER)",
                    // Force numeric comparison via CAST AS REAL -- otherwise
                    // TEXT-stored decimal columns get lex-compared and MIN(
                    // '10.0', '5.5') wrongly returns '10.0' (since '1' < '5').
                    // CAST(int AS REAL) is identity-with-decimal-zero so
                    // integer pairings still round-trip correctly.
                    nameof(Math.Min) when args.Length == 2 => $"MIN(CAST({args[0]} AS REAL), CAST({args[1]} AS REAL))",
                    nameof(Math.Max) when args.Length == 2 => $"MAX(CAST({args[0]} AS REAL), CAST({args[1]} AS REAL))",
                    // SQLite 3.35+ exposes log2() and pow() as built-ins via the
                    // math extension. Cbrt has no direct function -- use pow(x, 1/3)
                    // which matches Math.Cbrt for non-negative reals (the .NET
                    // double overload returns a real-valued root for negatives too,
                    // but POW returns NaN for x<0 with a fractional exponent --
                    // documented limitation, mirrors Math.Pow behaviour).
                    nameof(Math.Log2) when args.Length == 1 => $"LOG2({args[0]})",
                    nameof(Math.Cbrt) when args.Length == 1 => $"POW({args[0]}, 1.0/3.0)",
                    // Basic trig + inverse trig from SQLite 3.35+ math extension.
                    // Direct one-to-one mappings; all return double.
                    nameof(Math.Sin) when args.Length == 1 => $"SIN({args[0]})",
                    nameof(Math.Cos) when args.Length == 1 => $"COS({args[0]})",
                    nameof(Math.Tan) when args.Length == 1 => $"TAN({args[0]})",
                    nameof(Math.Asin) when args.Length == 1 => $"ASIN({args[0]})",
                    nameof(Math.Acos) when args.Length == 1 => $"ACOS({args[0]})",
                    nameof(Math.Atan) when args.Length == 1 => $"ATAN({args[0]})",
                    // Hyperbolic + 2-arg trig from SQLite 3.35+ math extension.
                    // Direct one-to-one mappings.
                    nameof(Math.Sinh) when args.Length == 1 => $"SINH({args[0]})",
                    nameof(Math.Cosh) when args.Length == 1 => $"COSH({args[0]})",
                    nameof(Math.Tanh) when args.Length == 1 => $"TANH({args[0]})",
                    nameof(Math.Atan2) when args.Length == 2 => $"ATAN2({args[0]}, {args[1]})",
                    // Inverse hyperbolic -- SQLite math extension built-ins.
                    nameof(Math.Asinh) when args.Length == 1 => $"ASINH({args[0]})",
                    nameof(Math.Acosh) when args.Length == 1 => $"ACOSH({args[0]})",
                    nameof(Math.Atanh) when args.Length == 1 => $"ATANH({args[0]})",
                    // MaxMagnitude/MinMagnitude -- whichever argument has the
                    // larger/smaller absolute value. For non-equal magnitudes
                    // a simple CASE on ABS matches .NET; the equal-magnitude
                    // IEEE 754 tie-break (Max favors +, Min favors -) is
                    // documented as out-of-scope -- real column data rarely
                    // hits exact-magnitude ties.
                    nameof(Math.MaxMagnitude) when args.Length == 2 => $"CASE WHEN ABS({args[0]}) >= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.MinMagnitude) when args.Length == 2 => $"CASE WHEN ABS({args[0]}) <= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    // ScaleB(x, n) = x * 2^n -- direct via SQLite POW.
                    nameof(Math.ScaleB) when args.Length == 2 => $"({args[0]} * POW(2.0, {args[1]}))",
                    // CopySign(x, y): returns x with the sign of y. ABS(x) * SIGN(y)
                    // is portable but loses the sign-of-zero distinction (SIGN(0) = 0,
                    // so CopySign(x, 0) emits 0 instead of x); acceptable since real
                    // column values rarely encounter signed zero.
                    nameof(Math.CopySign) when args.Length == 2 => $"(ABS({args[0]}) * SIGN({args[1]}))",
                    // BigMul(int, int) widens to long. SQLite INTEGER is 64-bit
                    // so the natural product never overflows for the int*int
                    // range -- emit the plain multiply; materializer reads the
                    // INTEGER column as long via column-type affinity.
                    nameof(Math.BigMul) when args.Length == 2 => $"({args[0]} * {args[1]})",
                    // Clamp(v, min, max) = MIN(MAX(v, min), max). The .NET
                    // ArgumentException for min > max is a runtime guard, not
                    // part of the expression semantics; callers writing
                    // Clamp(...) in a query already implicitly assume the
                    // ordering, so we mirror only the happy path.
                    nameof(Math.Clamp) when args.Length == 3 => $"MIN(MAX({args[0]}, {args[1]}), {args[2]})",
                    // IEEERemainder(x, y) = x - y * round(x/y, ToEven). SQLite's
                    // native ROUND() rounds half-away-from-zero, so we inline a
                    // banker's-rounding equivalent on |x/y|: integer part via
                    // CAST(... AS INTEGER) (truncates toward zero, so on a
                    // non-negative value that's FLOOR); fractional part > 0.5 -> +1,
                    // < 0.5 -> +0, == 0.5 -> add 1 only when the integer part is
                    // odd (i.e. round to even). Sign reapplied via the leading
                    // CASE, since the rounding is sign-symmetric.
                    nameof(Math.IEEERemainder) when args.Length == 2 =>
                        $"({args[0]} - {args[1]} * ((CASE WHEN ({args[0]})/({args[1]}) >= 0 THEN 1 ELSE -1 END) * " +
                        $"(CAST(ABS(({args[0]})/({args[1]})) AS INTEGER) + " +
                        $"CASE " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS INTEGER) > 0.5 THEN 1 " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS INTEGER) < 0.5 THEN 0 " +
                        $"ELSE (CAST(ABS(({args[0]})/({args[1]})) AS INTEGER) % 2) END)))",
                    _ => null
                };
            }

            // decimal static math: direct mirrors of Math.* equivalents.
            // SQLite REAL handles the underlying arithmetic; the materializer
            // converts the result back to decimal via column-type affinity.
            if (declaringType == typeof(decimal))
            {
                return name switch
                {
                    nameof(decimal.Truncate) when args.Length == 1 => $"CAST({args[0]} AS INTEGER)",
                    nameof(decimal.Floor) when args.Length == 1 => $"FLOOR({args[0]})",
                    nameof(decimal.Ceiling) when args.Length == 1 => $"CEIL({args[0]})",
                    nameof(decimal.Abs) when args.Length == 1 => $"ABS({args[0]})",
                    // Static method-form arithmetic -- equivalent to the
                    // operator-form (which already routes through binary
                    // expression nodes). Generated code sometimes emits the
                    // static form via the Expression API.
                    nameof(decimal.Add) when args.Length == 2 => $"({args[0]} + {args[1]})",
                    nameof(decimal.Subtract) when args.Length == 2 => $"({args[0]} - {args[1]})",
                    nameof(decimal.Multiply) when args.Length == 2 => $"({args[0]} * {args[1]})",
                    nameof(decimal.Divide) when args.Length == 2 => $"({args[0]} * 1.0 / {args[1]})",
                    nameof(decimal.Remainder) when args.Length == 2 => $"({args[0]} % {args[1]})",
                    nameof(decimal.Negate) when args.Length == 1 => $"(-({args[0]}))",
                    // Compare(a, b) returns -1/0/1 indicating less/equal/greater.
                    // SQLite's SIGN(a-b) yields exactly that triple for non-NaN
                    // numerics; sister to Math.Sign already mapped.
                    nameof(decimal.Compare) when args.Length == 2 => $"CAST(SIGN({args[0]} - {args[1]}) AS INTEGER)",
                    // CompareTo instance form -- same shape as static Compare,
                    // covers int/long/double/decimal numeric receivers since
                    // the name string is the same and arity matches.
                    nameof(decimal.CompareTo) when args.Length == 2 => $"CAST(SIGN({args[0]} - {args[1]}) AS INTEGER)",
                    _ => null
                };
            }

            if (declaringType == typeof(Convert))
            {
                // Convert.ToXyz overloads from-string are the canonical sister
                // of int.Parse / bool.Parse and emit identical SQL. The from-
                // string shape is what tests pin here; the from-numeric
                // overloads (ToInt32(double), etc.) would also be valid but
                // require more careful semantics around rounding (Convert
                // .ToInt32(double) rounds-to-even while CAST truncates).
                return name switch
                {
                    "ToInt32" when args.Length == 1 => $"CAST({args[0]} AS INTEGER)",
                    "ToInt64" when args.Length == 1 => $"CAST({args[0]} AS INTEGER)",
                    "ToDouble" when args.Length == 1 => $"CAST({args[0]} AS REAL)",
                    "ToDecimal" when args.Length == 1 => $"CAST({args[0]} AS REAL)",
                    // Convert.ToBoolean(string) -- .NET semantics are case-
                    // insensitive plus tolerant of surrounding whitespace.
                    // Mirror bool.Parse emission with a TRIM wrap.
                    "ToBoolean" when args.Length == 1 => $"(LOWER(TRIM({args[0]})) = 'true')",
                    _ => null
                };
            }

            return null;
        }
    }
}
