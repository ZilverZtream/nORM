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
    {
        /// <summary>
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
    }
}
