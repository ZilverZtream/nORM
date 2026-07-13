using System;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {
        private static string? TryTranslatePrimitiveCompareFunction(string name, Type declaringType, string[] args)
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

            return null;
        }

        private static string? TryTranslateIeee754Function(string name, Type declaringType, string[] args)
        {
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

            return null;
        }

        private static string? TryTranslateMathFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(Math))
            {
                return null;
            }

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

        private static string? TryTranslateDecimalFunction(string name, Type declaringType, string[] args)
        {
            // decimal static math: direct mirrors of Math.* equivalents.
            // SQLite REAL handles the underlying arithmetic; the materializer
            // converts the result back to decimal via column-type affinity.
            if (declaringType != typeof(decimal))
            {
                return null;
            }

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

        private static string? TryTranslateConvertFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(Convert))
            {
                return null;
            }

            // Convert.ToXyz overloads from-string are the canonical sister of
            // int.Parse / bool.Parse. The integral converts fold through banker's
            // rounding first: .NET Convert.ToIntXX(double/decimal) rounds half to
            // even while CAST truncates, and the round is identity for values that
            // are already integral (from-string and from-int callers unaffected).
            return name switch
            {
                "ToInt32" when args.Length == 1 => $"CAST({BankersRoundIntegralSql(args[0], "INTEGER")} AS INTEGER)",
                "ToInt64" when args.Length == 1 => $"CAST({BankersRoundIntegralSql(args[0], "INTEGER")} AS INTEGER)",
                "ToDouble" when args.Length == 1 => $"CAST({args[0]} AS REAL)",
                "ToDecimal" when args.Length == 1 => $"CAST({args[0]} AS REAL)",
                // Convert.ToBoolean(string) -- .NET semantics are case-
                // insensitive plus tolerant of surrounding whitespace.
                // Mirror bool.Parse emission with a TRIM wrap.
                "ToBoolean" when args.Length == 1 => $"(LOWER(TRIM({args[0]})) = 'true')",
                _ => null
            };
        }
    }
}
