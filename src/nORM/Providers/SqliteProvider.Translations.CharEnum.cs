using System;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {
        private static string? TryTranslateEnumFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(Enum))
            {
                return null;
            }

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

        private static string? TryTranslateCharFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(char))
            {
                return null;
            }

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
    }
}
