using System;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {
        private static string? TryTranslateStringFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(string))
            {
                return null;
            }

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
                // string.Concat static with 2+ args -- chain via SQLite's || operator.
                // Mirror of ExpressionToSqlVisitor's ~line 1333 inline path so SCV
                // doesn't fall through to its lambda-expecting Queryable fallback
                // (which crashes with "Expected a lambda expression as argument 1").
                nameof(string.Concat) when args.Length >= 2 => "(" + string.Join(" || ", args) + ")",
                // StartsWith / EndsWith / Contains in projection -- SQLite's LIKE folds ASCII
                // case regardless of collation, so mirror the Where path's byte-exact
                // instr/substr forms (ordinal, matching .NET). These also need no wildcard
                // escaping: the pattern text matches literally.
                nameof(string.StartsWith) when args.Length == 2 => OrdinalStringMatchCore(args[0], args[1], OrdinalStringMatch.StartsWith),
                nameof(string.EndsWith) when args.Length == 2 => OrdinalStringMatchCore(args[0], args[1], OrdinalStringMatch.EndsWith),
                nameof(string.Contains) when args.Length == 2 => OrdinalStringMatchCore(args[0], args[1], OrdinalStringMatch.Contains),
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
    }
}
