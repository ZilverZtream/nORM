using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Lightweight provider implementation targeting SQLite databases.
    /// Provides SQL generation and initialization routines specific to SQLite's feature set.
    /// </summary>
    public class SqliteProvider : DatabaseProvider
    {
        /// <summary>
        /// SQLite has no true async I/O — all async methods are synchronous wrappers.
        /// Using sync calls eliminates ~50-100ns of async state machine overhead per Read().
        /// </summary>
        public override bool PrefersSyncExecution => true;

        /// <summary>
        /// Bare boolean predicates avoid steering SQLite toward low-selectivity boolean indexes
        /// when a more selective conjunct is available.
        /// </summary>
        public override bool PrefersBareBooleanPredicates => true;

        /// <inheritdoc />
        public override string FormatBooleanPredicate(string expressionSql, bool expectedValue)
            => expectedValue ? expressionSql : $"NOT ({expressionSql})";

        /// <summary>
        /// SQLite's <c>IS</c> operator provides null-safe equality with index support.
        /// <c>col IS @p</c> is equivalent to <c>col = @p OR (col IS NULL AND @p IS NULL)</c>
        /// but allows the query planner to use column indexes efficiently.
        /// </summary>
        public override string NullSafeEqual(string left, string right)
            => $"{left} IS {right}";

        /// <summary>
        /// SQLite's <c>IS NOT</c> operator for null-safe inequality with index support.
        /// </summary>
        public override string NullSafeNotEqual(string left, string right)
            => $"{left} IS NOT {right}";

        /// <summary>
        /// Maximum length of a single SQL statement supported by SQLite.
        /// </summary>
        public override int MaxSqlLength => 1_000_000;

        /// <summary>
        /// Maximum number of parameters allowed in a single SQLite command.
        /// </summary>
        public override int MaxParameters => 999;

        /// <summary>
        /// Minimum SQLite version supported by nORM v1. SQLite 3.25 is the lowest version where
        /// the migration generator's RENAME COLUMN, window functions, and UPSERT (ON CONFLICT)
        /// emit valid statements. Older SQLite builds would parse-fail on those, so v1 fails
        /// startup validation rather than producing later runtime errors. This floor matches
        /// docs/provider-capabilities.md and is enforced by ProviderCapabilityContractTests.
        /// </summary>
        internal static readonly Version MinimumSqliteVersion = new(3, 25);

        /// <inheritdoc />
        public override ProviderCapabilities Capabilities => new(
            "SQLite",
            MinimumSqliteVersion,
            MaxParameters,
            true,
            true,
            false,
            true,
            "Requires Microsoft.Data.Sqlite 3.25 or newer. JSON support depends on the SQLite JSON1 extension.");

        /// <summary>
        /// Escapes an identifier by wrapping it in double quotes, per SQLite requirements.
        /// Handles multi-part identifiers (schema.table / attached-db.table) by escaping each
        /// segment separately so that <c>"schema"."table"</c> is produced rather than the invalid
        /// <c>"schema.table"</c>.
        /// Embedded double-quote characters are doubled to prevent SQL injection via identifiers.
        /// </summary>
        /// <param name="id">Identifier to escape (e.g., <c>"table"</c> or <c>"schema.table"</c>).</param>
        /// <returns>The escaped identifier.</returns>
        public override string Escape(string id)
        {
            if (string.IsNullOrWhiteSpace(id)) return id;
            if (id.Contains('.'))
                return string.Join(".", id.Split('.').Select(part => $"\"{part.Replace("\"", "\"\"")}\""));
            return $"\"{id.Replace("\"", "\"\"")}\"";
        }


        /// <summary>
        /// Character used to prefix parameter names in SQLite commands.
        /// </summary>
        public override char ParameterPrefixChar => '@';

        /// <summary>
        /// SQLite does not support stored procedures; commands are always text.
        /// </summary>
        public override CommandType StoredProcedureCommandType => CommandType.Text;

        /// <summary>
        /// SQLite uses the <c>||</c> operator for string concatenation instead of CONCAT.
        /// </summary>
        public override string GetConcatSql(string left, string right) => $"({left} || {right})";

        /// <summary>SQLite TEXT affinity is the natural target for numeric/Guid/DateTime ToString().</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS TEXT)";

        /// <summary>SQLite has no XOR operator — synthesize via `(a | b) - (a &amp; b)`.</summary>
        public override string GetBitwiseXorSql(string left, string right) => $"(({left} | {right}) - ({left} & {right}))";

        /// <summary>SQLite REAL handles both float and decimal — no DOUBLE PRECISION / DECIMAL(p,s) keywords.</summary>
        public override string GetRealCastSql(string innerSql, bool asDecimal = false) => $"CAST({innerSql} AS REAL)";

        /// <summary>SQLite supports INSERT OR IGNORE for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT OR IGNORE INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2})";

        /// <summary>
        /// Builds a minimal <c>SELECT</c> statement directly into a character buffer.
        /// </summary>
        /// <param name="buffer">Buffer receiving the SQL.</param>
        /// <param name="table">Table name to query.</param>
        /// <param name="columns">Comma-separated column list.</param>
        /// <param name="length">Receives the length of the generated SQL.</param>
        public override void BuildSimpleSelect(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns, out int length)
        {
            "SELECT ".CopyTo(buffer);
            var pos = 7;
            columns.CopyTo(buffer.Slice(pos));
            pos += columns.Length;
            " FROM ".CopyTo(buffer.Slice(pos));
            pos += 6;
            table.CopyTo(buffer.Slice(pos));
            length = pos + table.Length;
        }

        /// <summary>
        /// Applies performance-related <c>PRAGMA</c> settings to the supplied SQLite connection asynchronously.
        /// </summary>
        /// <param name="connection">The connection to configure.</param>
        /// <param name="ct">Cancellation token.</param>
        public override async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        {
            await base.InitializeConnectionAsync(connection, ct).ConfigureAwait(false);
            await using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            await pragmaCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Applies performance-related <c>PRAGMA</c> settings to the SQLite connection.
        /// </summary>
        /// <param name="connection">The connection to configure.</param>
        public override void InitializeConnection(DbConnection connection)
        {
            base.InitializeConnection(connection);
            using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            pragmaCmd.ExecuteNonQuery();
        }
        
        /// <summary>
        /// Appends SQLite <c>LIMIT</c> and <c>OFFSET</c> clauses to the SQL builder.
        /// </summary>
        /// <param name="sb">Builder receiving the clauses.</param>
        /// <param name="limit">Maximum number of rows to return.</param>
        /// <param name="offset">Number of rows to skip.</param>
        /// <param name="limitParameterName">Name of the parameter supplying the limit.</param>
        /// <param name="offsetParameterName">Name of the parameter supplying the offset.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            // Inline literal LIMIT/OFFSET values directly in SQL when no parameter name is provided.
            // Parameterized LIMIT prevents SQLite's planner from using cardinality estimates.
            if (limitParameterName != null)
                sb.Append(" LIMIT ").Append(limitParameterName);
            else if (limit.HasValue)
                sb.Append(" LIMIT ").Append(limit.Value);
            else if (offsetParameterName != null || offset.HasValue)
                // SQLite requires LIMIT when OFFSET is used; -1 means unlimited
                sb.Append(" LIMIT -1");

            if (offsetParameterName != null)
                sb.Append(" OFFSET ").Append(offsetParameterName);
            else if (offset.HasValue)
                sb.Append(" OFFSET ").Append(offset.Value);
        }
        
        /// <summary>
        /// Returns SQL that retrieves the last rowid generated by an <c>INSERT</c>.
        /// </summary>
        /// <param name="m">The mapping for which the identity is retrieved.</param>
        /// <returns>SQL fragment to append to the insert command.</returns>
        public override string GetIdentityRetrievalString(TableMapping m)
        {
            // Use RETURNING clause (SQLite 3.35+) for single-statement identity retrieval.
            // This is faster than "; SELECT last_insert_rowid();" because SQLite
            // executes one statement instead of two (no separate query plan/parse step).
            var keyCol = m?.KeyColumns?.FirstOrDefault(c => c.IsDbGenerated);
            return keyCol != null ? $" RETURNING {keyCol.EscCol}" : "; SELECT last_insert_rowid();";
        }
        
        /// <summary>
        /// Creates a SQLite parameter with the given name and value.
        /// </summary>
        /// <param name="name">Parameter name including prefix.</param>
        /// <param name="value">Parameter value; <c>null</c> becomes <see cref="DBNull.Value"/>.</param>
        /// <returns>A configured <see cref="SqliteParameter"/>.</returns>
        public override DbParameter CreateParameter(string name, object? value)
        {
            return new SqliteParameter(name, value ?? DBNull.Value);
        }

        /// <summary>
        /// SQLite has no native interval type. Convert both timestamps to Julian-day numbers
        /// (a fractional REAL) and multiply by 86400 to get seconds.
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"((julianday({endSql}) - julianday({startSql})) * 86400.0)";

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
                // Microsoft.Data.Sqlite binds TimeSpan as canonical 'HH:mm:ss'
                // text (TimeSpan.ToString 'c' format) for sub-day spans. The
                // component getters are fixed-position string slices into that
                // text. Multi-day spans bind with a 'd.' prefix and would shift
                // these offsets -- documented as out-of-scope for v1; a future
                // iteration can detect/handle the prefix via INSTR.
                return name switch
                {
                    nameof(TimeSpan.Hours) => $"CAST(substr({args[0]}, 1, 2) AS INTEGER)",
                    nameof(TimeSpan.Minutes) => $"CAST(substr({args[0]}, 4, 2) AS INTEGER)",
                    nameof(TimeSpan.Seconds) => $"CAST(substr({args[0]}, 7, 2) AS INTEGER)",
                    // Total* return double, so divide by a REAL literal (60.0 / 3600.0)
                    // rather than INTEGER to force fractional arithmetic. Sum is integer
                    // seconds: H*3600 + M*60 + S.
                    nameof(TimeSpan.TotalSeconds) => $"(CAST(substr({args[0]}, 1, 2) AS INTEGER) * 3600 + CAST(substr({args[0]}, 4, 2) AS INTEGER) * 60 + CAST(substr({args[0]}, 7, 2) AS INTEGER))",
                    nameof(TimeSpan.TotalMinutes) => $"((CAST(substr({args[0]}, 1, 2) AS INTEGER) * 3600 + CAST(substr({args[0]}, 4, 2) AS INTEGER) * 60 + CAST(substr({args[0]}, 7, 2) AS INTEGER)) / 60.0)",
                    nameof(TimeSpan.TotalHours) => $"((CAST(substr({args[0]}, 1, 2) AS INTEGER) * 3600 + CAST(substr({args[0]}, 4, 2) AS INTEGER) * 60 + CAST(substr({args[0]}, 7, 2) AS INTEGER)) / 3600.0)",
                    // TotalMilliseconds needs the fractional-seconds component. The
                    // canonical 'c' format emits 7-digit ticks past position 9 when
                    // nonzero (TimeSpan keeps trailing zeros unlike DateTime's
                    // FFFFFFF). Each tick is 100ns, so the 7-digit value divided by
                    // 10000 gives milliseconds. The CASE guards the length-8
                    // 'HH:mm:ss' shape (no fractional present); without it substr
                    // returns an empty string which CASTs to 0 anyway but the CASE
                    // makes the intent explicit.
                    nameof(TimeSpan.TotalMilliseconds) => $"((CAST(substr({args[0]}, 1, 2) AS INTEGER) * 3600 + CAST(substr({args[0]}, 4, 2) AS INTEGER) * 60 + CAST(substr({args[0]}, 7, 2) AS INTEGER)) * 1000.0 + CASE WHEN length({args[0]}) > 9 THEN CAST(substr({args[0]}, 10) AS REAL) / 10000.0 ELSE 0 END)",
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
                    nameof(char.ToUpper) when args.Length == 1 => $"UPPER({args[0]})",
                    nameof(char.ToLower) when args.Length == 1 => $"LOWER({args[0]})",
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


            if (declaringType == typeof(NormFunctions))
            {
                return name switch
                {
                    // SQLite LIKE is case-insensitive for ASCII by default. Force the
                    // case-fold explicitly so callers can rely on consistent semantics
                    // when collations or PRAGMA case_sensitive_like change.
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
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

        /// <summary>
        /// Translates JSON value access using SQLite's <c>json_extract</c> function.
        /// </summary>
        /// <param name="columnName">JSON column to access.</param>
        /// <param name="jsonPath">JSON path expression.</param>
        /// <returns>SQL fragment retrieving the requested JSON value.</returns>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);
            return $"json_extract({columnName}, '{jsonPath}')";
        }

        /// <summary>
        /// SQLite table-not-found errors use SQLITE_ERROR (code 1) combined with
        /// the "no such table" message. Code 1 alone is too broad (also covers syntax errors).
        /// Other error codes (SQLITE_PERM=3, SQLITE_CANTOPEN=14, etc.) indicate operational failures
        /// that must NOT be silently treated as "table absent."
        /// </summary>
        public override bool IsObjectNotFoundError(DbException ex)
            => ex is SqliteException sqliteEx
               && sqliteEx.SqliteErrorCode == 1
               && ex.Message.Contains("no such table", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Introspects live column definitions via PRAGMA table_info.
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                // PRAGMA table_info columns: cid[0], name[1], type[2], notnull[3], dflt_value[4], pk[5]
                var bare = tableName.Trim('"');
                cmd.CommandText = $"PRAGMA table_info(\"{bare.Replace("\"", "\"\"")}\")";
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(1);
                    var sqlType = rdr.GetString(2);
                    var notNull = rdr.GetInt32(3) != 0;
                    result.Add(new LiveColumnInfo(name, string.IsNullOrEmpty(sqlType) ? "TEXT" : sqlType, !notNull));
                }
            }
            catch (DbException dbEx) when (IsObjectNotFoundError(dbEx))
            {
                // Table does not exist yet — return empty list so caller falls back to CLR defaults.
            }
            return result;
        }

        /// <summary>
        /// Generates SQL to create a history table. Column types use the same SQLite type
        /// mapping as the main table (GetSqliteType), ensuring the history schema mirrors
        /// the main table exactly (INTEGER for int/bool/long, REAL for decimal/double/float,
        /// BLOB for byte[], TEXT for strings and everything else).
        /// When liveColumns are supplied, live SQL types and nullability override CLR defaults.
        /// </summary>
        /// <param name="mapping">The entity mapping being tracked.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use CLR defaults.</param>
        /// <returns>DDL statement that creates the history table.</returns>
        public override string GenerateCreateHistoryTableSql(
            TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            var liveMap = liveColumns?
                .ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase)
                ?? new Dictionary<string, LiveColumnInfo>(0);

            var columns = string.Join(",\n                ", mapping.Columns.Select(c =>
            {
                if (liveMap.TryGetValue(c.Name, out var live))
                    return $"{Escape(c.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}";
                var sqlType = GetSqliteType(c.Prop.PropertyType);
                var nullability = IsNullableOrReferenceType(c.Prop.PropertyType) ? "" : " NOT NULL";
                return $"{Escape(c.Name)} {sqlType}{nullability}";
            }));
            return @$"CREATE TABLE IF NOT EXISTS {Escape(mapping.TableName + "_History")} (
                __VersionId INTEGER PRIMARY KEY AUTOINCREMENT,
                __ValidFrom TEXT NOT NULL,
                __ValidTo TEXT NOT NULL,
                __Operation TEXT NOT NULL,
                {columns}
            );";
        }

        /// <summary>
        /// Returns true when the type can hold a SQL NULL value (reference types and Nullable&lt;T&gt;).
        /// Used by <see cref="GenerateCreateHistoryTableSql"/> to decide NOT NULL constraints.
        /// </summary>
        private static bool IsNullableOrReferenceType(Type t) =>
            !t.IsValueType || (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>));

        /// <summary>
        /// Generates trigger definitions that maintain the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the source table.</param>
        /// <returns>DDL statements creating the triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columnList = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.Name)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.Name)));
            var keyCondition = mapping.KeyColumns.Length > 0
                ? string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.Name)} = OLD.{Escape(c.Name)}"))
                : "1=1";

            return @$"
CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
BEGIN
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), '9999-12-31', 'I', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = datetime('now') WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), '9999-12-31', 'U', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = datetime('now') WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), datetime('now'), 'D', {oldColumns});
END;";
        }

        /// <summary>
        /// Verifies that the provided connection is a <see cref="SqliteConnection"/>, as required
        /// by this provider.
        /// </summary>
        /// <param name="connection">The connection to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown if the connection is not compatible.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for SqliteProvider.");
        }

        /// <summary>
        /// Determines if the SQLite provider can be used in the current environment by
        /// verifying that the required <c>Microsoft.Data.Sqlite</c> assembly is available
        /// and that the SQLite engine meets the minimum version requirement.
        /// </summary>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Data Source=:memory:";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "select sqlite_version()";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr);
                return version >= MinimumSqliteVersion;
            }
            catch (DbException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        /// <inheritdoc />
        protected override async Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "select sqlite_version()";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "select sqlite_version()";
            return cmd.ExecuteScalar() as string;
        }

        /// <summary>
        /// Creates a savepoint within a SQLite transaction allowing partial rollbacks.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to create.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken — a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Save(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Rolls back a SQLite transaction to the specified savepoint.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken — a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Rollback(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Inserts a collection of entities using SQLite-optimized prepared statements in a single transaction.
        /// Uses prepared statement reuse and transaction batching; significantly faster than multiple-transaction approaches.
        /// </summary>
        /// <typeparam name="T">Type of entity being inserted.</typeparam>
        /// <param name="ctx">Current <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping for the destination table.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows inserted.</returns>
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var entityList = entities as ICollection<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;
            var sw = ctx.Options.Logger != null ? Stopwatch.StartNew() : null;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();

            var totalInserted = 0;

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                if (cols.Length == 0)
                {
                    // All columns are DB-generated — use DEFAULT VALUES syntax.
                    // DEFAULT VALUES does not support batching so we loop per entity.
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"INSERT INTO {m.EscTable} DEFAULT VALUES";
                    foreach (var _ in entityList)
                        totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                else
                {
                    // 2. Create ONE command and ONE set of parameters that will be reused.
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    // Use INSERT without RETURNING — bulk path uses ExecuteNonQuery
                    // and doesn't hydrate generated keys, so RETURNING output is wasted work.
                    cmd.CommandText = BuildInsert(m, hydrateGeneratedKeys: false);

                    // Create parameter objects ONCE and add them to the command.
                    var parameters = new DbParameter[cols.Length];
                    for (int i = 0; i < cols.Length; i++)
                    {
                        var p = cmd.CreateParameter();
                        p.ParameterName = $"{ParamPrefix}{cols[i].PropName}";
                        cmd.Parameters.Add(p);
                        parameters[i] = p;
                    }

                    // 3. Prepare the command ONCE before the loop.
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    // 4. Loop through each entity and execute the prepared command.
                    foreach (var entity in entityList)
                    {
                        // 5. Simply update the values of the existing parameters. No new objects created.
                        for (int i = 0; i < cols.Length; i++)
                        {
                            parameters[i].Value = cols[i].Getter(entity) ?? DBNull.Value;
                        }

                        totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }

                // Use CancellationToken.None so a cancelled caller token after a successful commit
                // does not cause a spurious OperationCanceledException for already-committed data.
                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); // Use None so cancelled caller token does not abort rollback
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkInsert failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable — satisfies compiler
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw?.Elapsed ?? default);
            return totalInserted;
        }
        
        /// <summary>
        /// Updates multiple entities using a temp table approach for efficient bulk updates.
        /// Uses temp tables and UPDATE FROM pattern; significantly faster than the base class batched operations.
        /// </summary>
        /// <typeparam name="T">Type of entity being updated.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping metadata for the entity's table.</param>
        /// <param name="entities">Entities containing new values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var tempTableName = $"\"BulkUpdate_{Guid.NewGuid():N}\"";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToList();
            var keyCols = m.KeyColumns.ToList();

            var totalUpdated = 0;

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                // Create temp table with same schema
                var colDefs = string.Join(", ", m.Columns.Select(c => $"{Escape(c.PropName)} {GetSqliteType(c.Prop.PropertyType)}"));
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"CREATE TEMP TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                // Insert entities into temp table using prepared statement
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    var insertCols = m.Columns.ToArray();
                    var paramPlaceholders = string.Join(", ", insertCols.Select(c => ParamPrefix + c.PropName));
                    var colNames = string.Join(", ", insertCols.Select(c => Escape(c.PropName)));
                    cmd.CommandText = $"INSERT INTO {tempTableName} ({colNames}) VALUES ({paramPlaceholders})";

                    var parameters = new DbParameter[insertCols.Length];
                    for (int i = 0; i < insertCols.Length; i++)
                    {
                        var p = cmd.CreateParameter();
                        p.ParameterName = ParamPrefix + insertCols[i].PropName;
                        cmd.Parameters.Add(p);
                        parameters[i] = p;
                    }

                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    foreach (var entity in entityList)
                    {
                        for (int i = 0; i < insertCols.Length; i++)
                        {
                            parameters[i].Value = insertCols[i].Getter(entity) ?? DBNull.Value;
                        }
                        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }
                }

                // Perform bulk update using temp table join
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    var keyMatchConditions = string.Join(" AND ", keyCols.Select(k => $"{tempTableName}.{Escape(k.PropName)} = {m.EscTable}.{k.EscCol}"));
                    // X3: Include timestamp in EXISTS match to enforce OCC
                    // Use IS (null-safe equality) instead of = because NULL = NULL is NULL (falsy) in SQL.
                    // SQLite's IS operator treats NULL IS NULL as TRUE, matching entities with null tokens.
                    var tsCondition = m.TimestampColumn != null
                        ? $" AND {tempTableName}.{Escape(m.TimestampColumn.PropName)} IS {m.EscTable}.{m.TimestampColumn.EscCol}"
                        : "";
                    var setClause = string.Join(", ", nonKeyCols.Select(c => $"{c.EscCol} = (SELECT {Escape(c.PropName)} FROM {tempTableName} WHERE {keyMatchConditions})"));
                    var whereClause = $"EXISTS (SELECT 1 FROM {tempTableName} WHERE {keyMatchConditions}{tsCondition})";
                    // X1: Add tenant predicate to prevent cross-tenant modifications
                    if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                    {
                        var tenantParam = $"{ParamPrefix}__tenant_bulk";
                        cmd.AddParam(tenantParam, ctx.Options.TenantProvider.GetCurrentTenantId());
                        whereClause += $" AND {m.EscTable}.{m.TenantColumn.EscCol} = {tenantParam}";
                    }
                    cmd.CommandText = $"UPDATE {m.EscTable} SET {setClause} WHERE {whereClause}";
                    totalUpdated = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }

                // Clean up temp table
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"DROP TABLE {tempTableName}";
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                // Use CancellationToken.None so a cancelled caller token after a successful commit
                // does not cause a spurious OperationCanceledException for already-committed data.
                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); // Use None so cancelled caller token does not abort rollback
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkUpdate failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable — satisfies compiler
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        /// <summary>
        /// Maps a CLR type to its corresponding SQLite type affinity.
        /// </summary>
        private static string GetSqliteType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte) || t == typeof(bool)) return "INTEGER";
            if (t == typeof(decimal) || t == typeof(double) || t == typeof(float)) return "REAL";
            if (t == typeof(byte[])) return "BLOB";
            return "TEXT";
        }
        
        /// <summary>
        /// Deletes entities in bulk using WHERE IN clauses for single-key tables
        /// or prepared statements for composite keys.
        /// Uses batched WHERE IN clauses; significantly faster than the base class operations.
        /// </summary>
        /// <typeparam name="T">Type of entity to delete.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> managing the connection.</param>
        /// <param name="m">Mapping that provides key column information.</param>
        /// <param name="entities">Entities whose keys determine the rows to remove.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            if (m.KeyColumns.Length == 0)
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");
            
            var totalDeleted = 0;
            // Respect provider parameter limits when batching deletes
            var batchSize = ctx.Options.BulkBatchSize;
            if (MaxParameters != int.MaxValue)
                batchSize = Math.Min(batchSize, MaxParameters);
            if (batchSize <= 0) batchSize = 1;
            
            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                if (m.KeyColumns.Length == 1)
                {
                    var keyCol = m.KeyColumns[0];

                    for (int i = 0; i < entityList.Count; i += batchSize)
                    {
                        var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                        await using var cmd = ctx.Connection.CreateCommand();
                        cmd.Transaction = transaction;
                        cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                        var paramNames = new List<string>();
                        var paramIndex = 0;

                        foreach (var entity in batch)
                        {
                            var paramName = $"{ParamPrefix}p{paramIndex++}";
                            paramNames.Add(paramName);
                            cmd.AddParam(paramName, keyCol.Getter(entity));
                        }

                        // X1: Add tenant predicate to prevent cross-tenant deletes
                        var tenantSuffix = "";
                        if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                        {
                            var tenantParam = $"{ParamPrefix}__tenant_bulk";
                            cmd.AddParam(tenantParam, ctx.Options.TenantProvider.GetCurrentTenantId());
                            tenantSuffix = $" AND {m.TenantColumn.EscCol} = {tenantParam}";
                        }
                        cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} IN ({string.Join(",", paramNames)}){tenantSuffix}";
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }
                else
                {
                    // X1: Build delete SQL with optional tenant predicate
                    bool hasTenant = ctx.Options.TenantProvider != null && m.TenantColumn != null;
                    string compositeDeleteSql;
                    if (hasTenant)
                    {
                        var whereParts = m.KeyColumns.Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                        if (m.TimestampColumn != null)
                        {
                            var tc = m.TimestampColumn;
                            whereParts.Add($"({tc.EscCol}={ParamPrefix}{tc.PropName} OR ({tc.EscCol} IS NULL AND {ParamPrefix}{tc.PropName} IS NULL))");
                        }
                        whereParts.Add($"{m.TenantColumn!.EscCol}={ParamPrefix}__tenant_bulk");
                        compositeDeleteSql = $"DELETE FROM {m.EscTable} WHERE {string.Join(" AND ", whereParts)}";
                    }
                    else
                    {
                        compositeDeleteSql = BuildDelete(m);
                    }

                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = compositeDeleteSql;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    foreach (var entity in entityList)
                    {
                        cmd.Parameters.Clear();
                        foreach (var col in m.KeyColumns)
                        {
                            cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                        }
                        if (m.TimestampColumn != null)
                        {
                            cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                        }
                        if (hasTenant)
                        {
                            cmd.AddParam($"{ParamPrefix}__tenant_bulk", ctx.Options.TenantProvider!.GetCurrentTenantId());
                        }
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }

                // Use CancellationToken.None so a cancelled caller token after a successful commit
                // does not cause a spurious OperationCanceledException for already-committed data.
                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); // Use None so cancelled caller token does not abort rollback
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkDelete failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable — satisfies compiler
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
    }
}
