using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
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
    public partial class SqliteProvider : DatabaseProvider
    {
        /// <summary>
        /// SQLite has no true async I/O - all async methods are synchronous wrappers.
        /// Using sync calls eliminates ~50-100ns of async state machine overhead per Read().
        /// </summary>
        public override bool PrefersSyncExecution => true;

        internal override bool SupportsQueryPlanPreparedCommandCache => true;

        /// <summary>
        /// SQLite reuses a pooled prepared command on the simple/paging fast paths, exactly as it already
        /// does on the full query-plan path (<see cref="SupportsQueryPlanPreparedCommandCache"/>). This removes
        /// a fresh <c>DbCommand</c>+<c>DbParameter</c> allocation and re-parse per call on the hot read path —
        /// the largest remaining gap between the runtime and compiled paths — for the common repeated query.
        /// </summary>
        internal override bool SupportsFastPathPreparedCommandCache => true;

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
        /// SQLite generates a single-column INTEGER primary key from the rowid when the column is omitted
        /// on INSERT, and honors an explicitly-supplied value — exactly the store-generated-key convention
        /// (EF Core parity). Enabled so a plain <c>int Id</c> primary key with no <c>[DatabaseGenerated]</c>
        /// annotation is store-generated when default and honored when set. See <see cref="Column.IsConventionGeneratedKey"/>.
        /// </summary>
        public override bool SupportsConventionKeyStoreGeneration => true;

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

        /// <summary>
        /// SQLite's <c>||</c> propagates NULL; COALESCE each operand so a NULL
        /// contributes an empty string, matching C# string concatenation.
        /// </summary>
        public override string GetNullSafeConcatSql(string left, string right)
            => $"(COALESCE({left}, '') || COALESCE({right}, ''))";

        /// <summary>SQLite treats a negative LIMIT as unlimited; clamp to zero.</summary>
        internal override string ClampNonNegativeLimitExpression(string expr) => $"max({expr}, 0)";

        /// <summary>SQLite TEXT affinity is the natural target for numeric/Guid/DateTime ToString().</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS TEXT)";

        /// <inheritdoc />
        public override string ForceCaseSensitiveStringComparison(string sql) => $"{sql} COLLATE BINARY";

        /// <summary>SQLite has no XOR operator - synthesize via `(a | b) - (a &amp; b)`.</summary>
        public override string GetBitwiseXorSql(string left, string right) => $"(({left} | {right}) - ({left} & {right}))";

        /// <summary>
        /// SQLite stores DateTime as TEXT; lex compare on raw ISO strings with mixed
        /// timezone offsets ('+02:00' vs 'Z') silently mis-orders rows because the offset
        /// suffix dominates the comparison. <c>datetime(...)</c> parses any ISO format
        /// and produces a canonical 'YYYY-MM-DD HH:MM:SS' UTC text whose lex order matches
        /// chronological order. Applied bilaterally in ETSV.VisitBinary for DateTime
        /// comparisons.
        /// </summary>
        public override string NormalizeDateTimeForCompare(string sql) => $"datetime({sql})";

        /// <summary>
        /// SQLite stores decimal as TEXT and operators lex-compare ('10.5' &lt; '2.0' because
        /// '1' &lt; '2'). CAST(sql AS REAL) forces numeric semantics for comparison /
        /// arithmetic / aggregation / dedup / sort. IEEE-754 binary precision loss is the
        /// standard tradeoff. Other providers' DECIMAL is precise so they keep identity.
        /// </summary>
        public override string NormalizeDecimalForCompare(string sql) => $"CAST({sql} AS REAL)";

        /// <summary>
        /// Name of the registered collation that orders decimal TEXT at full 28-digit precision.
        /// </summary>
        internal const string DecimalOrderCollation = "NORM_DECIMAL";

        /// <summary>
        /// Full-precision decimal ORDER BY key. Unlike <see cref="NormalizeDecimalForCompare"/>
        /// (CAST AS REAL, which loses precision beyond ~16 significant digits), this orders by the
        /// registered <see cref="DecimalOrderCollation"/> collation, which parses the canonical decimal
        /// TEXT and compares via <see cref="decimal.Compare"/> at full precision. Ordering only -
        /// arithmetic keeps the REAL coercion; aggregation goes through <see cref="DecimalAggregateSql"/>.
        /// </summary>
        internal override string OrderByDecimalKeySql(string sql) => $"({sql}) COLLATE {DecimalOrderCollation}";

        /// <summary>
        /// Full-precision decimal aggregates. SUM / AVG route to the registered
        /// <c>norm_decimal_sum</c> / <c>norm_decimal_avg</c> aggregate functions, which accumulate in
        /// <see cref="decimal"/> and return canonical decimal TEXT (materialized exactly, like a stored
        /// decimal column). MIN / MAX stay native but compare their TEXT operand through the
        /// <see cref="DecimalOrderCollation"/> collation, so they pick the true extreme and return an
        /// actual stored value. Anything else falls back to the base REAL coercion.
        /// </summary>
        internal override string DecimalAggregateSql(string sqlFunction, string operandSql, bool distinct = false)
        {
            var d = distinct ? "DISTINCT " : string.Empty;
            return sqlFunction switch
            {
                "SUM" => $"norm_decimal_sum({d}{operandSql})",
                "AVG" => $"norm_decimal_avg({d}{operandSql})",
                "MIN" or "MAX" => $"{sqlFunction}({d}({operandSql}) COLLATE {DecimalOrderCollation})",
                _ => base.DecimalAggregateSql(sqlFunction, operandSql, distinct),
            };
        }

        /// <summary>
        /// Exact decimal key for TEXT-stored decimals: strip trailing fraction zeros
        /// (so '10.50' and '10.5' compare equal, matching decimal's scale-insensitive
        /// equality) and fold negative zero. Values without a '.' are already
        /// canonical -- rtrim must not run on them ('100' would become '1').
        /// </summary>
        internal override string? CanonicalDecimalTextForExactCompare(string sql)
        {
            var trimmed = $"rtrim(rtrim({sql}, '0'), '.')";
            return $"(CASE WHEN instr({sql}, '.') = 0 THEN {sql} WHEN {trimmed} = '-0' THEN '0' ELSE {trimmed} END)";
        }

        /// <summary>
        /// SQLite stores TimeSpan as canonical 'c' TEXT ("d.hh:mm:ss.fffffff").
        /// Lex-comparison of the TEXT column gives wrong order for multi-day durations
        /// ("10.00:00:00" &lt; "9.23:59:59" lex but 10 days &gt; 9 days 23 hours).
        /// Convert to fractional seconds so numeric ordering applies.
        /// </summary>
        public override string NormalizeTimeSpanForCompare(string sql)
            => TimeSpanColumnTotalSecondsSql(sql);

        /// <summary>
        /// SQLite stores DateTimeOffset as offset-suffixed TEXT, so lexical ORDER BY sorts by the
        /// wall-clock text rather than the UTC instant and mis-orders values recorded at different
        /// offsets. Convert to the UTC epoch (microsecond precision, MaxValue-safe) so the sort
        /// matches DateTimeOffset's instant comparison — the same conversion the WHERE path uses.
        /// </summary>
        public override string NormalizeDateTimeOffsetForCompare(string sql)
            => GetDateTimeOffsetUtcEpochMicrosecondsSql(sql);

        /// <summary>
        /// Renders the offset-suffixed DateTimeOffset text as canonical UTC text so same-instant
        /// values written at different offsets produce IDENTICAL group keys. strftime performs
        /// the offset conversion; the fraction is stripped first (MaxValue's '.9999999' would
        /// otherwise roll strftime past year 9999 into NULL) and re-appended verbatim — sub-second
        /// digits are offset-invariant since offsets have whole-minute granularity. The '+00:00'
        /// suffix keeps the selected key parseable as a DateTimeOffset.
        /// </summary>
        internal override string CanonicalizeDateTimeOffsetGroupKey(string sql)
        {
            var secondsPrecision = DateTimeOffsetSecondsPrecisionText(sql, out var fractionalTail, out var fractionalLength);
            // Zero-pad the fraction to 7 digits so '.5' and '.500' (same instant) render identically.
            return $"(strftime('%Y-%m-%d %H:%M:%S', {secondsPrecision}) || '.' || " +
                   $"CASE WHEN substr({sql}, 20, 1) = '.' " +
                   $"THEN substr(substr({fractionalTail}, 1, {fractionalLength}) || '0000000', 1, 7) ELSE '0000000' END" +
                   $" || '+00:00')";
        }

        // SQLite's LIKE folds ASCII case regardless of collation, so a case-sensitive (ordinal)
        // Contains/StartsWith/EndsWith cannot be expressed with LIKE. instr / substr / '=' are all
        // byte-exact (case-sensitive), so bypass LIKE for the default (non-ignoreCase) match to
        // mirror .NET's ordinal string.Contains/StartsWith/EndsWith.
        internal override bool UsesOrdinalStringMatchBypass => true;

        internal override string GetOrdinalStringMatchSql(string columnSql, string patternSql, OrdinalStringMatch kind)
            => OrdinalStringMatchCore(columnSql, patternSql, kind);

        // Shared by the visitor-facing hook above and TranslateFunction's projection route so the
        // WHERE and SELECT translations of the same string match cannot drift apart.
        private static string OrdinalStringMatchCore(string columnSql, string patternSql, OrdinalStringMatch kind)
            => kind switch
            {
                // An empty pattern matches every NON-NULL row in .NET. The empty-pattern arm must
                // require the column to be non-null: a bare `length(pattern) = 0` is TRUE regardless
                // of the column, silently matching NULL rows that .NET (and LIKE '%' on other
                // providers, which is UNKNOWN for NULL) would exclude.
                OrdinalStringMatch.Contains
                    => $"(({columnSql} IS NOT NULL AND length({patternSql}) = 0) OR instr({columnSql}, {patternSql}) > 0)",
                OrdinalStringMatch.StartsWith
                    => $"(substr({columnSql}, 1, length({patternSql})) = {patternSql})",
                OrdinalStringMatch.EndsWith
                    => $"(({columnSql} IS NOT NULL AND length({patternSql}) = 0) OR substr({columnSql}, -length({patternSql})) = {patternSql})",
                _ => throw new ArgumentOutOfRangeException(nameof(kind))
            };

        /// <summary>
        /// SQLite uses <c>printf('%.Nf', col)</c> to produce a fixed-decimal text
        /// matching .NET's <c>ToString("F{digits}")</c>.
        /// </summary>
        public override string FormatFixedDecimalSql(string sql, int digits)
            => $"printf('%.{digits}f', {sql})";

        /// <summary>
        /// SQLite uses <c>strftime(...)</c> with %Y/%m/%d/%H/%M/%S tokens.
        /// Converts the .NET pattern (yyyy/MM/dd/HH/mm/ss) to strftime tokens
        /// via the existing helper; returns null when conversion fails
        /// (locale-aware MMM/dddd/etc. or other unsupported tokens).
        /// </summary>
        public override string? FormatDateUsingDotNetPattern(string sql, string dotNetFormat)
        {
            if (!nORM.Query.DateFormatSqlConversion.TryConvertDotNetDateFormatToStrftime(dotNetFormat, out var strftimeFmt))
                return null;
            return $"strftime('{strftimeFmt}', {sql})";
        }

        /// <summary>
        /// SQLite emits <c>strftime(..., col, modifier)</c> with a sign-prefixed
        /// seconds modifier; the RTRIM trim strips trailing fractional-second
        /// zeros so the result matches the canonical TEXT-stored DateTime form
        /// the materializer reads.
        /// </summary>
        public override string? AddSecondsToDateTimeSql(string dateTimeSql, string secondsSqlFragment)
            => $"RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', {dateTimeSql}, ({secondsSqlFragment}) || ' seconds'), '0'), '.')";

        /// <summary>
        /// Parses a TimeSpan TEXT column (canonical "c" format
        /// <c>[-][d.]HH:mm:ss[.fffffff]</c>) into a fractional-seconds REAL
        /// expression. SQLite has no string-split primitive so this builds a
        /// CASE selecting on shape: signed-vs-unsigned crossed with has-day-
        /// prefix-vs-not. The day prefix is detected by the position of the
        /// first '.' relative to the first ':' (a day-separator dot precedes
        /// the hours colon; a fractional-seconds dot follows it).
        ///
        /// Sub-day spans like "05:30:00" use the simple HH:mm:ss substring
        /// slice; multi-day spans like "3.05:30:00" peel the day count off
        /// the leading digits before slicing the time portion. Negative spans
        /// drop the leading '-' and negate the final seconds value.
        /// </summary>
        internal static string TimeSpanColumnTotalSecondsSql(string col)
        {
            // Time-only parse (after any sign + day prefix have been stripped).
            // s already starts at the 'HH' position.
            static string ParseTimeOnly(string s) =>
                $"(CAST(substr({s}, 1, 2) AS INTEGER) * 3600 + " +
                $"CAST(substr({s}, 4, 2) AS INTEGER) * 60 + " +
                $"CAST(substr({s}, 7, 2) AS INTEGER) + " +
                $"CASE WHEN length({s}) > 9 THEN CAST(substr({s}, 10) AS REAL) / 10000000.0 ELSE 0 END)";

            // Day + time parse: leading digits up to '.' are the day count;
            // everything after the '.' is the time portion.
            static string ParseWithDays(string s) =>
                $"(CAST(substr({s}, 1, INSTR({s}, '.') - 1) AS INTEGER) * 86400 + " +
                $"{ParseTimeOnly($"substr({s}, INSTR({s}, '.') + 1)")})";

            // True when s contains a 'd.' day prefix.
            static string HasDays(string s) =>
                $"(INSTR({s}, '.') > 0 AND INSTR({s}, '.') < INSTR({s}, ':'))";

            var unsigned = $"substr({col}, 2)";
            return
                $"(CASE " +
                $"WHEN substr({col}, 1, 1) = '-' AND {HasDays(unsigned)} THEN -1.0 * {ParseWithDays(unsigned)} " +
                $"WHEN substr({col}, 1, 1) = '-' THEN -1.0 * {ParseTimeOnly(unsigned)} " +
                $"WHEN {HasDays(col)} THEN {ParseWithDays(col)} " +
                $"ELSE {ParseTimeOnly(col)} " +
                $"END)";
        }

        /// <summary>
        /// SQLite stores DateTimeOffset as canonical ISO-8601 text. strftime parses
        /// the column (including its trailing offset suffix) to a UTC instant; the
        /// 'N seconds' modifier shifts the rendered wall clock by the new offset.
        /// The literal offset suffix is appended client-side to complete the canonical
        /// text the materialiser parses via <c>DateTimeOffset.Parse</c>.
        /// </summary>
        public override string GetDateTimeOffsetWithOffsetSql(string dtoSql, TimeSpan offset)
        {
            var totalSec = (long)offset.TotalSeconds;
            var suffix = FormatOffsetSuffix(offset);
            var inner = totalSec == 0
                ? $"strftime('%Y-%m-%dT%H:%M:%S', {dtoSql})"
                : $"strftime('%Y-%m-%dT%H:%M:%S', {dtoSql}, '{(totalSec >= 0 ? "+" : "-")}{System.Math.Abs(totalSec)} seconds')";
            return $"({inner} || '{suffix}')";
        }

        /// <inheritdoc/>
        public override string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
        {
            var totalSec = (long)localOffset.TotalSeconds;
            return totalSec == 0
                ? $"strftime('%Y-%m-%d %H:%M:%S', {dtoSql})"
                : $"strftime('%Y-%m-%d %H:%M:%S', {dtoSql}, '{(totalSec >= 0 ? "+" : "-")}{System.Math.Abs(totalSec)} seconds')";
        }

        // SQLite stores DateTimeOffset as text `yyyy-MM-dd HH:mm:ss[.fffffff][±HH:MM|Z]`.
        // strftime('%s', …) must be handed the whole-second datetime WITH its offset but WITHOUT
        // the fractional part: feeding the raw '.9999999' makes SQLite round 23:59:59 up past year
        // 9999 for DateTimeOffset.MaxValue and return NULL, which silently corrupts equality (NULL =
        // NULL is never true) and ORDER BY (NULL sorts first). The fraction is added back separately
        // by callers that need sub-second precision.
        private static string DateTimeOffsetSecondsPrecisionText(string dtoSql, out string fractionalTail, out string fractionalLength)
        {
            fractionalTail = $"substr({dtoSql}, 21)";
            fractionalLength =
                $"(CASE WHEN instr({fractionalTail}, '+') > 0 THEN instr({fractionalTail}, '+') - 1 " +
                $"WHEN instr({fractionalTail}, '-') > 0 THEN instr({fractionalTail}, '-') - 1 " +
                $"WHEN instr({fractionalTail}, 'Z') > 0 THEN instr({fractionalTail}, 'Z') - 1 " +
                $"ELSE length({fractionalTail}) END)";
            // When a fraction is present (position 20 is '.'), rebuild `date+time` (positions 1-19)
            // concatenated with the offset suffix that follows the fraction; otherwise the stored
            // text is already fraction-free.
            return $"(CASE WHEN substr({dtoSql}, 20, 1) = '.' " +
                   $"THEN substr({dtoSql}, 1, 19) || substr({dtoSql}, 21 + {fractionalLength}) ELSE {dtoSql} END)";
        }

        /// <inheritdoc/>
        public override string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
        {
            var secondsPrecision = DateTimeOffsetSecondsPrecisionText(dtoSql, out _, out _);
            return $"CAST(strftime('%s', {secondsPrecision}) AS INTEGER)";
        }

        internal override string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"CAST(ROUND((julianday({dtoSql}) - 2440587.5) * 86400000.0) AS INTEGER)";

        internal override string GetDateTimeOffsetUtcEpochMicrosecondsSql(string dtoSql)
        {
            var secondsPrecision = DateTimeOffsetSecondsPrecisionText(dtoSql, out var fractionalTail, out var fractionalLength);
            return $"((CAST(strftime('%s', {secondsPrecision}) AS INTEGER) * 1000000) + " +
                   $"CAST(CASE WHEN substr({dtoSql}, 20, 1) = '.' " +
                   $"THEN substr(substr({fractionalTail}, 1, {fractionalLength}) || '000000', 1, 6) ELSE '0' END AS INTEGER))";
        }

        /// <inheritdoc/>
        public override string GetTimeSpanColumnSecondsSql(string timeSpanColumnSql)
            // Force REAL coercion. TimeSpanColumnTotalSecondsSql returns INTEGER
            // when the parsed span has no fractional component (the common
            // 'HH:mm:ss' canonical 'c' format with no '.fffffff' suffix); the
            // materialiser's ConvertToTimeSpan(long) path would then treat the
            // value as ticks instead of seconds.
            => $"({TimeSpanColumnTotalSecondsSql(timeSpanColumnSql)} * 1.0)";

        /// <summary>
        /// SQLite TimeSpan columns are canonical-c-format TEXT; parse into a
        /// fractional-seconds expression via <see cref="TimeSpanColumnTotalSecondsSql"/>
        /// (which handles multi-day spans and signed values) and feed to
        /// AddSecondsToDateTimeSql.
        /// </summary>
        public override string? AddTimeSpanColumnToDateTimeSql(string dateTimeSql, string timeSpanColumnSql, bool subtract)
        {
            var sign = subtract ? "-1.0 * " : "";
            var seconds = $"{sign}{TimeSpanColumnTotalSecondsSql(timeSpanColumnSql)}";
            return AddSecondsToDateTimeSql(dateTimeSql, seconds);
        }

        /// <summary>
        /// SQLite stores DTOs as offset-suffixed ISO text. The base hook's
        /// strftime drops the trailing offset, so the materialiser would parse
        /// the result as Local and shift the wall clock by the machine's TZ.
        /// Preserve the original suffix and shift the wall clock by both the
        /// TimeSpan seconds AND the original offset (the latter cancels out
        /// strftime's UTC normalisation). substr(col, 20) extracts the
        /// trailing offset since canonical DTO text is 19-char date+time
        /// followed by a signed HH:MM offset.
        /// </summary>
        public override string? AddTimeSpanColumnToDateTimeOffsetSql(string dtoSql, string timeSpanColumnSql, bool subtract)
        {
            var sign = subtract ? "-1.0 * " : "";
            var seconds = $"{sign}{TimeSpanColumnTotalSecondsSql(timeSpanColumnSql)}";
            // strftime with '%Y-%m-%d %H:%M:%f' parses the offset-suffixed input
            // and outputs the UTC wall clock; appending the original suffix
            // would mislabel UTC as the original offset. Instead, use the offset
            // as a modifier to keep wall-clock at the original offset, then
            // re-append the suffix.
            // Approach: shift by seconds AND by inverse-offset-then-offset (net
            // zero TZ shift, just preserves the offset suffix).
            var offsetSuffix = $"substr({dtoSql}, 20)";
            var shifted = $"RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', {dtoSql}, ({seconds}) || ' seconds', {offsetSuffix}), '0'), '.')";
            return $"({shifted} || {offsetSuffix})";
        }

        /// <summary>SQLite uses strftime with a 'N days' modifier on the DateOnly TEXT column.</summary>
        public override string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment)
            => $"strftime('%Y-%m-%d', {dateOnlySql}, ({daysSqlFragment}) || ' days')";

        /// <summary>SQLite uses GROUP_CONCAT(expr, sep); the 2-arg form takes the separator directly.</summary>
        public override string GetStringAggregateSql(string expr, string sepLiteral)
            => $"GROUP_CONCAT({expr}, {sepLiteral})";

        /// <summary>
        /// Microsoft.Data.Sqlite 8.0.0 ships SQLite 3.43, which does not yet support the
        /// aggregate ORDER BY syntax (<c>GROUP_CONCAT(x, sep ORDER BY key)</c> added in 3.44).
        /// Setting this to false keeps ORDER BY on the outer SELECT instead; SQLite then scans
        /// the table in ORDER BY order (guaranteed for INTEGER PRIMARY KEY), which GROUP_CONCAT
        /// observes as its input order.
        /// </summary>
        public override bool SupportsNativeOrderedStringAggregate => false;

        /// <summary>
        /// SQLite REGEXP is an infix operator backed by a user-defined function;
        /// nORM registers the deterministic backing function during SQLite provider
        /// initialization, so generated LINQ regex predicates execute without
        /// caller-owned connection setup. The emitted SQL stays the canonical
        /// operator form so the contract is portable.
        /// </summary>
        public override string GetRegexMatchSql(string inputSql, string patternLiteral)
            => $"({inputSql} REGEXP {patternLiteral})";

        /// <summary>
        /// SQLite has no built-in regex_replace; nORM registers a deterministic
        /// managed backing function during SQLite provider initialization. The
        /// emitted shape stays portable with PostgreSQL's <c>regexp_replace</c>.
        /// </summary>
        public override string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => $"regexp_replace({inputSql}, {patternLiteral}, {replacementLiteral})";

        /// <summary>
        /// SQLite's 'months'/'years' modifiers normalize an overflowed day forward
        /// ('Feb 31' becomes Mar 2/3) while .NET clamps to the last day of the
        /// intended month. A rolled-over result always has a SMALLER day-of-month
        /// than the source (1..3 vs 29..31), so the CASE detects overflow and backs
        /// up to 'start of month, -1 day'. Same shape as the DateTime calendar add.
        /// </summary>
        private static string CalendarAddDateOnly(string dateOnlySql, string modifierSql)
        {
            var cand = $"strftime('%Y-%m-%d', {dateOnlySql}, {modifierSql})";
            return $"(CASE WHEN CAST(strftime('%d', {cand}) AS INTEGER) < CAST(strftime('%d', {dateOnlySql}) AS INTEGER) " +
                   $"THEN date({cand}, 'start of month', '-1 day') ELSE {cand} END)";
        }

        /// <summary>SQLite 'N months' modifier on the DateOnly TEXT column, day-clamped like .NET.</summary>
        public override string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment)
            => CalendarAddDateOnly(dateOnlySql, $"({monthsSqlFragment}) || ' months'");

        /// <summary>SQLite 'N years' modifier on the DateOnly TEXT column, day-clamped like .NET.</summary>
        public override string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment)
            => CalendarAddDateOnly(dateOnlySql, $"({yearsSqlFragment}) || ' years'");

        /// <summary>
        /// SQLite TimeOnly is 'HH:mm:ss' text. strftime needs a date prefix to
        /// apply a seconds modifier; we inject '1900-01-01 ' and re-format to
        /// 'HH:mm:ss' afterward. Sub-day only (overflow drops the day field).
        /// </summary>
        public override string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment)
            => $"strftime('%H:%M:%S', '1900-01-01 ' || {timeOnlySql}, ({secondsSqlFragment}) || ' seconds')";

        /// <summary>
        /// SQLite TimeSpan columns are 'HH:mm:ss' text; parse seconds via
        /// substr/CAST and feed to AddSecondsToTimeOnlySql for the strftime
        /// modifier emit.
        /// </summary>
        public override string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract)
        {
            var sign = subtract ? "-1.0 * " : "";
            var seconds = $"{sign}{TimeSpanColumnTotalSecondsSql(timeSpanColumnSql)}";
            return AddSecondsToTimeOnlySql(timeOnlySql, seconds);
        }

        /// <summary>SQLite REAL handles both float and decimal - no DOUBLE PRECISION / DECIMAL(p,s) keywords.</summary>
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
            RegisterProviderFunctions(connection);
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
            RegisterProviderFunctions(connection);
            using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            pragmaCmd.ExecuteNonQuery();
        }

        private static void RegisterProviderFunctions(DbConnection connection)
        {
            if (connection is not SqliteConnection sqlite)
                return;

            sqlite.CreateFunction<string?, string?, bool>(
                "regexp",
                static (pattern, input) => pattern is not null && input is not null && Regex.IsMatch(input, pattern),
                isDeterministic: true);
            sqlite.CreateFunction<string?, string?, string?, string?>(
                "regexp_replace",
                static (input, pattern, replacement) => input is null || pattern is null
                    ? input
                    : Regex.Replace(input, pattern, replacement ?? string.Empty),
                isDeterministic: true);

            // Full-precision decimal ordering: SQLite stores decimal as canonical TEXT, and lexical
            // BINARY ordering is wrong for magnitude while CAST-AS-REAL loses precision beyond ~16
            // significant digits. This collation parses both operands and compares via decimal.Compare
            // at full 28-digit precision. Applied only where OrderByDecimalKeySql emits COLLATE NORM_DECIMAL.
            sqlite.CreateCollation(DecimalOrderCollation, static (x, y) => CompareCanonicalDecimalText(x, y));

            // Full-precision decimal SUM / AVG: the REAL coercion would accumulate in IEEE-754 double,
            // silently losing precision beyond ~16 significant digits. These aggregates accumulate in
            // decimal and return canonical decimal TEXT, which materializes exactly like a stored
            // decimal column. Empty / all-NULL inputs return NULL, matching SQL SUM / AVG semantics
            // (the query layer's empty-set handling is unchanged). Emitted by DecimalAggregateSql.
            sqlite.CreateAggregate<string?, (decimal Sum, bool Any), string?>(
                "norm_decimal_sum",
                (Sum: 0m, Any: false),
                static (acc, v) => v is null
                    ? acc
                    : (acc.Sum + ParseDecimalAggregateOperand(v), true),
                static acc => acc.Any ? acc.Sum.ToString(CultureInfo.InvariantCulture) : null,
                isDeterministic: true);
            sqlite.CreateAggregate<string?, (decimal Sum, long Count), string?>(
                "norm_decimal_avg",
                (Sum: 0m, Count: 0L),
                static (acc, v) => v is null
                    ? acc
                    : (acc.Sum + ParseDecimalAggregateOperand(v), acc.Count + 1),
                static acc => acc.Count == 0
                    ? null
                    : (acc.Sum / acc.Count).ToString(CultureInfo.InvariantCulture),
                isDeterministic: true);
        }

        /// <summary>
        /// Parses an aggregate operand for the full-precision decimal aggregates. Stored decimals are
        /// canonical plain-notation TEXT, but a COMPUTED operand (e.g. <c>Sum(x =&gt; x.Price * x.Qty)</c>)
        /// arrives as SQLite REAL arithmetic output, whose text form may use exponent notation -
        /// NumberStyles.Float accepts both.
        /// </summary>
        private static decimal ParseDecimalAggregateOperand(string v)
            => decimal.Parse(v, NumberStyles.Float, CultureInfo.InvariantCulture);

        /// <summary>
        /// Compares two canonical decimal TEXT values at full precision for the NORM_DECIMAL collation.
        /// Both operands are nORM's stored canonical decimal text; parse invariantly and compare via
        /// <see cref="decimal.Compare"/>. Falls back to ordinal text comparison only if an operand is not
        /// a parseable decimal (defensive - nORM always stores parseable canonical text).
        /// </summary>
        internal static int CompareCanonicalDecimalText(string? x, string? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;
            if (decimal.TryParse(x, NumberStyles.Number, CultureInfo.InvariantCulture, out var dx)
                && decimal.TryParse(y, NumberStyles.Number, CultureInfo.InvariantCulture, out var dy))
                return decimal.Compare(dx, dy);
            return string.CompareOrdinal(x, y);
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
            // A store-generated-key CONVENTION column (int PK, no explicit config) is also read back
            // via RETURNING for the default-value (store-generated) insert run: it is the rowid alias,
            // so RETURNING yields the generated key with the same clean one-result-set-per-row shape the
            // batch reader loop relies on. Explicit-value rows take the plain path (no read-back).
            var keyCol = m?.KeyColumns?.FirstOrDefault(c => c.IsDbGenerated)
                ?? m?.ConventionGeneratedKeyColumn;
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
            if (value is Guid guid)
                value = guid.ToString("D");
            return new SqliteParameter(name, value ?? DBNull.Value);
        }

        /// <summary>
        /// SQLite has no native interval type. julianday subtraction is a REAL whose
        /// float error reaches microseconds ('00:01:30' differences came back as
        /// 89.9999946s), so the difference works in integer epoch ticks — exact to
        /// the tick — and divides once at the end for the seconds-as-REAL contract
        /// (exact below 2^53 ticks, about 29,000 years of span).
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"(CAST({DateTimeTextEpochTicks(endSql)} - {DateTimeTextEpochTicks(startSql)} AS REAL) / 10000000.0)";

        /// <summary>
        /// SQLite has no DATEFROMPARTS; build the canonical 'yyyy-MM-dd HH:mm:ss'
        /// text with zero-padded components via strftime. printf gives us %02d
        /// padding (4-digit year is the standard; -1 to 9999 range covers .NET
        /// DateTime).
        /// </summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"strftime('%Y-%m-%d 00:00:00', " +
               $"printf('%04d-%02d-%02d', {yearSql}, {monthSql}, {daySql}))";

        /// <summary>6-arg SQLite date-from-parts with full 'yyyy-MM-dd HH:mm:ss' shape.</summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql)
            => $"strftime('%Y-%m-%d %H:%M:%S', " +
               $"printf('%04d-%02d-%02d %02d:%02d:%02d', {yearSql}, {monthSql}, {daySql}, {hourSql}, {minuteSql}, {secondSql}))";

        /// <summary>
        /// 7-arg variant: append '.fff' millisecond tail via printf; strftime
        /// '%Y-%m-%d %H:%M:%f' preserves the fractional seconds.
        /// </summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"strftime('%Y-%m-%d %H:%M:%f', " +
               $"printf('%04d-%02d-%02d %02d:%02d:%02d.%03d', {yearSql}, {monthSql}, {daySql}, {hourSql}, {minuteSql}, {secondSql}, {millisecondSql}))";

        /// <summary>SQLite printf yields the canonical DateOnly text 'yyyy-MM-dd'.</summary>
        public override string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"printf('%04d-%02d-%02d', {yearSql}, {monthSql}, {daySql})";

        /// <summary>SQLite printf yields the canonical TimeOnly text 'HH:mm:ss'.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => $"printf('%02d:%02d:%02d', {hourSql}, {minuteSql}, {secondSql})";

        /// <summary>4-arg variant: append '.fff' millisecond tail.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"printf('%02d:%02d:%02d.%03d', {hourSql}, {minuteSql}, {secondSql}, {millisecondSql})";

        /// <summary>
        /// SQLite stores TimeOnly as 'HH:mm:ss[.fffffff]' text. Parse each side's
        /// HH/MM/SS components (the SS substring carries the fractional tail when
        /// present, so CAST(... AS REAL) preserves sub-second precision) into total
        /// seconds, then wrap the diff modulo 86400 to match TimeOnly's [0, 24h)
        /// semantics.
        /// </summary>
        public override string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
        {
            string toSecs(string t) =>
                $"(CAST(substr({t}, 1, 2) AS INTEGER) * 3600 + " +
                $"CAST(substr({t}, 4, 2) AS INTEGER) * 60 + " +
                $"CAST(substr({t}, 7) AS REAL))";
            // SQLite's % works on REAL operands when either side is REAL; the
            // raw diff is in (-86400, 86400), so + 86400 first shifts into
            // (0, 172800) and the modulo wraps anything >= 86400 back down.
            return $"((({toSecs(endSql)} - {toSecs(startSql)}) + 86400.0) % 86400.0)";
        }
    }
}
