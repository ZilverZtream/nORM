using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Configuration;

#nullable enable

namespace nORM.Providers
{
    /// <summary>Which ordinal (case-sensitive) string-match a predicate is testing.</summary>
    internal enum OrdinalStringMatch
    {
        Contains,
        StartsWith,
        EndsWith
    }

    public abstract partial class DatabaseProvider
    {
        /// <summary>
        /// True when a connection with this connection string reaches a database PRIVATE to that
        /// connection - two connections with the IDENTICAL string still see DIFFERENT data (e.g.
        /// SQLite ':memory:' without shared cache). Cache keys derive database identity from the
        /// connection string, so connection-private databases must additionally be keyed per
        /// connection instance or a shared cache provider serves one database's rows for another.
        /// </summary>
        /// <param name="connectionString">The connection string to classify.</param>
        public virtual bool IsConnectionScopedDatabase(string connectionString) => false;

        /// <summary>
        /// Character used to escape wildcards in patterns passed to SQL <c>LIKE</c> clauses.
        /// Defaults to a backslash but can be overridden by providers with different
        /// escaping semantics.
        /// </summary>
        public virtual char LikeEscapeChar => '\\';

        /// <summary>
        /// Escapes occurrences of wildcard characters (<c>%</c> and <c>_</c>) in a
        /// pattern so that they are treated as literals in <c>LIKE</c> expressions.
        /// </summary>
        /// <param name="value">The raw pattern value supplied by the user.</param>
        /// <returns>The escaped pattern safe for inclusion in a <c>LIKE</c> clause.</returns>
        public virtual string EscapeLikePattern(string value)
        {
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar).ToString();
            return value
                .Replace(esc, esc + esc)
                .Replace("%", esc + "%")
                .Replace("_", esc + "_");
        }

        /// <summary>
        /// Generates SQL that escapes wildcard characters in a SQL expression for safe use in LIKE patterns.
        /// This is used when the LIKE pattern value comes from a runtime variable (not a constant).
        /// </summary>
        /// <param name="sqlExpression">The SQL expression (parameter reference or column) to escape.</param>
        /// <returns>SQL that escapes the expression for safe use in LIKE patterns.</returns>
        public virtual string GetLikeEscapeSql(string sqlExpression)
        {
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar).ToString();
            // Generate nested REPLACE calls to escape the escape char, %, and _
            // Example: REPLACE(REPLACE(REPLACE(value, '\', '\\'), '%', '\%'), '_', '\_')
            return $"REPLACE(REPLACE(REPLACE({sqlExpression}, '{esc}', '{esc}{esc}'), '%', '{esc}%'), '_', '{esc}_')";
        }

        /// <summary>
        /// Generates SQL that concatenates two SQL expressions. Defaults to ANSI CONCAT function.
        /// Providers that do not support CONCAT (e.g. SQLite) override this method.
        /// </summary>
        /// <param name="left">Left SQL expression.</param>
        /// <param name="right">Right SQL expression.</param>
        /// <returns>SQL fragment that concatenates the two expressions.</returns>
        public virtual string GetConcatSql(string left, string right) => $"CONCAT({left}, {right})";

        /// <summary>
        /// Concatenation with C# <see cref="string.Concat(string?, string?)"/> null semantics:
        /// a NULL operand contributes an empty string instead of nulling the whole result
        /// (<c>null + "!"</c> is <c>"!"</c> in C#, never null). The ANSI CONCAT default
        /// already ignores NULLs on SQL Server and PostgreSQL; providers whose concatenation
        /// propagates NULL (SQLite <c>||</c>, MySQL CONCAT) override with per-operand
        /// COALESCE. Used for user-level string concatenation (<c>+</c>, string.Concat,
        /// string.Join, string.Format); LIKE-pattern composition keeps
        /// <see cref="GetConcatSql"/> — there a NULL pattern must stay NULL, because
        /// COALESCE would turn it into <c>'%%'</c> and match every row.
        /// </summary>
        public virtual string GetNullSafeConcatSql(string left, string right) => GetConcatSql(left, right);

        /// <summary>
        /// Clamps a composite LIMIT/FETCH expression (e.g. the <c>(take - skip)</c>
        /// rewrite of Take-then-Skip) to zero when it evaluates negative. LINQ
        /// semantics for such a window are "no rows", but SQLite interprets a
        /// negative LIMIT as UNLIMITED, so passing the raw difference through would
        /// silently return the whole table. Identity by default: MySQL's LIMIT does
        /// not accept expressions at all, and providers whose engines already error
        /// on negative values keep their behavior.
        /// </summary>
        internal virtual string ClampNonNegativeLimitExpression(string expr) => expr;

        /// <summary>
        /// Converts a TimeSpan-typed operand to fractional SECONDS for comparison
        /// against a DateTime/TimeOnly difference (which the difference hooks emit
        /// as seconds). Distinct from <see cref="NormalizeTimeSpanForCompare"/>:
        /// that one is identity on providers whose native TIME/INTERVAL compare
        /// against each other, but a seconds-vs-native comparison needs an explicit
        /// conversion everywhere. The base reuses the normalize hook, which is the
        /// text-to-seconds parse on SQLite.
        /// </summary>
        internal virtual string TimeSpanOperandToSecondsSql(string sql)
            => NormalizeTimeSpanForCompare(sql);

        /// <summary>
        /// Renders a boolean PREDICATE as a selectable VALUE. Most dialects allow a
        /// predicate directly in the SELECT list (SQLite/MySQL yield 0/1, PostgreSQL
        /// boolean); T-SQL rejects it ('Incorrect syntax near IS') and needs the
        /// CASE-to-BIT form. Only for value positions — inside CASE WHEN tests the
        /// bare predicate is required.
        /// </summary>
        internal virtual string BooleanPredicateAsValue(string predicateSql) => predicateSql;

        /// <summary>
        /// A FROM-clause suffix exposing a correlated scalar as a named column the
        /// query can GROUP BY. SQL Server rejects subqueries inside GROUP BY and
        /// MySQL's only_full_group_by rejects a SELECT-side key that repeats the
        /// subquery, so those dialects group by an applied lateral column instead.
        /// Null means the dialect accepts the subquery directly in GROUP BY
        /// (SQLite, PostgreSQL) and no rewrite is needed.
        /// </summary>
        internal virtual string? AppliedScalarColumnClause(string scalarSql, string escapedAlias, string escapedColumn)
            => null;

        /// <summary>
        /// SQL for a C# EXPLICIT cast from floating point/decimal to an integer type,
        /// which truncates toward zero ((int)2.7 is 2, (int)-2.7 is -2). A bare CAST
        /// already truncates on SQLite and SQL Server; MySQL's CAST rounds half away
        /// from zero and PostgreSQL's rounds half to even, so those providers
        /// truncate explicitly before casting. Distinct from
        /// <see cref="ConvertFloatingToIntegralSql"/>, which implements
        /// Convert.ToIntXX's round-half-to-even.
        /// </summary>
        internal virtual string FloatingToIntegralTruncatingSql(string sql, bool asLong)
            => GetIntCastSql(sql, asLong);

        /// <summary>
        /// Returns SQL that converts <paramref name="innerSql"/> to its textual representation -
        /// used by the translator for LINQ <c>x.ToString()</c> calls on non-string columns.
        /// Default uses ANSI <c>CAST(x AS VARCHAR)</c>; providers override with their native
        /// text type (NVARCHAR(MAX) on SQL Server, TEXT on SQLite/Postgres, CHAR on MySQL).
        /// </summary>
        public virtual string GetToStringSql(string innerSql) => $"CAST({innerSql} AS VARCHAR)";

        /// <summary>
        /// Wraps a string operand so equality/IN comparisons use ordinal, case-sensitive
        /// semantics even on databases whose default column collation is case-insensitive.
        /// PostgreSQL is already case-sensitive by default, so the base implementation is
        /// identity.
        /// </summary>
        public virtual string ForceCaseSensitiveStringComparison(string sql) => sql;

        /// <summary>
        /// Wraps a string operand so RELATIONAL comparisons (&lt;, &gt;, and the
        /// string.CompareOrdinal / Compare(..., Ordinal) CASE sentinel built on them)
        /// order by code point. Equality case-sensitivity and relational ordering
        /// diverge on PostgreSQL: its equality is byte-exact (so
        /// <see cref="ForceCaseSensitiveStringComparison"/> is identity there), but the
        /// database's locale/ICU collation interleaves 'M' and 'm' for ordering —
        /// C#'s ordinal comparison puts all uppercase ASCII first. The default
        /// reuses the case-sensitivity wrap, which is already a binary COLLATION on
        /// SQL Server/MySQL and identity on SQLite (BINARY collation by default).
        /// </summary>
        internal virtual string OrdinalRelationalStringOperand(string sql)
            => ForceCaseSensitiveStringComparison(sql);

        /// <summary>
        /// Returns SQL that XORs two integer expressions. SQL Server and MySQL accept the
        /// `^` operator; PostgreSQL uses `#`; SQLite has no XOR operator and falls back to
        /// `(a | b) - (a &amp; b)` - algebraically equivalent on integers.
        /// </summary>
        public virtual string GetBitwiseXorSql(string left, string right) => $"({left} ^ {right})";

        /// <summary>
        /// Wraps a SQL operand for chronological DateTime comparison. SQL Server / Postgres /
        /// MySQL all have native DATETIME types whose comparison operators are timezone- and
        /// offset-aware, so the default is identity. SQLite stores DateTime as TEXT and
        /// comparison is lex-based, which silently mis-orders rows with mixed timezone
        /// offsets (e.g. '+02:00' suffix vs 'Z') -- SqliteProvider overrides to wrap the
        /// operand with <c>datetime(...)</c> for chronological semantics.
        /// </summary>
        public virtual string NormalizeDateTimeForCompare(string sql) => sql;

        /// <summary>
        /// Wraps a SQL operand for numeric decimal comparison / arithmetic / aggregation /
        /// dedup / sort. SQL Server / Postgres / MySQL all have native DECIMAL types whose
        /// operators preserve full decimal precision -- the default is identity. SQLite
        /// stores decimal as TEXT and lex-compares ('10.5' &lt; '2.0' because '1' &lt; '2'),
        /// so SqliteProvider overrides to wrap with <c>CAST({sql} AS REAL)</c>. The REAL
        /// coercion forces numeric semantics but with IEEE-754 binary precision loss --
        /// the standard tradeoff documented across the decimal-cluster fixes.
        /// </summary>
        public virtual string NormalizeDecimalForCompare(string sql) => sql;

        /// <summary>
        /// True when this provider stores <see cref="decimal"/> as TEXT (SQLite), so a raw
        /// <c>col = @p</c> / <c>col &lt; @p</c> comparison is a lexical string compare rather than a
        /// numeric one: equality is scale-sensitive (<c>"24.500"</c> ≠ <c>"24.5"</c> though the values
        /// are equal) and relational operators mis-order by magnitude. Only the full translator's
        /// <see cref="NormalizeDecimalForCompare"/> (CAST AS REAL) / <see cref="CanonicalDecimalTextForExactCompare"/>
        /// wrapping is correct, so the read fast paths must defer a decimal predicate to it when this is
        /// true. Native-DECIMAL providers (SQL Server / PostgreSQL / MySQL) compare exactly, so their
        /// fast-path raw predicate is already identical to what the full translator emits — the default
        /// is <c>false</c> and the fast path stays engaged.
        /// </summary>
        internal virtual bool StoresDecimalAsText => false;

        /// <summary>
        /// The sort key for a decimal ORDER BY / ThenBy. Server providers (SQL Server / PostgreSQL /
        /// MySQL) order the native DECIMAL exactly, so the default reuses
        /// <see cref="NormalizeDecimalForCompare"/> (identity). SQLite stores decimal as TEXT and the
        /// ordinary comparison coercion is CAST AS REAL (IEEE-754, precise only to ~16 significant
        /// digits), so SqliteProvider overrides this to a full-precision decimal collation for ordering.
        /// Ordering only: arithmetic keeps <see cref="NormalizeDecimalForCompare"/>; aggregation goes
        /// through <see cref="DecimalAggregateSql"/>.
        /// </summary>
        internal virtual string OrderByDecimalKeySql(string sql) => NormalizeDecimalForCompare(sql);

        /// <summary>
        /// The operand SQL to place inside <c>MIN()</c>/<c>MAX()</c> so the extreme is chosen by VALUE. The
        /// default is identity — native providers order their column types correctly. SQLite stores
        /// <see cref="TimeSpan"/> / <see cref="DateTimeOffset"/> as TEXT that BINARY-collates lexically
        /// (multi-day durations and mixed-offset instants mis-sort), so SqliteProvider wraps them in a
        /// value-ordering collation; MIN/MAX still returns the actual stored value (which materializes back).
        /// Decimal MIN/MAX go through <see cref="DecimalAggregateSql"/> instead.
        /// </summary>
        internal virtual string MinMaxAggregateOperand(string operandSql, Type clrType) => operandSql;

        /// <summary>
        /// The complete aggregate-call SQL for a decimal operand (SUM / AVG / MIN / MAX). Server
        /// providers aggregate the native DECIMAL exactly, so the default emits the plain aggregate
        /// over <see cref="NormalizeDecimalForCompare"/> (identity there). SQLite stores decimal as
        /// TEXT and the REAL coercion is IEEE-754 double - a SUM/AVG silently loses precision beyond
        /// ~16 significant digits and a MIN/MAX can return a value that does not exist in the data -
        /// so SqliteProvider overrides this with registered full-precision decimal aggregate functions
        /// and collation-ordered MIN/MAX.
        /// </summary>
        internal virtual string DecimalAggregateSql(string sqlFunction, string operandSql, bool distinct = false)
            => $"{sqlFunction}({(distinct ? "DISTINCT " : string.Empty)}{NormalizeDecimalForCompare(operandSql)})";

        /// <summary>
        /// Canonical decimal TEXT for EXACT equality / grouping / dedup on providers
        /// that store decimal as text. <see cref="NormalizeDecimalForCompare"/>'s REAL
        /// coercion is right for ordering and arithmetic but is IEEE-754 double, so two
        /// decimals that differ beyond ~15-17 significant digits collapse to the same
        /// key -- silently merging rows in equality predicates, GROUP BY, and DISTINCT.
        /// The canonical text (trailing fraction zeros stripped, negative zero folded)
        /// is unique per numeric value at full 28-digit precision AND stays a parseable
        /// decimal, so it can serve as a SELECT-side group key. Null means the
        /// provider's native DECIMAL comparisons are already exact.
        /// </summary>
        internal virtual string? CanonicalDecimalTextForExactCompare(string sql) => null;

        /// <summary>
        /// Canonical <see cref="TimeOnly"/> TEXT for EXACT equality on providers that store TimeOnly as
        /// TEXT (SQLite: <c>"HH:mm:ss[.fffffff]"</c>). A raw <c>=</c> is lexical, so the same time written
        /// with a different fractional scale (<c>"12:00:00.0000000"</c> vs <c>"12:00:00"</c>) compares
        /// unequal and silently drops rows. The canonical text strips trailing fraction zeros (only when a
        /// fractional part is present, so the whole-second field is untouched). Ordering does NOT need this
        /// — zero-padded TimeOnly text sorts chronologically. Null means the provider's native TIME type
        /// already compares by value.
        /// </summary>
        internal virtual string? CanonicalTimeOnlyTextForExactCompare(string sql) => null;

        /// <summary>
        /// Canonical <see cref="TimeSpan"/> TEXT for an EXACT KEY (GROUP BY / DISTINCT / dedup / join) on
        /// providers that store TimeSpan as <c>'c'</c> TEXT (SQLite: <c>"[d.]hh:mm:ss[.fffffff]"</c>). The
        /// same duration written with a different fraction scale (<c>"1.00:00:00.0000000"</c> vs
        /// <c>"1.00:00:00"</c>) must key together, so trailing fraction zeros are stripped — but ONLY from a
        /// real fractional part (the day-separator <c>.</c> and whole-second zeros must survive). The
        /// canonical text still parses back as the TimeSpan, so it doubles as the selected key. Null means
        /// the provider's native TIME/INTERVAL type already keys by value. Distinct from
        /// <see cref="NormalizeTimeSpanForCompare"/> (numeric seconds — right for ordering/equality, but a
        /// number does not materialize back as a TimeSpan group key).
        /// </summary>
        internal virtual string? CanonicalTimeSpanTextForExactCompare(string sql) => null;

        /// <summary>
        /// The expression to use where a decimal participates as an exact KEY
        /// (GROUP BY, DISTINCT, set-op dedup): the canonical text when the provider
        /// needs it, otherwise the ordinary comparison normalization.
        /// </summary>
        internal string ExactDecimalKeySql(string sql)
            => CanonicalDecimalTextForExactCompare(sql) ?? NormalizeDecimalForCompare(sql);

        /// <summary>
        /// The SQL to use where a column/value of <paramref name="clrType"/> participates as an EXACT KEY
        /// (GROUP BY, DISTINCT, set-op dedup, join equi-key): the type's canonical form on a provider that
        /// stores it as TEXT, otherwise the value unchanged. Centralizes the per-type canonicalization so a
        /// key site does not have to know which CLR types need it — <c>decimal</c> uses the canonical decimal
        /// text (REAL would merge values beyond double precision), <c>TimeOnly</c> strips trailing fraction
        /// zeros so the same time in a different scale keys together. Native-typed providers return identity.
        /// </summary>
        internal string ExactKeySql(string sql, Type clrType)
        {
            var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
            if (t == typeof(decimal)) return ExactDecimalKeySql(sql);
            if (t == typeof(TimeOnly)) return CanonicalTimeOnlyTextForExactCompare(sql) ?? sql;
            if (t == typeof(TimeSpan)) return CanonicalTimeSpanTextForExactCompare(sql) ?? sql;
            return sql;
        }

        /// <summary>
        /// Wraps a SQL operand for numeric TimeSpan comparison. SQL Server / Postgres /
        /// MySQL store TimeSpan in native TIME / INTERVAL types whose comparison operators
        /// already use numeric ordering - the default is identity. SQLite stores TimeSpan
        /// as canonical 'c' TEXT (<c>"d.hh:mm:ss.fffffff"</c>) and lex-compares, which
        /// silently mis-orders multi-day durations ("10.00:00:00" &lt; "9.23:59:59"
        /// lexicographically but 10 days &gt; 9 days 23 hours). SqliteProvider overrides
        /// to convert the column to fractional seconds via
        /// <see cref="SqliteProvider.TimeSpanColumnTotalSecondsSql"/>.
        /// </summary>
        public virtual string NormalizeTimeSpanForCompare(string sql) => sql;

        /// <summary>
        /// Wraps a SQL operand (a DateTimeOffset column/expression) so ORDER BY / DISTINCT / GROUP BY
        /// sort by the UTC instant, matching <see cref="DateTimeOffset"/>'s instant-based comparison
        /// in LINQ. SQL Server (native <c>datetimeoffset</c>), PostgreSQL (<c>timestamptz</c>) and
        /// MySQL (nORM stores a <c>DATETIME(6)</c> pre-normalised to UTC) already order by instant,
        /// so the default is identity — which also keeps any index on the column usable. SQLite
        /// stores DateTimeOffset as offset-suffixed TEXT, whose lexical order is NOT instant order
        /// when offsets differ, so SqliteProvider overrides to convert to the UTC epoch. The WHERE
        /// path already normalises DateTimeOffset comparisons; without this the ORDER BY silently
        /// disagreed with both WHERE and LINQ.
        /// </summary>
        public virtual string NormalizeDateTimeOffsetForCompare(string sql) => sql;

        /// <summary>
        /// The SQL operator for INTEGER division — C# <c>int / int</c> truncates toward zero, and
        /// SQLite / SQL Server / PostgreSQL <c>/</c> on integer operands does the same, so the
        /// default is <c>/</c>. MySQL's <c>/</c> always produces a DECIMAL (5/2 = 2.5), which
        /// silently changes predicate results and materialized values; it overrides with
        /// <c>DIV</c>, whose truncation toward zero matches C# for negative operands too.
        /// Only used when the expression's static type is integral; floating/decimal division
        /// keeps <c>/</c> everywhere.
        /// </summary>
        internal virtual string IntegerDivisionOperator => "/";

        /// <summary>
        /// True when <paramref name="t"/> (nullable-unwrapped) is an integral numeric type, i.e.
        /// C# arithmetic over it uses integer semantics (truncating division). Drives the
        /// <see cref="IntegerDivisionOperator"/> emit decision.
        /// </summary>
        internal static bool IsIntegralArithmeticType(Type t)
        {
            var u = Nullable.GetUnderlyingType(t) ?? t;
            return u == typeof(int) || u == typeof(long) || u == typeof(short) || u == typeof(sbyte)
                || u == typeof(byte) || u == typeof(ushort) || u == typeof(uint) || u == typeof(ulong);
        }

        /// <summary>
        /// True when the provider's DEFAULT column collation compares strings case-insensitively
        /// with <c>=</c> (MySQL utf8mb4_*_ci, SQL Server *_CI_AS), so translating C#'s ordinal
        /// string equality faithfully requires the sargable wrap below. SQLite (BINARY) and
        /// PostgreSQL are already ordinal, so the default is false and their emits are untouched.
        /// </summary>
        internal virtual bool DefaultStringEqualityIsCaseInsensitive => false;

        /// <summary>
        /// Sargable ordinal (case-sensitive) string equality: byte-equality implies
        /// collation-equality, so the plain <c>=</c> term narrows through any index on the column
        /// while the <see cref="ForceCaseSensitiveStringComparison"/> term filters the case
        /// variants exactly. Only emitted when
        /// <see cref="DefaultStringEqualityIsCaseInsensitive"/> is true.
        /// </summary>
        internal string OrdinalStringEqualSql(string left, string right)
            => $"({left} = {right} AND {ForceCaseSensitiveStringComparison(left)} = {right})";

        /// <summary>
        /// Wraps the operand of an AVG aggregate. C# Average over integral values returns a
        /// double (avg(1,2) = 1.5), but SQL Server's AVG(int) does integer division (1) — its
        /// override casts integral operands to FLOAT. The other providers already return
        /// fractional averages for integer inputs, so the default is identity.
        /// </summary>
        internal virtual string AverageAggregateOperand(string sql, Type operandClrType) => sql;

        /// <summary>
        /// Rewrites a DateTimeOffset GROUP BY key so same-instant values recorded at different
        /// offsets land in ONE group, matching .NET's instant-based DateTimeOffset equality.
        /// The result must be BOTH a valid grouping expression and a materializable
        /// DateTimeOffset text (it doubles as the selected key), so SQLite canonicalizes to
        /// UTC-rendered text rather than an epoch number. Null means the provider's native
        /// type already groups by instant (SQL Server datetimeoffset, PostgreSQL timestamptz,
        /// MySQL's UTC-normalized DATETIME).
        /// </summary>
        internal virtual string? CanonicalizeDateTimeOffsetGroupKey(string sql) => null;

        /// <summary>
        /// True when ORDER BY must emit an explicit null-rank for NULLABLE keys to match C#'s
        /// "null is smallest" ordering (nulls first ascending, last descending). PostgreSQL
        /// defaults to the opposite (NULLS LAST asc / NULLS FIRST desc); the other providers
        /// already sort nulls first ascending. The rank is a leading `(key IS NOT NULL)` entry
        /// sharing the key's direction, which stays correct when Reverse() flips directions.
        /// </summary>
        internal virtual bool RequiresExplicitNullOrderingForNullableKeys => false;

        /// <summary>
        /// A VALUE-PRESERVING wrap that makes a projected string expression compare with ordinal
        /// (binary) semantics in set operations (UNION / INTERSECT / EXCEPT dedup and matching).
        /// Unlike <see cref="ForceCaseSensitiveStringComparison"/>, the result must still
        /// materialize as a string (MySQL's bare <c>BINARY x</c> would come back as raw bytes).
        /// Null means the provider's set ops are already ordinal (SQLite, PostgreSQL).
        /// </summary>
        internal virtual string? OrdinalComparableStringProjection(string sql) => null;

        /// <summary>
        /// Ordinal string inequality, the negation of <see cref="OrdinalStringEqualSql"/>:
        /// collation-inequality implies byte-inequality (short-circuits), otherwise the binary
        /// term decides. Inequality is never index-sargable, so no narrowing term is needed.
        /// </summary>
        internal string OrdinalStringNotEqualSql(string left, string right)
            => $"({left} <> {right} OR {ForceCaseSensitiveStringComparison(left)} <> {right})";

        /// <summary>
        /// True when this provider's default <c>LIKE</c> cannot be forced case-sensitive with a
        /// collation modifier and the ordinal string-match path must instead bypass <c>LIKE</c>
        /// entirely (see <see cref="GetOrdinalStringMatchSql"/>). Only SQLite sets this: its
        /// <c>LIKE</c> folds ASCII case irrespective of collation. Default is false.
        /// </summary>
        internal virtual bool UsesOrdinalStringMatchBypass => false;

        /// <summary>
        /// Builds a case-sensitive (ordinal) Contains/StartsWith/EndsWith predicate WITHOUT
        /// <c>LIKE</c>, for providers where <see cref="UsesOrdinalStringMatchBypass"/> is true.
        /// <paramref name="patternSql"/> is the raw pattern SQL (a bound parameter or literal) with
        /// no wildcards or escaping. Only called when the bypass flag is set.
        /// </summary>
        internal virtual string GetOrdinalStringMatchSql(string columnSql, string patternSql, OrdinalStringMatch kind)
            => throw new NotSupportedException(
                $"Provider '{GetType().Name}' does not implement ordinal string-match bypass.");

        /// <summary>
        /// Builds a SQL predicate fragment that is TRUE when <paramref name="colSql"/> is
        /// NULL or an empty string. The default <c>(col IS NULL OR col = '')</c> works on
        /// every provider except SQL Server, where trailing spaces are insignificant in
        /// equality comparisons so <c>'   ' = ''</c> evaluates to TRUE. SQL Server must use
        /// <c>DATALENGTH(col) = 0</c> for a byte-exact empty-string test.
        /// </summary>
        public virtual string IsNullOrEmptySql(string colSql) =>
            $"({colSql} IS NULL OR {colSql} = '')";

        /// <summary>
        /// Formats a numeric SQL expression as a fixed-decimal text with exactly
        /// <paramref name="digits"/> fractional digits, matching .NET's
        /// <c>ToString("F{digits}")</c>. Each provider has a different primitive:
        /// SQLite <c>printf('%.Nf', x)</c>; SQL Server <c>FORMAT(x, 'FN', 'en-US')</c>;
        /// PostgreSQL <c>to_char(x, 'FM999999990.{N zeros}')</c>; MySQL strips the
        /// thousand-separators from <c>FORMAT(x, N)</c>.
        ///
        /// The default base implementation routes to <c>CAST({sql} AS VARCHAR)</c>
        /// which loses the fixed-decimal padding but produces valid SQL on any
        /// provider that doesn't override -- callers should override for
        /// correctness, not depend on the default for production formatting.
        /// </summary>
        public virtual string FormatFixedDecimalSql(string sql, int digits)
            => $"CAST({sql} AS VARCHAR)";

        /// <summary>
        /// Formats a DateTime/DateTimeOffset/DateOnly/TimeOnly SQL expression as
        /// text using a .NET custom date format string (e.g. "yyyy-MM-dd").
        /// SQLite uses <c>strftime('%Y-%m-%d', x)</c>; SQL Server's <c>FORMAT(x,
        /// 'yyyy-MM-dd', 'en-US')</c> accepts .NET-style patterns directly;
        /// PostgreSQL uses <c>to_char(x, 'YYYY-MM-DD')</c>; MySQL uses
        /// <c>DATE_FORMAT(x, '%Y-%m-%d')</c>. Returns null when the format is
        /// not supported by the provider (so callers can fall through).
        /// </summary>
        public virtual string? FormatDateUsingDotNetPattern(string sql, string dotNetFormat)
            => null;

        /// <summary>
        /// Adds a number of seconds (expressed as a SQL fragment, e.g. "3600"
        /// or "(CAST(substr(ts,1,2) AS INTEGER) * 3600 + ...)") to a DateTime
        /// SQL expression. Used by ETSV/SCV for `Stamp + TimeSpan.FromHours(1)`
        /// and constant-TimeSpan shift translation.
        ///
        /// SQLite: <c>RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', col, '+N seconds'), '0'), '.')</c>
        /// SQL Server: <c>DATEADD(SECOND, N, col)</c>
        /// PostgreSQL: <c>(col + (N || ' seconds')::interval)</c>
        /// MySQL: <c>DATE_ADD(col, INTERVAL N SECOND)</c>
        ///
        /// Default returns null so callers can fall through to client-eval.
        /// </summary>
        public virtual string? AddSecondsToDateTimeSql(string dateTimeSql, string secondsSqlFragment)
            => null;

        /// <summary>
        /// Returns SQL that converts a single-character expression to its
        /// integer code point. Used by ETSV's <c>char.IsPunctuation/IsSymbol/
        /// IsControl/GetNumericValue</c> handlers.
        /// SQLite: <c>unicode(c)</c>; SQL Server: <c>UNICODE(c)</c>;
        /// PostgreSQL: <c>ascii(c)</c>; MySQL: <c>ORD(c)</c>.
        /// </summary>
        public virtual string GetCharCodeSql(string charSql) => $"unicode({charSql})";

        /// <summary>
        /// Inverse of <see cref="GetCharCodeSql"/>: converts an integer code point
        /// to a single-character SQL expression. SQLite / SQL Server / MySQL use
        /// <c>CHAR(N)</c>; PostgreSQL uses <c>chr(N)</c>.
        /// </summary>
        public virtual string GetCharFromCodeSql(string codePointSql) => $"CHAR({codePointSql})";

        /// <summary>
        /// Adds N days (a SQL fragment / integer literal) to a DateOnly SQL
        /// expression. Default returns null so callers can fall through to
        /// the existing NormUnsupportedFeatureException pathway.
        ///
        /// SQLite: <c>strftime('%Y-%m-%d', col, '+N days')</c>
        /// SQL Server: <c>DATEADD(DAY, N, col)</c>
        /// PostgreSQL: <c>(col + N)</c> (date + int is native)
        /// MySQL: <c>DATE(DATE_ADD(col, INTERVAL N DAY))</c>
        ///   (DATE_ADD returns DATETIME; DATE() casts back to DATE so the
        ///   materializer reads a DateOnly-compatible value.)
        /// </summary>
        public virtual string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment) => null;

        /// <summary>
        /// Adds N months (a SQL fragment / integer literal) to a DateOnly SQL
        /// expression. Default returns null so callers can fall through.
        ///
        /// SQLite: <c>strftime('%Y-%m-%d', col, '+N months')</c>
        /// SQL Server: <c>DATEADD(MONTH, N, col)</c>
        /// PostgreSQL: <c>(col + N * INTERVAL '1 month')::date</c>
        /// MySQL: <c>DATE(DATE_ADD(col, INTERVAL N MONTH))</c>
        /// </summary>
        public virtual string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment) => null;

        /// <summary>
        /// Adds N years (a SQL fragment / integer literal) to a DateOnly SQL
        /// expression. Default returns null so callers can fall through.
        ///
        /// SQLite: <c>strftime('%Y-%m-%d', col, '+N years')</c>
        /// SQL Server: <c>DATEADD(YEAR, N, col)</c>
        /// PostgreSQL: <c>(col + N * INTERVAL '1 year')::date</c>
        /// MySQL: <c>DATE(DATE_ADD(col, INTERVAL N YEAR))</c>
        /// </summary>
        public virtual string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment) => null;

        /// <summary>
        /// Emits the per-provider aggregate that concatenates the elements of
        /// a group's string-typed column with a separator. Used to translate
        /// <c>g =&gt; string.Join(sep, g.Select(x =&gt; x.Member))</c> inside a
        /// <c>GroupBy</c> projection.
        ///
        /// SQLite: <c>GROUP_CONCAT(expr, sep)</c>
        /// SQL Server (2017+): <c>STRING_AGG(expr, sep)</c>
        /// PostgreSQL: <c>STRING_AGG(expr, sep)</c>
        /// MySQL: <c>GROUP_CONCAT(expr SEPARATOR sep)</c>
        ///
        /// Default routes to STRING_AGG which is portable across SqlServer and
        /// PostgreSQL; SQLite and MySQL override.
        /// </summary>
        public virtual string GetStringAggregateSql(string expr, string sepLiteral)
            => $"STRING_AGG({expr}, {sepLiteral})";

        /// <summary>
        /// Ordered variant: STRING_AGG with an ORDER BY clause inside the aggregate.
        /// SQL Server uses <c>WITHIN GROUP (ORDER BY expression)</c>; Postgres puts ORDER BY
        /// inside the function; MySQL differs - those providers override.
        /// </summary>
        public virtual string GetStringAggregateSql(string expr, string sepLiteral, string orderBySql)
            => $"STRING_AGG({expr}, {sepLiteral}) WITHIN GROUP (ORDER BY {orderBySql})";

        /// <summary>
        /// When true, the translator routes an ordered string-concat aggregate fold
        /// through <see cref="GetStringAggregateSql(string,string,string)"/> and
        /// removes the ORDER BY from the outer SELECT. When false (SQLite &lt; 3.44),
        /// the unordered aggregate form is used and the ORDER BY stays on the outer
        /// SELECT so the query planner's index selection determines GROUP_CONCAT order.
        /// </summary>
        public virtual bool SupportsNativeOrderedStringAggregate => true;

        /// <summary>
        /// Returns SQL that casts <paramref name="innerSql"/> to a boolean.
        /// Default emits <c>CAST(x AS BOOLEAN)</c> which works on Postgres and SQLite.
        /// SQL Server overrides to <c>BIT</c>; MySQL overrides to a comparison.
        /// </summary>
        public virtual string GetBoolCastSql(string innerSql)
            => $"CAST({innerSql} AS BOOLEAN)";

        /// <summary>
        /// Returns SQL that evaluates the .NET <c>Regex.IsMatch(input, pattern)</c>
        /// where <paramref name="patternLiteral"/> is the already-emitted SQL
        /// fragment for the pattern (typically a single-quoted literal or @-param).
        /// PostgreSQL: <c>input ~ pattern</c>.
        /// MySQL: <c>input REGEXP pattern</c>.
        /// SQLite: <c>input REGEXP pattern</c> (requires a user-registered REGEXP
        ///   function on the connection -- Microsoft.Data.Sqlite doesn't supply
        ///   one by default; emitting the operator is still the correct shape).
        /// SQL Server: throws -- T-SQL has no native regex primitive. A CLR
        ///   function with the same surface is the user-supplied workaround.
        /// </summary>
        public virtual string GetRegexMatchSql(string inputSql, string patternLiteral)
            => throw new NormUnsupportedFeatureException(
                $"Regex.IsMatch is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Returns SQL evaluating <c>Regex.Replace(input, pattern, replacement)</c>.
        /// PostgreSQL: <c>regexp_replace(input, pattern, replacement)</c>.
        /// MySQL (8.0+): <c>REGEXP_REPLACE(input, pattern, replacement)</c>.
        /// SQLite: <c>regexp_replace(input, pattern, replacement)</c> (requires a
        ///   user-registered function; Microsoft.Data.Sqlite has no built-in).
        /// SQL Server: throws -- no native regex primitive.
        /// </summary>
        public virtual string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => throw new NormUnsupportedFeatureException(
                $"Regex.Replace is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Whether a constant <c>Regex.IsMatch</c> / <c>Regex.Replace</c> pattern or replacement
        /// argument must be emitted as an inline SQL string literal instead of a parameter.
        /// Default is <c>false</c> (parameterise). SQL Server overrides it to <c>true</c> because
        /// it decodes the pattern literal at translation time to build a LIKE/PATINDEX shape, so
        /// it needs the literal text rather than a runtime parameter. Keeping this a provider
        /// capability (not an <c>is SqlServerProvider</c> check in the query layer) preserves the
        /// provider-mobility contract.
        /// </summary>
        internal virtual bool InlinesConstantRegexArguments => false;

        /// <summary>
        /// Case-insensitive variant of <see cref="GetRegexMatchSql"/>. Default
        /// falls back to wrapping both sides in LOWER() which is portable but
        /// loses Unicode case-folding nuance; providers with a native case-
        /// insensitive primitive override (e.g. PostgreSQL <c>~*</c>).
        /// </summary>
        public virtual string GetRegexMatchIgnoreCaseSql(string inputSql, string patternLiteral)
            => GetRegexMatchSql($"LOWER({inputSql})", $"LOWER({patternLiteral})");

        /// <summary>
        /// Case-insensitive variant of <see cref="GetRegexReplaceSql"/>. Default
        /// lowers both input and pattern; the replacement passes through
        /// unchanged (case is preserved on the replacement text). Providers
        /// with a flag-style override (PostgreSQL <c>regexp_replace(... 'gi')</c>,
        /// MySQL <c>REGEXP_REPLACE(... 'i')</c>) can supersede.
        /// </summary>
        public virtual string GetRegexReplaceIgnoreCaseSql(string inputSql, string patternLiteral, string replacementLiteral)
            => GetRegexReplaceSql($"LOWER({inputSql})", $"LOWER({patternLiteral})", replacementLiteral);

        /// <summary>
        /// Portable Gregorian leap-year predicate. Year divisible by 4 but
        /// not by 100, OR divisible by 400. Pure arithmetic; works on every
        /// provider.
        /// </summary>
        protected static string BuildIsLeapYearSql(string yearSql)
            => $"((({yearSql}) % 4 = 0 AND ({yearSql}) % 100 != 0) OR ({yearSql}) % 400 = 0)";

        /// <summary>
        /// Portable DateTime.DaysInMonth lookup. Month-length CASE with the
        /// leap-year exception on February. Pure SQL CASE; works on every
        /// provider.
        /// </summary>
        protected static string BuildDaysInMonthSql(string yearSql, string monthSql)
            => $"(CASE ({monthSql}) " +
               $"WHEN 1 THEN 31 WHEN 3 THEN 31 WHEN 5 THEN 31 WHEN 7 THEN 31 " +
               $"WHEN 8 THEN 31 WHEN 10 THEN 31 WHEN 12 THEN 31 " +
               $"WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30 " +
               $"WHEN 2 THEN (CASE WHEN ({yearSql}) % 4 = 0 AND (({yearSql}) % 100 != 0 OR ({yearSql}) % 400 = 0) THEN 29 ELSE 28 END) " +
               $"END)";

        /// <summary>
        /// Emits the per-provider DATETIME-from-parts primitive used to
        /// translate <c>new DateTime(year, month, day)</c> on column args.
        /// Default throws so each provider opts in with its native function.
        /// Hours / minutes / seconds default to 0 -- the 7-arg DateTime
        /// ctor variants are handled by the same hook with more SQL args.
        /// </summary>
        public virtual string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime(year, month, day) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// 6-arg DATETIME-from-parts variant covering
        /// <c>new DateTime(year, month, day, hour, minute, second)</c>. Default
        /// throws; each provider overrides with its native primitive.
        /// </summary>
        public virtual string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime(year, month, day, hour, minute, second) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// 7-arg DATETIME-from-parts variant covering
        /// <c>new DateTime(year, month, day, hour, minute, second, millisecond)</c>.
        /// Default throws; each provider overrides with its native primitive.
        /// </summary>
        public virtual string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime(year, month, day, hour, minute, second, millisecond) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Per-provider DATE-from-parts primitive for translating
        /// <c>new DateOnly(year, month, day)</c> with column args.
        /// </summary>
        public virtual string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => throw new NormUnsupportedFeatureException(
                $"DateOnly(year, month, day) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Per-provider TIME-from-parts primitive for translating
        /// <c>new TimeOnly(hour, minute, second)</c> with column args.
        /// </summary>
        public virtual string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeOnly(hour, minute, second) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// 4-arg TIME-from-parts variant covering
        /// <c>new TimeOnly(hour, minute, second, millisecond)</c>. Default
        /// throws; each provider overrides with its native primitive.
        /// </summary>
        public virtual string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeOnly(hour, minute, second, millisecond) with column args is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Adds N seconds (a SQL fragment) to a TimeOnly SQL expression.
        /// Default returns null so callers can fall through.
        ///
        /// SQLite: strftime over a fake-date-prefixed time text + 'N seconds'
        ///   modifier; result re-formatted to 'HH:mm:ss'.
        /// SQL Server: CAST(DATEADD(SECOND, N, col) AS TIME) -- DATEADD on
        ///   TIME promotes to DATETIME, the CAST brings it back to TIME.
        /// PostgreSQL: (col + (N || ' seconds')::interval) -- TIME + INTERVAL
        ///   = TIME natively.
        /// MySQL: ADDTIME(col, SEC_TO_TIME(N)) -- stays TIME.
        ///
        /// Sub-day arithmetic only; wrap-around past 24h is provider-specific
        /// (SQLite drops, SqlServer wraps via TIME range, etc.).
        /// </summary>
        public virtual string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment) => null;

        /// <summary>
        /// Adds (or subtracts) a TimeSpan-typed column to a TimeOnly SQL
        /// expression. Sister of AddTimeSpanColumnToDateTimeSql. Each provider
        /// uses its TIME storage primitive without parsing text.
        ///
        /// SQLite: extract seconds from TimeSpan-as-text via substr/CAST and
        ///   feed to AddSecondsToTimeOnlySql.
        /// SQL Server: CAST(DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', dur), time) AS TIME).
        /// PostgreSQL: (time +/- dur) -- TIME + INTERVAL is native.
        /// MySQL: ADDTIME / SUBTIME(time, dur) -- both stay TIME.
        /// </summary>
        public virtual string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract) => null;

        /// <summary>
        /// Adds (or subtracts when <paramref name="subtract"/>) a TimeSpan-typed
        /// column expression to a DateTime SQL expression. Differs from
        /// <see cref="AddSecondsToDateTimeSql"/> which takes a numeric seconds
        /// fragment; this one takes the TimeSpan column directly and the
        /// implementation handles the storage format (SQLite stores 'HH:mm:ss'
        /// text and parses; SqlServer / Postgres / MySQL have native
        /// TIME/INTERVAL types).
        ///
        /// SQLite: parse 'HH:mm:ss' substring via substr/CAST and feed to
        /// strftime modifier.
        /// SQL Server: <c>DATEADD(SECOND, [+/-]DATEDIFF(SECOND, '00:00:00', col), dt)</c>
        /// PostgreSQL: <c>(dt + col)</c> / <c>(dt - col)</c> (native interval)
        /// MySQL: <c>DATE_ADD(dt, INTERVAL [+/-]TIME_TO_SEC(col) SECOND)</c>
        ///
        /// Default returns null so callers can fall through.
        /// </summary>
        public virtual string? AddTimeSpanColumnToDateTimeSql(string dateTimeSql, string timeSpanColumnSql, bool subtract)
            => null;

        /// <summary>
        /// Sister of <see cref="AddTimeSpanColumnToDateTimeSql"/> for
        /// DateTimeOffset operands. Default delegates to the DateTime hook;
        /// providers that store DTOs as offset-suffixed text (SQLite) override
        /// to preserve the original offset on the result so the materialiser
        /// round-trips with the same wall-clock rendering rather than re-
        /// interpreting the suffixless result as Local.
        /// </summary>
        public virtual string? AddTimeSpanColumnToDateTimeOffsetSql(string dtoSql, string timeSpanColumnSql, bool subtract)
            => AddTimeSpanColumnToDateTimeSql(dtoSql, timeSpanColumnSql, subtract);

        /// <summary>
        /// Returns SQL that evaluates a TimeSpan column as fractional seconds (double).
        /// Used by the LINQ projection-side translator to lower
        /// <c>col1 + col2</c> / <c>col1 - col2</c> between two TimeSpan columns into
        /// a portable scalar; the materialiser reads the resulting double and
        /// reconstructs the TimeSpan via <c>TimeSpan.FromSeconds</c>.
        ///
        /// Default throws because providers store TimeSpan in dialect-specific
        /// formats (SQLite TEXT, SqlServer TIME(7), Postgres INTERVAL, MySQL TIME);
        /// each provider supplies its own emit.
        /// </summary>
        public virtual string GetTimeSpanColumnSecondsSql(string timeSpanColumnSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeSpan column arithmetic is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Returns SQL evaluating <paramref name="dtoSql"/> (a DateTimeOffset column or
        /// expression) re-rendered at <paramref name="offset"/>. Implements
        /// <see cref="DateTimeOffset.ToOffset(TimeSpan)"/>: the UTC instant is invariant,
        /// only the wall-clock representation and trailing offset suffix change.
        /// Default throws; each provider overrides with a path tailored to its
        /// storage shape (native DATETIMEOFFSET on SqlServer, ISO-8601 text
        /// elsewhere).
        /// </summary>
        public virtual string GetDateTimeOffsetWithOffsetSql(string dtoSql, TimeSpan offset)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset.ToOffset is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as the wall-clock DateTime at
        /// <paramref name="localOffset"/> - i.e. the value of
        /// <see cref="DateTimeOffset.LocalDateTime"/> for one concrete local
        /// offset. Query translation composes this provider hook inside a
        /// generated timezone-offset range CASE expression so each row uses the
        /// correct per-instant local offset where nORM owns the full expression.
        /// Result is the date+time portion only (no offset suffix), readable as
        /// a <see cref="DateTime"/> by the materialiser.
        /// </summary>
        public virtual string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset.LocalDateTime is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as the integer count of seconds
        /// since the Unix epoch (UTC). Kept as a provider extension point and as a
        /// fallback for custom providers; built-in DateTimeOffset equality and
        /// subtraction use the microsecond hook below.
        /// </summary>
        public virtual string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset UTC-instant comparison is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as milliseconds since the Unix
        /// epoch (UTC). Used internally by timezone-range lowering where transition
        /// boundaries do not need sub-millisecond precision.
        /// </summary>
        internal virtual string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"(({GetDateTimeOffsetUtcEpochSecondsSql(dtoSql)}) * 1000)";

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as microseconds since the Unix
        /// epoch (UTC). Built-in DateTimeOffset equality and subtraction use this
        /// as the common precision floor; 100ns .NET ticks are still not portable
        /// across all providers.
        /// </summary>
        internal virtual string GetDateTimeOffsetUtcEpochMicrosecondsSql(string dtoSql)
            => $"(({GetDateTimeOffsetUtcEpochMillisecondsSql(dtoSql)}) * 1000)";

        /// <summary>
        /// SQL evaluating the UTC-instant difference between two DateTimeOffset
        /// expressions as fractional seconds. Providers may override to force
        /// floating arithmetic where native decimal division would round away
        /// microsecond precision.
        /// </summary>
        internal virtual string GetDateTimeOffsetDifferenceSecondsSql(string leftSql, string rightSql)
            => $"(({GetDateTimeOffsetUtcEpochMicrosecondsSql(leftSql)} - {GetDateTimeOffsetUtcEpochMicrosecondsSql(rightSql)}) / 1000000.0)";

        /// <summary>
        /// Returns SQL that parses <paramref name="innerSql"/> (a textual expression) as a
        /// 32- or 64-bit signed integer. Used to translate <c>int.Parse(col)</c> /
        /// <c>long.Parse(col)</c>. Most providers accept ANSI <c>CAST(x AS INTEGER)</c>;
        /// MySQL requires <c>CAST(x AS SIGNED)</c> instead - override on the MySQL provider.
        /// </summary>
        public virtual string GetIntCastSql(string innerSql, bool asLong = false)
            => $"CAST({innerSql} AS {(asLong ? "BIGINT" : "INTEGER")})";

        /// <summary>
        /// Returns SQL that truncates a numeric expression toward zero and returns
        /// an integer. Used for TimeSpan component extraction where .NET truncates
        /// fractional totals before modulo arithmetic.
        /// </summary>
        public virtual string GetTruncateToIntSql(string numericSql)
            => $"CAST({numericSql} AS INTEGER)";

        /// <summary>
        /// Returns SQL that parses <paramref name="innerSql"/> as a floating-point /
        /// fixed-precision number. Used to translate <c>double.Parse(col)</c> and
        /// <c>decimal.Parse(col)</c>. Defaults to ANSI <c>CAST(x AS DOUBLE PRECISION)</c>
        /// for <paramref name="asDecimal"/>=false and <c>CAST(x AS DECIMAL(38, 10))</c>
        /// otherwise. Providers without DOUBLE PRECISION or with stricter DECIMAL
        /// syntax override.
        /// </summary>
        public virtual string GetRealCastSql(string innerSql, bool asDecimal = false)
            => asDecimal
                ? $"CAST({innerSql} AS DECIMAL(38, 10))"
                : $"CAST({innerSql} AS DOUBLE PRECISION)";

        /// <summary>
        /// Returns an INSERT statement for a join table row that does nothing (ignores) on duplicate key.
        /// Providers override this for their native upsert/ignore syntax.
        /// </summary>
        /// <param name="escTable">Escaped join table name.</param>
        /// <param name="escC1">Escaped left FK column name.</param>
        /// <param name="escC2">Escaped right FK column name.</param>
        /// <param name="p1">Parameter placeholder for the left FK value.</param>
        /// <param name="p2">Parameter placeholder for the right FK value.</param>
        public virtual string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT INTO {escTable} ({escC1}, {escC2}) SELECT {p1}, {p2} WHERE NOT EXISTS (SELECT 1 FROM {escTable} WHERE {escC1} = {p1} AND {escC2} = {p2})";

    }
}
