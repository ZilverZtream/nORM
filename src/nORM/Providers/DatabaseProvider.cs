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
    /// <summary>
    /// Base class for provider-specific behavior such as SQL generation, parameter
    /// creation and bulk operations. Concrete providers override members to adapt to
    /// the capabilities and quirks of a particular database system.
    /// </summary>
    public abstract partial class DatabaseProvider : IFastProvider
    {
        // S1/Save1 fix: cache key includes table and mapping shape to prevent cross-context
        // DML contamination when the same provider instance is shared across divergent mappings.
        private readonly ConcurrentLruCache<(Type Type, string TableName, string Operation, string Shape), string> _sqlCache = new(maxSize: 1000);

        /// <summary>Number of entities sampled for dynamic batch sizing heuristics.</summary>
        protected const int BatchSizingSampleCount = 100;

        /// <summary>
        /// Utility used to dynamically determine optimal batch sizes for bulk
        /// operations based on runtime metrics.
        /// </summary>
        protected static readonly DynamicBatchSizer BatchSizer = new();

        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        /// <summary>
        /// Gets the character used to prefix parameter names in SQL statements.
        /// </summary>
        public virtual char ParameterPrefixChar => '@';

        /// <summary>
        /// Gets the string representation of the parameter prefix used by the provider.
        /// </summary>
        public virtual string ParamPrefix => ParameterPrefixChar.ToString();

        /// <summary>SQL literal for boolean true ("1" for SQLite/SQL Server/MySQL; PostgreSQL overrides to "true").</summary>
        public virtual string BooleanTrueLiteral => "1";

        /// <summary>SQL literal for boolean false ("0" for SQLite/SQL Server/MySQL; PostgreSQL overrides to "false").</summary>
        public virtual string BooleanFalseLiteral => "0";

        /// <summary>
        /// Indicates whether this provider should prefer synchronous Read/ExecuteReader calls
        /// within async methods. True for providers like SQLite that don't support true async I/O,
        /// where async wrappers add pure overhead (~50-100ns per call).
        /// </summary>
        public virtual bool PrefersSyncExecution => false;

        /// <summary>
        /// Indicates whether small query fast paths should prefer synchronous reader execution
        /// while preserving the public async API. Defaults to <see cref="PrefersSyncExecution"/>
        /// so providers such as SQLite keep their existing behavior.
        /// </summary>
        internal virtual bool PrefersSyncFastPathExecution => PrefersSyncExecution;

        /// <summary>
        /// Indicates whether compiled query execution should prefer synchronous reader loops.
        /// This is intentionally separate from <see cref="PrefersSyncFastPathExecution"/> because
        /// provider prepared-command behavior can differ between ad-hoc fast paths and compiled
        /// query command pooling.
        /// </summary>
        internal virtual bool PrefersSyncCompiledQueryExecution => PrefersSyncExecution;

        /// <summary>
        /// Indicates whether general cached query-plan execution should prefer synchronous
        /// reader loops. Kept separate from fast-path and compiled execution because provider
        /// async overhead differs across prepared, pooled, and ad-hoc commands.
        /// </summary>
        internal virtual bool PrefersSyncQueryPlanExecution => PrefersSyncExecution;

        internal virtual bool SupportsQueryPlanPreparedCommandCache => false;

        internal virtual bool ParameterizeFastPathBooleanPredicates => false;

        internal virtual bool SupportsFastPathPreparedCommandCache => false;

        internal virtual bool SupportsCommandGeneratedKeyRetrieval => false;

        internal virtual object? GetCommandGeneratedKey(DbCommand command, TableMapping mapping) => null;

        /// <summary>
        /// MySQL rejects <c>DELETE FROM t WHERE pk IN (SELECT pk FROM t JOIN ...)</c> with
        /// "You can't specify target table for update in FROM clause". The fix is to wrap the
        /// inner subquery in another SELECT so the optimizer doesn't see a direct self-reference.
        /// All other providers handle the single-wrap IN subquery correctly.
        /// </summary>
        internal virtual bool CudWhereInSubqueryNeedsDoubleWrap => false;

        /// <summary>
        /// Returns true when the provider supports row-tuple comparison in WHERE IN, e.g.
        /// <c>WHERE (pk1, pk2) IN (SELECT a, b FROM ...)</c>. SQLite, PostgreSQL, and MySQL all
        /// support this syntax. SQL Server does not; composite-key CUD must use a JOIN-based form.
        /// </summary>
        internal virtual bool SupportsRowTupleComparison => true;

        /// <summary>
        /// Returns true when bare boolean predicates such as <c>WHERE IsActive</c> and
        /// <c>WHERE NOT IsActive</c> are valid and preferred over equality-to-literal forms.
        /// </summary>
        public virtual bool PrefersBareBooleanPredicates => false;

        /// <summary>
        /// Formats a SQL predicate that tests a non-nullable boolean expression for the expected value.
        /// </summary>
        public virtual string FormatBooleanPredicate(string expressionSql, bool expectedValue)
            => $"{expressionSql} = {(expectedValue ? BooleanTrueLiteral : BooleanFalseLiteral)}";

        /// <summary>
        /// Returns true when the provider uses TOP(n)/OFFSET-FETCH paging syntax (SQL Server style).
        /// Providers using LIMIT return false. Used by the fast-path query executor to emit the
        /// correct paging fragment without inspecting the provider type name.
        /// </summary>
        public virtual bool UsesFetchOffsetPaging => false;

        /// <summary>
        /// Generates a null-safe equality expression: TRUE when both sides are equal OR both are NULL.
        /// Providers can override for more efficient syntax (e.g., SQLite's <c>IS</c> operator).
        /// The default uses the portable OR-based expansion.
        /// </summary>
        public virtual string NullSafeEqual(string left, string right)
            => $"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))";

        /// <summary>
        /// Generates a null-safe inequality expression: TRUE when sides differ, including NULL vs non-NULL.
        /// Default: portable three-way expansion for full NULL correctness.
        /// </summary>
        public virtual string NullSafeNotEqual(string left, string right)
            => $"(({left} IS NOT NULL AND {right} IS NOT NULL AND {left} <> {right})" +
               $" OR ({left} IS NULL AND {right} IS NOT NULL)" +
               $" OR ({left} IS NOT NULL AND {right} IS NULL))";

        /// <summary>
        /// Maximum length of a single SQL statement supported by the provider.
        /// </summary>
        public virtual int MaxSqlLength => int.MaxValue;

        /// <summary>
        /// Maximum number of parameters allowed in a single command.
        /// </summary>
        public virtual int MaxParameters => int.MaxValue;

        /// <summary>
        /// Gets the provider capability descriptor used by startup validation,
        /// diagnostics and documentation.
        /// </summary>
        public virtual ProviderCapabilities Capabilities => new(
            GetType().Name,
            null,
            MaxParameters,
            false,
            false,
            false,
            true,
            "Generic provider capabilities are unknown.");

        /// <summary>
        /// Escapes an identifier (such as a table or column name) for inclusion in SQL statements.
        /// </summary>
        /// <param name="id">The identifier to escape.</param>
        /// <returns>The escaped identifier.</returns>
        public abstract string Escape(string id);

        /// <summary>
        /// Applies provider-specific paging clauses to the supplied SQL builder.
        /// </summary>
        /// <param name="sb">The builder containing the base SQL statement.</param>
        /// <param name="limit">The maximum number of rows to return.</param>
        /// <param name="offset">The number of rows to skip before starting to return rows.</param>
        /// <param name="limitParameterName">The parameter name used for the limit value.</param>
        /// <param name="offsetParameterName">The parameter name used for the offset value.</param>
        public abstract void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName);

        /// <summary>
        /// Returns SQL that retrieves the identity value generated for an inserted row.
        /// </summary>
        /// <param name="m">The mapping for the table being inserted into.</param>
        /// <returns>A SQL fragment that retrieves the generated identity.</returns>
        public abstract string GetIdentityRetrievalString(TableMapping m);

        /// <summary>
        /// Returns a SQL clause injected between the column list and <c>VALUES</c> to capture
        /// generated key values inline (e.g. <c>OUTPUT INSERTED.[col]</c> on SQL Server).
        /// The default implementation returns an empty string (suffix-based retrieval is used instead).
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <returns>SQL clause placed between <c>({cols})</c> and <c>VALUES</c>, or empty.</returns>
        public virtual string GetIdentityRetrievalPrefix(TableMapping m) => string.Empty;

        /// <summary>
        /// Creates a database parameter with the given name and value.
        /// </summary>
        /// <param name="name">The parameter name, including prefix.</param>
        /// <param name="value">The parameter value.</param>
        /// <returns>A parameter configured for the underlying provider.</returns>
        public abstract DbParameter CreateParameter(string name, object? value);

        /// <summary>
        /// Translates a .NET method invocation into its SQL equivalent for the provider.
        /// </summary>
        /// <param name="name">The name of the method being translated.</param>
        /// <param name="declaringType">The type that declares the method.</param>
        /// <param name="args">The SQL arguments to the function.</param>
        /// <returns>The translated SQL expression, or <c>null</c> if the method is not supported.</returns>
        public abstract string? TranslateFunction(string name, Type declaringType, params string[] args);

        /// <summary>
        /// Overload-aware translation hook with access to the original
        /// <see cref="System.Linq.Expressions.MethodCallExpression"/>. Called by the visitors BEFORE
        /// <see cref="TranslateFunction"/> so providers can dispatch on a
        /// method's full signature (parameter types, enum constants) when the
        /// stringified args alone are ambiguous -- e.g. distinguishing
        /// <c>Math.Round(x, MidpointRounding)</c> from <c>Math.Round(x, int)</c>,
        /// which have the same arity but different semantics. Default returns
        /// null so providers without overload-specific handling fall through to
        /// the existing <see cref="TranslateFunction"/> path unchanged.
        /// </summary>
        public virtual string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args) => null;

        /// <summary>
        /// Shared helper for non-SQLite providers' Math.Round / decimal.Round
        /// with MidpointRounding handling. SQLite has its own override because
        /// its default ROUND is AwayFromZero rather than banker's, and the
        /// emit shapes diverge enough that sharing is awkward. Providers that
        /// call this supply the truncate-toward-zero primitive and any
        /// AwayFromZero override; the rest (FLOOR / CEILING / ToEven via
        /// formula) is shared.
        /// </summary>
        protected string? TryTranslateMathRoundWithMode(
            System.Linq.Expressions.MethodCallExpression node,
            string[] args,
            Func<string, string?, string> awayFromZero,
            Func<string, string?, string> truncateTowardZero,
            string integerCastType = "BIGINT")
        {
            var declType = node.Method.DeclaringType;
            if (!((declType == typeof(Math) && node.Method.Name == nameof(Math.Round))
                  || (declType == typeof(decimal) && node.Method.Name == nameof(decimal.Round))))
                return null;
            var ps = node.Method.GetParameters();
            System.MidpointRounding mode = System.MidpointRounding.ToEven; // .NET default
            string? digitsArg = null;
            if (ps.Length == 2 && ps[1].ParameterType == typeof(System.MidpointRounding))
            {
                if (node.Arguments[1] is System.Linq.Expressions.ConstantExpression c1 && c1.Value is System.MidpointRounding m1)
                    mode = m1;
            }
            else if (ps.Length == 2)
            {
                digitsArg = args[1];
            }
            else if (ps.Length == 3)
            {
                digitsArg = args[1];
                if (node.Arguments[2] is System.Linq.Expressions.ConstantExpression c2 && c2.Value is System.MidpointRounding m2)
                    mode = m2;
            }
            var x = args[0];
            string scaled = digitsArg == null ? x : $"({x} * POWER(10.0, {digitsArg}))";
            string unscale(string s) => digitsArg == null ? s : $"({s} / POWER(10.0, {digitsArg}))";
            switch (mode)
            {
                case System.MidpointRounding.AwayFromZero:
                    return awayFromZero(x, digitsArg);
                case System.MidpointRounding.ToZero:
                    return truncateTowardZero(x, digitsArg);
                case System.MidpointRounding.ToNegativeInfinity:
                    return unscale($"FLOOR({scaled})");
                case System.MidpointRounding.ToPositiveInfinity:
                    return unscale($"CEILING({scaled})");
                case System.MidpointRounding.ToEven:
                default:
                    // Banker's: integer-part-via-truncate plus +1 only when the
                    // half-tie's integer part is odd. Sign reapplied via the
                    // leading CASE so negatives round symmetrically.
                    return unscale(
                        $"((CASE WHEN {scaled} >= 0 THEN 1 ELSE -1 END) * " +
                        $"(FLOOR(ABS({scaled})) + " +
                        $"CASE " +
                        $"WHEN ABS({scaled}) - FLOOR(ABS({scaled})) > 0.5 THEN 1 " +
                        $"WHEN ABS({scaled}) - FLOOR(ABS({scaled})) < 0.5 THEN 0 " +
                        $"ELSE (CAST(FLOOR(ABS({scaled})) AS {integerCastType}) % 2) END))");
            }
        }

        /// <summary>
        /// Builds SQL for <c>new DateTimeOffset(year, month, day, hour, minute, second, offset)</c>
        /// when at least one of the date/time parts is a column expression and the
        /// <paramref name="offset"/> is a compile-time constant. Producing canonical
        /// ISO-8601 text (<c>yyyy-MM-ddTHH:mm:ss+/-HH:MM</c>) lets the materialiser
        /// route through <c>DateTimeOffset.Parse</c> uniformly across providers.
        ///
        /// Default implementation uses CASE-based zero-padding so it works on every
        /// SQL dialect; providers can override with a native faster path when one
        /// exists (e.g. SqlServer's DATETIMEOFFSETFROMPARTS).
        /// </summary>
        public virtual string GetDateTimeOffsetFromPartsSql(
            string ySql, string mSql, string dSql,
            string hSql, string miSql, string sSql,
            TimeSpan offset)
        {
            var suffix = FormatOffsetSuffix(offset);
            string PadNum(string sql, int width)
            {
                var castSql = CastToVarchar(sql, width);
                return width == 4
                    ? $"(CASE WHEN ({sql}) < 10 THEN {GetConcatSql("'000'", castSql)} WHEN ({sql}) < 100 THEN {GetConcatSql("'00'", castSql)} WHEN ({sql}) < 1000 THEN {GetConcatSql("'0'", castSql)} ELSE {castSql} END)"
                    : $"(CASE WHEN ({sql}) < 10 THEN {GetConcatSql("'0'", castSql)} ELSE {castSql} END)";
            }
            return Concat(
                PadNum(ySql, 4), "'-'",
                PadNum(mSql, 2), "'-'",
                PadNum(dSql, 2), "'T'",
                PadNum(hSql, 2), "':'",
                PadNum(miSql, 2), "':'",
                PadNum(sSql, 2),
                $"'{suffix}'");
        }

        /// <summary>
        /// Cast a numeric expression to the provider's variable-length string type
        /// for concatenation in <see cref="GetDateTimeOffsetFromPartsSql"/>.
        /// Defaults to ANSI <c>VARCHAR(N)</c>; providers without VARCHAR override.
        /// </summary>
        protected virtual string CastToVarchar(string sql, int width)
            => $"CAST(({sql}) AS VARCHAR({width}))";

        /// <summary>
        /// Reduces an arbitrary-length sequence of SQL fragments via the provider's
        /// <see cref="GetConcatSql"/>. Used by helpers that need to compose many
        /// padded numeric parts into a single canonical text expression.
        /// </summary>
        protected string Concat(params string[] parts)
        {
            if (parts.Length == 0) return "''";
            if (parts.Length == 1) return parts[0];
            var result = parts[0];
            for (int i = 1; i < parts.Length; i++)
                result = GetConcatSql(result, parts[i]);
            return result;
        }

        /// <summary>
        /// Renders a <see cref="TimeSpan"/> offset as the trailing <c>+/-HH:MM</c>
        /// suffix that <see cref="System.DateTimeOffset.Parse(string, System.IFormatProvider)"/>
        /// accepts.
        /// </summary>
        protected static string FormatOffsetSuffix(TimeSpan offset)
        {
            var sign = offset.Ticks >= 0 ? '+' : '-';
            var abs = offset.Duration();
            return string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"{sign}{abs.Hours:D2}:{abs.Minutes:D2}");
        }

        /// <summary>
        /// Shared helper for non-SQLite providers' <c>TimeSpan.From*</c> static
        /// factories applied to a column expression (or any non-constant arg).
        /// Emits REAL seconds; the materializer's
        /// <c>double ? TimeSpan.FromSeconds</c> path reconstructs the TimeSpan.
        ///
        /// SQLite has its own override that emits canonical 'c' format TEXT
        /// because Microsoft.Data.Sqlite reads TimeSpan via TimeSpan.Parse;
        /// the seconds-as-double shape used here would force a lossy parse.
        /// </summary>
        protected static string? TryTranslateTimeSpanFactory(
            System.Linq.Expressions.MethodCallExpression node,
            string[] args)
        {
            if (node.Method.DeclaringType != typeof(TimeSpan) || node.Object != null || args.Length != 1)
                return null;
            return node.Method.Name switch
            {
                nameof(TimeSpan.FromDays)         => $"(({args[0]}) * 86400.0)",
                nameof(TimeSpan.FromHours)        => $"(({args[0]}) * 3600.0)",
                nameof(TimeSpan.FromMinutes)      => $"(({args[0]}) * 60.0)",
                nameof(TimeSpan.FromSeconds)      => $"(({args[0]}) * 1.0)",
                nameof(TimeSpan.FromMilliseconds) => $"(({args[0]}) / 1000.0)",
                nameof(TimeSpan.FromTicks)        => $"(({args[0]}) / 10000000.0)",
                _ => null
            };
        }

        /// <summary>
        /// Returns SQL that evaluates `end - start` as a fractional number of seconds. Used
        /// by the LINQ translator to lower `(end - start).TotalSeconds / TotalMinutes /
        /// TotalHours / Days / etc.` to a portable scalar expression. Both arguments are
        /// already-translated SQL fragments.
        /// </summary>
        public virtual string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime subtraction is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Returns SQL evaluating `end - start` (both TimeOnly) as fractional seconds
        /// wrapped to the half-open interval [0, 86400). Matches .NET's
        /// <c>TimeOnly.op_Subtraction</c>, which adds 24h to the raw difference when
        /// the result would be negative (so 01:00 - 23:00 yields +02:00, not -22:00).
        /// The materializer converts the REAL seconds back to <see cref="TimeSpan"/>
        /// via <c>TimeSpan.FromSeconds</c> -- same path used by DateTime subtraction.
        /// Default throws so each provider must opt in.
        /// </summary>
        public virtual string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
            => throw new NormUnsupportedFeatureException(
                $"TimeOnly subtraction is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// Translates a JSON path access expression for the provider.
        /// </summary>
        /// <param name="columnName">The name of the JSON column.</param>
        /// <param name="jsonPath">The JSON path to access within the column.</param>
        /// <returns>The SQL fragment that accesses the specified JSON path.</returns>
        public abstract string TranslateJsonPathAccess(string columnName, string jsonPath);

        /// <summary>
        /// Validates a raw JSON path string for SQL-injection-capable characters.
        /// The LINQ path performs this check before calling <see cref="TranslateJsonPathAccess"/>,
        /// but callers who invoke the method directly on the provider API must also be protected.
        /// </summary>
        /// <param name="jsonPath">The path to validate.</param>
        /// <exception cref="ArgumentException">Thrown when the path contains illegal characters or is too long.</exception>
        protected static void ValidateJsonPath(string jsonPath)
        {
            const int MaxLength = 500;
            if (jsonPath.Length > MaxLength)
                throw new ArgumentException(
                    $"JSON path exceeds maximum length of {MaxLength} characters (actual: {jsonPath.Length}).",
                    nameof(jsonPath));

            // Block characters that can escape an enclosing SQL string literal or open new statements.
            if (jsonPath.IndexOfAny(new[] { '\'', '"', ';', '\\' }) >= 0)
                throw new ArgumentException(
                    "JSON path contains an illegal character. Paths must not contain single-quote, double-quote, semicolon, or backslash.",
                    nameof(jsonPath));

            foreach (char ch in jsonPath)
                if (ch < 0x20)
                    throw new ArgumentException(
                        $"JSON path contains an illegal control character (U+{(int)ch:X4}).",
                        nameof(jsonPath));
        }

    }
}
