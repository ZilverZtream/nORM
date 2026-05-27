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

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Base class for provider-specific behavior such as SQL generation, parameter
    /// creation and bulk operations. Concrete providers override members to adapt to
    /// the capabilities and quirks of a particular database system.
    /// </summary>
    public abstract class DatabaseProvider : IFastProvider
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
        /// ISO-8601 text (<c>yyyy-MM-ddTHH:mm:ss±HH:MM</c>) lets the materialiser
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
        /// Renders a <see cref="TimeSpan"/> offset as the trailing <c>±HH:MM</c>
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
        /// <c>double → TimeSpan.FromSeconds</c> path reconstructs the TimeSpan.
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

        /// <summary>
        /// Describes a column as it actually exists in the live database.
        /// Used by <see cref="IntrospectTableColumnsAsync"/> and
        /// <see cref="GenerateCreateHistoryTableSql"/> to match history-table column
        /// types to the main table rather than deriving them from CLR defaults.
        /// </summary>
        public record LiveColumnInfo(string Name, string SqlType, bool IsNullable);

        /// <summary>
        /// Introspects the live column definitions for the named table.
        /// Returns an empty list when the table does not yet exist, allowing callers
        /// to fall back to CLR-default type mapping.
        /// Providers override this to use their native schema-inspection facilities.
        /// </summary>
        public virtual Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<LiveColumnInfo>>(Array.Empty<LiveColumnInfo>());

        /// <summary>
        /// Generates the SQL required to create a history table for temporal table support.
        /// When <paramref name="liveColumns"/> is supplied, column types are taken from the live
        /// DB schema rather than derived from CLR property types, ensuring the history table
        /// mirrors any custom precision/length settings on the main table.
        /// </summary>
        /// <param name="mapping">The table mapping representing the entity.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use CLR defaults.</param>
        /// <returns>The SQL statement that creates the history table.</returns>
        public abstract string GenerateCreateHistoryTableSql(
            TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null);

        /// <summary>
        /// Generates the SQL required to create triggers for maintaining the temporal history table.
        /// </summary>
        /// <param name="mapping">The table mapping representing the entity.</param>
        /// <returns>The SQL script containing the trigger definitions.</returns>
        public abstract string GenerateTemporalTriggersSql(TableMapping mapping);

        /// <summary>
        /// Returns provider-specific SQL to create the temporal tags table if it does not exist.
        /// Default uses IF NOT EXISTS syntax with TEXT column types (SQLite/MySQL/Postgres).
        /// SQL Server overrides this to use OBJECT_ID check and NVARCHAR/DATETIME2 types.
        /// </summary>
        public virtual string GetCreateTagsTableSql()
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"CREATE TABLE IF NOT EXISTS {table} ({tagCol} TEXT NOT NULL, {tsCol} TEXT NOT NULL, PRIMARY KEY ({tagCol}))";
        }

        /// <summary>
        /// Returns provider-specific SQL to probe for the existence of a history table.
        /// Default uses SELECT 1 … LIMIT 1 (SQLite/MySQL/Postgres).
        /// SQL Server overrides this to use SELECT TOP 1.
        /// </summary>
        /// <param name="escapedHistoryTable">The already-escaped history table name.</param>
        public virtual string GetHistoryTableExistsProbeSql(string escapedHistoryTable)
            => $"SELECT 1 FROM {escapedHistoryTable} LIMIT 1";

        /// <summary>
        /// Returns true when the DbException definitively indicates a table/object
        /// does not exist (schema error), as opposed to a permission denied or connectivity error.
        /// Only return true for definitive "object not found" schema errors so that operational
        /// failures propagate rather than being silently swallowed.
        /// </summary>
        public virtual bool IsObjectNotFoundError(DbException ex)
        {
            // Default: message-based fallback for providers without typed exception support.
            var msg = ex.Message;
            return msg.Contains("no such table", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("Invalid object name", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("relation", StringComparison.OrdinalIgnoreCase) && msg.Contains("does not exist", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns provider-specific SQL to look up a temporal tag's timestamp.
        /// All identifiers are escaped using the provider's Escape method.
        /// </summary>
        /// <param name="paramName">The already-prefixed parameter name for the tag name value.</param>
        public virtual string GetTagLookupSql(string paramName)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"SELECT {tsCol} FROM {table} WHERE {tagCol} = {paramName}";
        }

        /// <summary>
        /// Returns provider-specific SQL to insert a temporal tag record.
        /// All identifiers are escaped using the provider's Escape method.
        /// </summary>
        /// <param name="pTagName">The already-prefixed parameter name for the tag name value.</param>
        /// <param name="pTimestamp">The already-prefixed parameter name for the timestamp value.</param>
        public virtual string GetCreateTagSql(string pTagName, string pTimestamp)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"INSERT INTO {table} ({tagCol}, {tsCol}) VALUES ({pTagName}, {pTimestamp})";
        }

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
        /// Returns SQL that converts <paramref name="innerSql"/> to its textual representation —
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
        /// Returns SQL that XORs two integer expressions. SQL Server and MySQL accept the
        /// `^` operator; PostgreSQL uses `#`; SQLite has no XOR operator and falls back to
        /// `(a | b) - (a &amp; b)` — algebraically equivalent on integers.
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
        /// Wraps a SQL operand for numeric TimeSpan comparison. SQL Server / Postgres /
        /// MySQL store TimeSpan in native TIME / INTERVAL types whose comparison operators
        /// already use numeric ordering — the default is identity. SQLite stores TimeSpan
        /// as canonical 'c' TEXT (<c>"d.hh:mm:ss.fffffff"</c>) and lex-compares, which
        /// silently mis-orders multi-day durations ("10.00:00:00" &lt; "9.23:59:59"
        /// lexicographically but 10 days &gt; 9 days 23 hours). SqliteProvider overrides
        /// to convert the column to fractional seconds via
        /// <see cref="SqliteProvider.TimeSpanColumnTotalSecondsSql"/>.
        /// </summary>
        public virtual string NormalizeTimeSpanForCompare(string sql) => sql;

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
        /// SQL Server uses <c>WITHIN GROUP (ORDER BY …)</c>; Postgres puts ORDER BY
        /// inside the function; MySQL differs — those providers override.
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
        /// <paramref name="localOffset"/> — i.e. the value of
        /// <see cref="DateTimeOffset.LocalDateTime"/> with the local offset baked
        /// in at query-build time. Result is the date+time portion only (no offset
        /// suffix), readable as a <see cref="DateTime"/> by the materialiser.
        /// </summary>
        public virtual string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset.LocalDateTime is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as the integer count of seconds
        /// since the Unix epoch (UTC). Kept as a provider extension point and as a
        /// fallback for custom providers; built-in DateTimeOffset equality and
        /// subtraction use the millisecond hook below.
        /// </summary>
        public virtual string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTimeOffset UTC-instant comparison is not supported by provider '{GetType().Name}'.");

        /// <summary>
        /// SQL evaluating <paramref name="dtoSql"/> as milliseconds since the Unix
        /// epoch (UTC). Used internally for DateTimeOffset equality/subtraction
        /// where second-resolution is too coarse but full .NET tick precision is
        /// not portable across the built-in providers.
        /// </summary>
        internal virtual string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"(({GetDateTimeOffsetUtcEpochSecondsSql(dtoSql)}) * 1000)";

        /// <summary>
        /// Returns SQL that parses <paramref name="innerSql"/> (a textual expression) as a
        /// 32- or 64-bit signed integer. Used to translate <c>int.Parse(col)</c> /
        /// <c>long.Parse(col)</c>. Most providers accept ANSI <c>CAST(x AS INTEGER)</c>;
        /// MySQL requires <c>CAST(x AS SIGNED)</c> instead — override on the MySQL provider.
        /// </summary>
        public virtual string GetIntCastSql(string innerSql, bool asLong = false)
            => $"CAST({innerSql} AS {(asLong ? "BIGINT" : "INTEGER")})";

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

        /// <summary>
        /// Returns <c>true</c> when the ADO.NET driver reports <em>affected</em> (changed) rows
        /// rather than <em>matched</em> rows in response to UPDATE/DELETE.
        ///
        /// <para>
        /// <b>Optimistic-concurrency contract for affected-row providers (e.g. MySQL with default
        /// <c>useAffectedRows=true</c> in the connection string):</b>
        /// The <c>[Timestamp]</c> / row-version feature relies on the driver returning the number of
        /// rows that matched the WHERE clause so that a zero result can signal a stale-row conflict.
        /// Affected-row drivers return 0 even for a successful same-value update (no bytes changed),
        /// which would produce a false-positive <see cref="DbConcurrencyException"/>. To prevent this,
        /// nORM skips the rowcount conflict check for such providers. This means that on an
        /// affected-row provider a stale-row conflict is <b>not detected</b> when a concurrent writer
        /// updates the token to the same value that the current session is writing.
        /// </para>
        ///
        /// <para>
        /// For strict optimistic-concurrency guarantees on MySQL, use the connection-string option
        /// <c>useAffectedRows=false</c>, which switches MySQL Connector/NET to report matched rows.
        /// When <c>useAffectedRows=false</c>, override this property to return <c>false</c> so that
        /// nORM can perform the rowcount check normally.
        /// </para>
        /// </summary>
        internal virtual bool UseAffectedRowsSemantics => false;

        /// <summary>
        /// Ensures the provided connection is open before executing provider operations.
        /// </summary>
        /// <param name="connection">The connection to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown if the connection is not open.</exception>
        protected virtual void ValidateConnection(DbConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                var safeConnStr = NormValidator.MaskSensitiveConnectionStringData(connection.ConnectionString);
                throw new InvalidOperationException($"Connection must be open for {GetType().Name}. Connection: {safeConnStr}");
            }
        }

        /// <summary>
        /// Validates that a parameter name uses the provider's expected prefix.
        /// </summary>
        /// <param name="parameterName">Name to validate.</param>
        /// <param name="argumentName">Name of the argument being validated.</param>
        /// <exception cref="ArgumentException">Thrown when the prefix is missing.</exception>
        protected void EnsureValidParameterName(string? parameterName, string argumentName)
        {
            if (parameterName == null) return;
            // Translators may pass a composed paging expression like `(@p0 - @p1)` or
            // `(@p0 + 5)` when chaining Take/Skip/ElementAt; allow either a bare parameter
            // reference OR an expression that starts with an opening paren and contains a
            // parameter reference. Reject anything else to keep raw SQL injection out of
            // ApplyPaging.
            if (parameterName.StartsWith(ParamPrefix, StringComparison.Ordinal))
                return;
            if (parameterName.Length > 0 && parameterName[0] == '(' && parameterName.Contains(ParamPrefix, StringComparison.Ordinal))
                return;
            throw new ArgumentException($"Parameter name must start with '{ParamPrefix}' or be a translator-built composite expression containing one.", argumentName);
        }

        /// <summary>
        /// Determines whether the provider can operate in the current environment. The
        /// base implementation simply returns <c>true</c> but derived providers may
        /// perform runtime checks for required assemblies or database availability.
        /// </summary>
        public virtual Task<bool> IsAvailableAsync()
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Creates a database savepoint within the given transaction. The default
        /// implementation throws as savepoints are provider specific and may not be
        /// supported.
        /// </summary>
        /// <param name="transaction">The transaction in which to create the savepoint.</param>
        /// <param name="name">Name of the savepoint.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Rolls the specified transaction back to a previously created savepoint. The
        /// default implementation throws as savepoints are provider specific.
        /// </summary>
        /// <param name="transaction">The active transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Performs provider-specific initialization when a connection is opened.
        /// </summary>
        /// <param name="connection">The open connection to initialize.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        {
            ValidateConnection(connection);
            await ValidateServerVersionAsync(connection, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronous counterpart to <see cref="InitializeConnectionAsync"/> allowing
        /// providers to perform connection initialization without asynchronous overhead.
        /// </summary>
        /// <param name="connection">The open connection to initialize.</param>
        public virtual void InitializeConnection(DbConnection connection)
        {
            ValidateConnection(connection);
            ValidateServerVersion(connection);
        }

        /// <summary>
        /// Reads the connected database server version as provider-specific text.
        /// Providers with a minimum-version contract override this method.
        /// </summary>
        protected virtual Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
            => Task.FromResult<string?>(null);

        /// <summary>
        /// Reads the connected database server version synchronously as provider-specific text.
        /// Providers with a minimum-version contract override this method.
        /// </summary>
        protected virtual string? GetServerVersionString(DbConnection connection) => null;

        /// <summary>
        /// Parses a provider version string into a comparable <see cref="Version"/>.
        /// </summary>
        protected virtual Version? ParseServerVersion(string? versionText)
        {
            if (string.IsNullOrWhiteSpace(versionText))
                return null;

            var text = versionText.Trim();
            var start = -1;
            for (var i = 0; i < text.Length; i++)
            {
                if (char.IsDigit(text[i]))
                {
                    start = i;
                    break;
                }
            }

            if (start < 0)
                return null;

            var end = start;
            while (end < text.Length && (char.IsDigit(text[end]) || text[end] == '.'))
                end++;

            var candidate = text[start..end].Trim('.');
            if (candidate.Length == 0)
                return null;

            var parts = candidate.Split('.', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 1)
                candidate += ".0";

            return Version.TryParse(candidate, out var version) ? version : null;
        }

        private async Task ValidateServerVersionAsync(DbConnection connection, CancellationToken ct)
        {
            var minimum = Capabilities.MinimumServerVersion;
            if (minimum == null)
                return;

            var versionText = await GetServerVersionStringAsync(connection, ct).ConfigureAwait(false);
            ValidateServerVersionText(versionText, minimum);
        }

        private void ValidateServerVersion(DbConnection connection)
        {
            var minimum = Capabilities.MinimumServerVersion;
            if (minimum == null)
                return;

            var versionText = GetServerVersionString(connection);
            ValidateServerVersionText(versionText, minimum);
        }

        private void ValidateServerVersionText(string? versionText, Version minimum)
        {
            var actual = ParseServerVersion(versionText);
            if (actual == null)
                throw new NormConfigurationException(
                    $"{Capabilities.ProviderName} server version could not be determined during provider startup validation.");

            if (actual < minimum)
                throw new NormConfigurationException(
                    $"{Capabilities.ProviderName} server version {actual} is not supported. Minimum supported version is {minimum}.");
        }

        /// <summary>
        /// Gets the <see cref="CommandType"/> used when executing stored procedures for
        /// the provider. Providers that emulate procedures via text commands can
        /// override this to return <see cref="CommandType.Text"/>.
        /// </summary>
        public virtual CommandType StoredProcedureCommandType => CommandType.StoredProcedure;

        /// <summary>
        /// Constructs a minimal <c>SELECT</c> statement directly into the provided
        /// character buffer to avoid intermediate string allocations.
        /// </summary>
        /// <param name="buffer">Destination buffer that receives the generated SQL.</param>
        /// <param name="table">Name of the table to select from.</param>
        /// <param name="columns">Comma separated list of columns to select.</param>
        /// <param name="length">Outputs the number of characters written to the buffer.</param>
        public virtual void BuildSimpleSelect(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns, out int length)
        {
            length = BuildSimpleSelectSlow(buffer, table, columns);
        }

        /// <summary>
        /// Fallback implementation of <see cref="BuildSimpleSelect"/> that uses string
        /// concatenation. Providers can override <see cref="BuildSimpleSelect"/> to
        /// supply more efficient implementations.
        /// </summary>
        protected virtual int BuildSimpleSelectSlow(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns)
        {
            var sql = string.Concat("SELECT ", columns.ToString(), " FROM ", table.ToString());
            sql.AsSpan().CopyTo(buffer);
            return sql.Length;
        }

        /// <summary>
        /// Builds an SQL <c>IN</c> clause for the specified column and parameter values.
        /// Each value is added as a parameter to the provided command to guard against
        /// SQL injection.
        /// </summary>
        /// <param name="cmd">The command that will execute the generated SQL.</param>
        /// <param name="columnName">Column to apply the <c>IN</c> filter to.</param>
        /// <param name="values">Values to include in the <c>IN</c> list.</param>
        /// <returns>SQL fragment representing the <c>IN</c> clause.</returns>
        public virtual string BuildContainsClause(DbCommand cmd, string columnName, IReadOnlyList<object?> values)
        {
            // Empty collection: IN () is not valid SQL. Emit a never-true predicate instead.
            if (values.Count == 0)
                return "(1=0)";

            var paramNames = new List<string>(values.Count);
            for (int i = 0; i < values.Count; i++)
            {
                var pn = $"{ParamPrefix}p{i}";
                cmd.AddParam(pn, values[i]);
                paramNames.Add(pn);
            }
            return $"{columnName} IN ({string.Join(",", paramNames)})";
        }

        /// <summary>
        /// Indicates whether the transaction log is close to capacity and bulk
        /// operations should be throttled. Base implementation always returns
        /// <c>false</c>.
        /// </summary>
        protected virtual Task<bool> IsTransactionLogNearCapacityAsync(DbContext ctx, CancellationToken ct)
            => Task.FromResult(false);

        #region Bulk Operations (Abstract & Fallback)
        /// <summary>
        /// Inserts a large collection of entities into the database in batches.
        /// The method dynamically tunes batch size using <see cref="DynamicBatchSizer"/>
        /// to balance throughput with resource consumption and transaction log pressure.
        /// </summary>
        /// <typeparam name="T">Type of entities being inserted.</typeparam>
        /// <param name="ctx">The active <see cref="DbContext"/> that supplies the connection and options.</param>
        /// <param name="m">Mapping metadata describing how the entity maps to the database table.</param>
        /// <param name="entities">The entity instances to persist.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of rows inserted across all batches.</returns>
        public virtual async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0)
            {
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, 0, sw.Elapsed);
                return 0;
            }

            var operationKey = $"BulkInsert_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);
            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var maxBatchForProvider = MaxParameters == int.MaxValue
                ? 1000
                : Math.Max(1, Math.Min(1000, (MaxParameters - 10) / Math.Max(1, cols.Count)));
            var effectiveBatchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));
            // Logging infrastructure doesn't support arbitrary info; batch size can be inferred from performance metrics.

            var recordsAffected = 0;
            var index = 0;
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                while (index < entityList.Count)
                {
                    var availableMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
                    if (availableMemory < sizing.EstimatedMemoryUsage * 2)
                        effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);

                    if (await IsTransactionLogNearCapacityAsync(ctx, ct).ConfigureAwait(false))
                        effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);

                    var batch = entityList.GetRange(index, Math.Min(effectiveBatchSize, entityList.Count - index));
                    var batchSw = Stopwatch.StartNew();
                    recordsAffected += await ExecuteInsertBatch(ctx, m, batch, ct, transaction).ConfigureAwait(false);
                    batchSw.Stop();
                    BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                    index += batch.Count;
                }

                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkInsert failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw;
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }

        /// <summary>
        /// Executes a single batch insert for the supplied entities.
        /// </summary>
        /// <typeparam name="T">Type of entity being inserted.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/> providing the database connection.</param>
        /// <param name="m">Table mapping used to generate the insert statement.</param>
        /// <param name="batch">Entities to insert in a single round-trip.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <param name="transaction">Transaction that should contain the batch, or <c>null</c> to use the context's active transaction.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        protected async Task<int> ExecuteInsertBatch<T>(DbContext ctx, TableMapping m, List<T> batch, CancellationToken ct, DbTransaction? transaction = null) where T : class
        {
            ValidateConnection(ctx.Connection);
            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            // All columns are DB-generated — use DEFAULT VALUES per row (no batching possible).
            if (cols.Count == 0)
            {
                var inserted = 0;
                await using var cmd = ctx.CreateCommand();
                if (transaction != null) cmd.Transaction = transaction;
                cmd.CommandText = $"INSERT INTO {m.EscTable} DEFAULT VALUES";
                for (int i = 0; i < batch.Count; i++)
                    inserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                return inserted;
            }
            var sb = _stringBuilderPool.Get();
            try
            {
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                sb.Append($"INSERT INTO {m.EscTable} ({colNames}) VALUES ");

                await using var cmd = ctx.CreateCommand();
                if (transaction != null) cmd.Transaction = transaction;
                var pIndex = 0;
                for (int i = 0; i < batch.Count; i++)
                {
                    sb.Append(i > 0 ? ",(" : "(");
                    for (int j = 0; j < cols.Count; j++)
                    {
                        var pName = $"{ParamPrefix}p{pIndex++}";
                        cmd.AddParam(pName, cols[j].Getter(batch[i]));
                        sb.Append(j > 0 ? $",{pName}" : pName);
                    }
                    sb.Append(")");
                }
                cmd.CommandText = sb.ToString();
                return await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Performs a bulk update across the provided entities. Providers without a
        /// native implementation fall back to batched updates when enabled.
        /// </summary>
        public virtual Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedUpdateAsync(ctx, m, e, ct);
            throw new NormUnsupportedFeatureException(
                $"Bulk update is not supported by provider {Capabilities.ProviderName}. " +
                "Use the standard SaveChanges path instead, or set DbContextOptions.UseBatchedBulkOps = true.");
        }

        /// <summary>
        /// Performs a bulk delete across the provided entities. Providers without a
        /// native implementation fall back to batched deletes when enabled.
        /// </summary>
        public virtual Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedDeleteAsync(ctx, m, e, ct);
            throw new NormUnsupportedFeatureException(
                $"Bulk delete is not supported by provider {Capabilities.ProviderName}. " +
                "Use the standard SaveChanges path instead, or set DbContextOptions.UseBatchedBulkOps = true.");
        }

        /// <summary>
        /// Executes update statements in batches, selecting the batch size dynamically
        /// to balance performance and resource usage.
        /// </summary>
        protected async Task<int> BatchedUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var totalUpdated = 0;
            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                foreach (var entity in entities)
                {
                    await using var cmd = ctx.CreateCommand();
                    cmd.Transaction = transaction;
                    var batchHasTenant = ctx.Options.TenantProvider != null && m.TenantColumn != null;
                    cmd.CommandText = BuildUpdate(m, batchHasTenant);
                    foreach (var col in m.Columns.Where(c => !c.IsTimestamp && !(batchHasTenant && ReferenceEquals(c, m.TenantColumn))))
                        cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    if (m.TimestampColumn != null) cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                    // X1: bind tenant param to match the WHERE predicate added when batchHasTenant is true.
                    if (batchHasTenant) cmd.AddParam(ParamPrefix + m.TenantColumn!.PropName, ctx.Options.TenantProvider!.GetCurrentTenantId());
                    totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
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
            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        /// <summary>
        /// Executes delete statements in batches, selecting the batch size dynamically
        /// to balance performance and resource usage.
        /// </summary>
        protected async Task<int> BatchedDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            if (m.KeyColumns.Length == 0)
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var totalDeleted = 0;
            var keyColumns = m.KeyColumns.ToList();

            // Determine maximum entities per batch based on provider parameter limit
            var batchSize = Math.Min(ctx.Options.BulkBatchSize, 1000);
            if (MaxParameters != int.MaxValue)
            {
                var paramsPerEntity = Math.Max(1, keyColumns.Count);
                var maxBatchByParams = Math.Max(1, (MaxParameters - 10) / paramsPerEntity);
                batchSize = Math.Min(batchSize, maxBatchByParams);
            }

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                    await using var cmd = ctx.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                    var paramNames = new List<string>();
                    var paramIndex = 0;

                    string whereClause;

                    if (keyColumns.Count == 1)
                    {
                        var keyCol = keyColumns[0];
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var pName = $"{ParamPrefix}p{paramIndex++}";
                            paramNames.Add(pName);
                            cmd.AddParam(pName, keyCol.Getter(batch[j]));
                        }
                        whereClause = $"{keyCol.EscCol} IN ({string.Join(",", paramNames)})";
                    }
                    else
                    {
                        var orConditions = new List<string>();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var keyValues = keyColumns.Select(c =>
                            {
                                var pName = $"{ParamPrefix}p{paramIndex++}";
                                cmd.AddParam(pName, c.Getter(batch[j]));
                                return $"{c.EscCol} = {pName}";
                            });
                            orConditions.Add($"({string.Join(" AND ", keyValues)})");
                        }
                        whereClause = string.Join(" OR ", orConditions);
                    }

                    if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                    {
                        var tenantParam = $"{ParamPrefix}__tenant_bulk";
                        cmd.AddParam(tenantParam, ctx.Options.TenantProvider.GetCurrentTenantId());
                        whereClause = $"({whereClause}) AND {m.TenantColumn.EscCol} = {tenantParam}";
                    }

                    cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {whereClause}";
                    var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    totalDeleted += deleted;
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

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
        #endregion

        #region SQL Generation
        /// <summary>
        /// Returns the columns that should appear in an INSERT statement for the given mapping.
        /// Override in provider subclasses to exclude server-managed columns (e.g. SQL Server ROWVERSION)
        /// that cannot receive explicit values on INSERT.
        /// </summary>
        public virtual Column[] GetInsertColumns(TableMapping m) => m.InsertColumns;

        /// <summary>
        /// Builds a parameterized <c>INSERT</c> statement for the specified table
        /// mapping, caching the generated SQL for future use.
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <param name="hydrateGeneratedKeys">
        /// When <c>true</c>, append provider-specific identity retrieval so callers can hydrate generated keys.
        /// </param>
        /// <returns>An <c>INSERT</c> statement ready for parameter substitution.</returns>
        public string BuildInsert(TableMapping m, bool hydrateGeneratedKeys = true)
        {
            var includeIdentityRetrieval = hydrateGeneratedKeys && m.KeyColumns.Any(k => k.IsDbGenerated);
            var cacheKey = includeIdentityRetrieval ? "INSERT" : "INSERT_PLAIN";
            return _sqlCache.GetOrAdd((m.Type, m.TableName, cacheKey, m.SqlShapeKey), _ => {
                var cols = GetInsertColumns(m);
                var identityPrefix = includeIdentityRetrieval ? GetIdentityRetrievalPrefix(m) : string.Empty;
                var identitySuffix = includeIdentityRetrieval ? GetIdentityRetrievalString(m) : string.Empty;
                if (cols.Length == 0)
                {
                    return $"INSERT INTO {m.EscTable}{identityPrefix} DEFAULT VALUES{identitySuffix}";
                }
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                var valParams = string.Join(", ", cols.Select(c => ParamPrefix + c.PropName));
                return $"INSERT INTO {m.EscTable} ({colNames}){identityPrefix} VALUES ({valParams}){identitySuffix}";
            });
        }

        /// <summary>
        /// Builds a parameterized <c>UPDATE</c> statement that updates all non-key
        /// columns and filters by the entity's primary key (and timestamp when present).
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <param name="includeTenant">When <c>true</c>, appends an AND predicate for the tenant
        /// column so the statement can only modify the current tenant's rows.</param>
        /// <returns>An <c>UPDATE</c> SQL statement.</returns>
        public string BuildUpdate(TableMapping m, bool includeTenant = false)
        {
            if (m.UpdateColumns.Length == 0)
                throw new NormConfigurationException(
                    $"Entity '{m.Type.Name}' has no mutable columns to update. Add at least one non-key, non-timestamp property.");

            // X1: cache key distinguishes tenant vs non-tenant SQL so both shapes can coexist.
            var cacheOp = includeTenant ? "UPDATE_TENANT" : "UPDATE";
            return _sqlCache.GetOrAdd((m.Type, m.TableName, cacheOp, m.SqlShapeKey), _ =>
            {
                var set = string.Join(", ", m.UpdateColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}"));

                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                if (m.TimestampColumn != null)
                {
                    var tc = m.TimestampColumn;
                    whereCols.Add($"({tc.EscCol}={ParamPrefix}{tc.PropName} OR ({tc.EscCol} IS NULL AND {ParamPrefix}{tc.PropName} IS NULL))");
                }
                // X1: include tenant column in WHERE so direct UpdateAsync cannot cross-write rows
                // belonging to other tenants — parity with the batched SaveChangesAsync path.
                if (includeTenant && m.TenantColumn != null)
                    whereCols.Add($"{m.TenantColumn.EscCol}={ParamPrefix}{m.TenantColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"UPDATE {m.EscTable} SET {set} WHERE {where}";
            });
        }

        /// <summary>
        /// Builds a parameterized <c>DELETE</c> statement that filters by the primary
        /// key (and timestamp when applicable) to ensure a single row is targeted.
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <param name="includeTenant">When <c>true</c>, appends an AND predicate for the tenant
        /// column so the statement can only delete the current tenant's rows.</param>
        /// <returns>A <c>DELETE</c> SQL statement.</returns>
        public string BuildDelete(TableMapping m, bool includeTenant = false)
        {
            // X1: cache key distinguishes tenant vs non-tenant SQL so both shapes can coexist.
            var cacheOp = includeTenant ? "DELETE_TENANT" : "DELETE";
            return _sqlCache.GetOrAdd((m.Type, m.TableName, cacheOp, m.SqlShapeKey), _ =>
            {
                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                if (m.TimestampColumn != null)
                {
                    var tc = m.TimestampColumn;
                    whereCols.Add($"({tc.EscCol}={ParamPrefix}{tc.PropName} OR ({tc.EscCol} IS NULL AND {ParamPrefix}{tc.PropName} IS NULL))");
                }
                // X1: include tenant column in WHERE so direct DeleteAsync cannot cross-delete rows
                // belonging to other tenants — parity with the batched SaveChangesAsync path.
                if (includeTenant && m.TenantColumn != null)
                    whereCols.Add($"{m.TenantColumn.EscCol}={ParamPrefix}{m.TenantColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"DELETE FROM {m.EscTable} WHERE {where}";
            });
        }
        #endregion
    }
}
