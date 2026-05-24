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
        /// Returns SQL that evaluates `end - start` as a fractional number of seconds. Used
        /// by the LINQ translator to lower `(end - start).TotalSeconds / TotalMinutes /
        /// TotalHours / Days / etc.` to a portable scalar expression. Both arguments are
        /// already-translated SQL fragments.
        /// </summary>
        public virtual string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => throw new NormUnsupportedFeatureException(
                $"DateTime subtraction is not supported by provider '{GetType().Name}'.");

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
        /// Returns SQL that XORs two integer expressions. SQL Server and MySQL accept the
        /// `^` operator; PostgreSQL uses `#`; SQLite has no XOR operator and falls back to
        /// `(a | b) - (a &amp; b)` — algebraically equivalent on integers.
        /// </summary>
        public virtual string GetBitwiseXorSql(string left, string right) => $"({left} ^ {right})";

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
                var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
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
