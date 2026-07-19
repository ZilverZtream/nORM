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
    public abstract partial class DatabaseProvider
    {
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
        /// True when the provider generates a new concurrency token on every UPDATE at the database
        /// level (SQL Server ROWVERSION). When false (SQLite, PostgreSQL, MySQL — no native
        /// rowversion), nORM client-manages the [Timestamp] token: it writes a fresh value in the SET
        /// clause on each UPDATE so a stale concurrent write is detected instead of silently winning.
        /// </summary>
        internal virtual bool SupportsNativeRowVersion => false;

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
        /// Releases (destroys) a previously created savepoint, keeping the work done since it was created but
        /// making it no longer a rollback target. The default implementation throws as savepoints are provider
        /// specific. Providers whose engine auto-releases savepoints (SQL Server) override this as a no-op.
        /// </summary>
        /// <param name="transaction">The active transaction.</param>
        /// <param name="name">Name of the savepoint to release.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual Task ReleaseSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
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
                    ProviderMobilityTranslator.BuildProviderVersionViolationMessage(Capabilities, null));

            if (actual < minimum)
                throw new NormConfigurationException(
                    ProviderMobilityTranslator.BuildProviderVersionViolationMessage(Capabilities, actual));
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
    }
}
