using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Database provider tailored for MySQL and MariaDB.
    /// Implements SQL dialect nuances, bulk operations and connection features
    /// that leverage MySQL capabilities for high throughput data access.
    /// </summary>
    public sealed partial class MySqlProvider : DatabaseProvider
    {
        internal override bool ParameterizeFastPathBooleanPredicates => true;

        internal override bool SupportsFastPathPreparedCommandCache => true;

        // MySQL rejects self-referencing IN subqueries in DELETE/UPDATE - wrap in a derived table.
        internal override bool CudWhereInSubqueryNeedsDoubleWrap => true;

        internal override bool SupportsCommandGeneratedKeyRetrieval => true;

        /// <summary>
        /// A single-column integer primary key with no explicit value-generation config is store-generated
        /// (EF Core parity): MySQL realizes it as an AUTO_INCREMENT column, which generates a value when the
        /// column is omitted and honors an explicitly-supplied non-zero value. See <see cref="TableMapping.ConventionGeneratedKeyColumn"/>.
        /// </summary>
        public override bool SupportsConventionKeyStoreGeneration => true;

        /// <summary>MySQL realizes the convention key as an AUTO_INCREMENT column (needs identity DDL).</summary>
        internal override bool ConventionKeyUsesIdentityColumn => true;

        internal override bool PrefersSyncFastPathExecution => true;

        internal override bool PrefersSyncQueryPlanExecution => true;

        internal override bool SupportsQueryPlanPreparedCommandCache => true;

        /// <inheritdoc />
        public override string ForceCaseSensitiveStringComparison(string sql) => $"BINARY {sql}";

        /// <summary>
        /// MySQL's CONCAT propagates NULL (unlike SQL Server's and PostgreSQL's, which
        /// ignore NULL operands); COALESCE each operand so a NULL contributes an empty
        /// string, matching C# string concatenation.
        /// </summary>
        public override string GetNullSafeConcatSql(string left, string right)
            => $"CONCAT(COALESCE({left}, ''), COALESCE({right}, ''))";

        /// <summary>MySQL rejects <c>DEFAULT VALUES</c>; an all-default row is <c>INSERT INTO t () VALUES ()</c>.</summary>
        public override string DefaultValuesInsertClause => "() VALUES ()";

        /// <summary>Maximum number of cached DataTable schemas used for bulk insert data tables.</summary>
        private const int TableSchemaCacheSize = 100;

        private static readonly ConcurrentLruCache<TableMapping, DataTable> _tableSchemas = new(maxSize: TableSchemaCacheSize);
        private static readonly ConcurrentDictionary<string, bool> _bulkCopyUnavailable = new(StringComparer.Ordinal);
        private static readonly ConcurrentDictionary<Type, Func<DbCommand, object?>> _lastInsertedIdAccessors = new();
        private readonly IDbParameterFactory _parameterFactory;
        private readonly bool _useAffectedRowsSemantics;

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <remarks>
        /// Uses a reflection-based MySQL parameter factory. Consumer applications must
        /// reference either <c>MySqlConnector</c> or <c>MySql.Data</c> when executing
        /// MySQL commands.
        /// </remarks>
        public MySqlProvider()
            : this(new ReflectionMySqlParameterFactory(), useAffectedRowsSemantics: true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <param name="useAffectedRowsSemantics">
        /// <see langword="true"/> when the MySQL driver reports changed rows for UPDATE statements.
        /// Use <see langword="false"/> only when the connection string is configured for matched-row
        /// semantics, for example <c>UseAffectedRows=false</c> with MySqlConnector.
        /// </param>
        public MySqlProvider(bool useAffectedRowsSemantics)
            : this(new ReflectionMySqlParameterFactory(), useAffectedRowsSemantics)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <param name="parameterFactory">Factory used to create provider-specific parameters.</param>
        /// <remarks>
        /// <para>
        /// <b>Runtime dependency:</b> This provider uses MySQL via reflection and requires either
        /// <c>MySqlConnector</c> or <c>MySql.Data</c> to be installed in the consuming application.
        /// Add one of the following to your project:
        /// <code>
        /// dotnet add package MySqlConnector
        /// </code>
        /// or
        /// <code>
        /// dotnet add package MySql.Data
        /// </code>
        /// A clear <see cref="InvalidOperationException"/> is thrown at runtime if neither package is present.
        /// </para>
        /// </remarks>
        public MySqlProvider(IDbParameterFactory parameterFactory)
            : this(parameterFactory, useAffectedRowsSemantics: true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <param name="parameterFactory">Factory used to create provider-specific parameters.</param>
        /// <param name="useAffectedRowsSemantics">
        /// <see langword="true"/> when the MySQL driver reports changed rows for UPDATE statements.
        /// Use <see langword="false"/> only when the connection string is configured for matched-row
        /// semantics, for example <c>UseAffectedRows=false</c> with MySqlConnector.
        /// </param>
        /// <remarks>
        /// When this value is <see langword="false"/>, nORM treats zero-row UPDATE results as strict
        /// optimistic-concurrency conflicts. The database connection must be configured consistently;
        /// setting this to <see langword="false"/> while the driver still uses affected-row semantics
        /// can produce false-positive conflicts for same-value updates.
        /// </remarks>
        public MySqlProvider(IDbParameterFactory parameterFactory, bool useAffectedRowsSemantics)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
            _useAffectedRowsSemantics = useAffectedRowsSemantics;
            // Dialect-only mode: when a non-native parameter factory is supplied, the caller is using
            // this provider purely for its SQL dialect against a foreign engine (e.g., SQLite). Native
            // connection-type and server-version validation must be skipped in that scenario.
            _isDialectOnly = parameterFactory is not ReflectionMySqlParameterFactory;
        }

        private readonly bool _isDialectOnly;

        private sealed class ReflectionMySqlParameterFactory : IDbParameterFactory
        {
            [System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)]
            private readonly Type? _parameterType = ResolveMySqlParameterType();

            // The driver assembly is loaded by name at runtime without a build-time reference, so
            // the trimmer cannot preserve its members. Trimmed or NativeAOT deployments must pass
            // the driver's native IDbParameterFactory to the provider constructor instead; this
            // fallback throws with package guidance when the driver types are unavailable.
            [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
                Justification = "Optional driver types are resolved by name at runtime; trimmed deployments use an explicit IDbParameterFactory.")]
            [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)]
            private static Type? ResolveMySqlParameterType() =>
                Type.GetType("MySqlConnector.MySqlParameter, MySqlConnector") ??
                Type.GetType("MySql.Data.MySqlClient.MySqlParameter, MySql.Data");

            public DbParameter CreateParameter(string name, object? value)
            {
                if (_parameterType == null)
                {
                    throw new InvalidOperationException(
                        "MySQL package is required for MySQL support. Add PackageReference Include=\"MySqlConnector\" or Include=\"MySql.Data\" to the consuming project.");
                }

                return (DbParameter)Activator.CreateInstance(_parameterType, name, value ?? DBNull.Value)!;
            }
        }

        /// <summary>
        /// Maximum allowed length of a single SQL statement in characters.
        /// </summary>
        public override int MaxSqlLength => 4_194_304;

        /// <summary>
        /// Maximum number of parameters permitted in a single MySQL command.
        /// </summary>
        public override int MaxParameters => 65_535;

        /// <inheritdoc />
        public override ProviderCapabilities Capabilities => new(
            "MySQL",
            new Version(8, 0),
            MaxParameters,
            true,
            true,
            true,
            true,
            "Requires MySqlConnector or MySql.Data and MySQL 8.0 or newer.");

        /// <summary>
        /// Escapes an identifier using MySQL backtick delimiters.
        /// Handles multi-part identifiers (schema.table) by escaping each segment separately so
        /// that <c>`schema`.`table`</c> is produced rather than the invalid <c>`schema.table`</c>.
        /// Embedded backtick characters are doubled to prevent SQL injection via identifiers.
        /// </summary>
        /// <param name="id">The identifier to escape.</param>
        /// <returns>The escaped identifier.</returns>
        public override string Escape(string id)
        {
            if (string.IsNullOrWhiteSpace(id)) return id;
            if (id.Contains('.'))
                return string.Join(".", id.Split('.').Select(part => $"`{part.Replace("`", "``")}`"));
            return $"`{id.Replace("`", "``")}`";
        }

        /// <summary>
        /// Appends a MySQL <c>LIMIT</c> clause to the SQL builder to paginate results.
        /// </summary>
        /// <param name="sb">The SQL builder being appended to.</param>
        /// <param name="limit">The maximum number of rows to return.</param>
        /// <param name="offset">The number of rows to skip before returning results.</param>
        /// <param name="limitParameterName">Parameter name for the limit value.</param>
        /// <param name="offsetParameterName">Parameter name for the offset value.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            // MySQL uses a single LIMIT clause for both limit and offset in the form
            // "LIMIT offset, limit". The previous implementation only emitted the clause
            // when a limit was present which meant that queries using only Skip() would
            // ignore the offset. MySQL requires a limit value when an offset is specified,
            // so when only an offset is provided we use the maximum unsigned BIGINT value
            // to effectively indicate "no limit".
            bool hasLimit = limitParameterName != null || limit.HasValue;
            bool hasOffset = offsetParameterName != null || offset.HasValue;
            if (hasLimit || hasOffset)
            {
                sb.Append(" LIMIT ");
                if (hasOffset)
                {
                    if (offsetParameterName != null) sb.Append(offsetParameterName);
                    else sb.Append(offset!.Value);
                    sb.Append(", ");
                }

                if (limitParameterName != null) sb.Append(limitParameterName);
                else if (limit.HasValue) sb.Append(limit.Value);
                else sb.Append("18446744073709551615");
            }
        }
        
        /// <summary>
        /// MySQL Connector/NET defaults to reporting <em>affected</em> (changed) rows rather than
        /// <em>matched</em> rows. Returning <c>true</c> here disables the rowcount-based OCC check
        /// in the save pipeline to prevent false-positive <see cref="DbConcurrencyException"/>s
        /// on same-value updates.
        ///
        /// <para><b>Known trade-off (S1):</b> With affected-row semantics, a genuine stale-row
        /// conflict where the concurrent writer sets the token to the <em>same</em> value is not
        /// detected. For full OCC guarantee on MySQL, add <c>UseAffectedRows=false</c> to the
        /// connection string and construct this provider with
        /// <c>useAffectedRowsSemantics: false</c>.</para>
        /// </summary>
        internal override bool UseAffectedRowsSemantics => _useAffectedRowsSemantics;
    }
}
