using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class SqlServerProvider
    {
        /// <inheritdoc />
        public override bool SupportsNativeTenantSessionContext => true;

        /// <inheritdoc />
        public override string GetSetNativeTenantSessionContextSql(string sessionKey, string tenantParameterName)
        {
            EnsureValidParameterName(tenantParameterName, nameof(tenantParameterName));
            if (string.IsNullOrWhiteSpace(sessionKey) || sessionKey.Contains('\'', StringComparison.Ordinal))
                throw new ArgumentException("Session key must be non-empty and must not contain single quotes.", nameof(sessionKey));
            return $"EXEC sys.sp_set_session_context @key = N'{sessionKey}', @value = {tenantParameterName};";
        }

        /// <inheritdoc />
        public override string GenerateNativeTenantPolicySql(TableMapping mapping, string sessionKey)
        {
            var tenantCol = mapping.TenantColumn
                ?? throw new NormConfigurationException(
                    $"Entity '{mapping.Type.Name}' does not map tenant column required for native SQL Server RLS.");
            if (string.IsNullOrWhiteSpace(sessionKey) || sessionKey.Contains('\'', StringComparison.Ordinal))
                throw new ArgumentException("Session key must be non-empty and must not contain single quotes.", nameof(sessionKey));

            var baseTableName = mapping.TableName.Contains('.', StringComparison.Ordinal)
                ? mapping.TableName[(mapping.TableName.LastIndexOf('.') + 1)..]
                : mapping.TableName;
            var predicateFunction = Escape("dbo.fn_norm_rls_" + baseTableName);
            var policy = Escape("dbo.sp_norm_rls_" + baseTableName);
            var table = Escape(mapping.TableName.Contains('.', StringComparison.Ordinal)
                ? mapping.TableName
                : "dbo." + mapping.TableName);
            var sqlType = GetSqlType(tenantCol.Prop.PropertyType);
            return $@"
CREATE OR ALTER FUNCTION {predicateFunction}(@TenantId {sqlType})
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE CONVERT(NVARCHAR(4000), @TenantId) = CONVERT(NVARCHAR(4000), SESSION_CONTEXT(N'{sessionKey}'));
GO

IF NOT EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'sp_norm_rls_{baseTableName}' AND SCHEMA_NAME(schema_id) = N'dbo')
BEGIN
    CREATE SECURITY POLICY {policy}
    ADD FILTER PREDICATE {predicateFunction}({tenantCol.EscCol}) ON {table},
    ADD BLOCK PREDICATE {predicateFunction}({tenantCol.EscCol}) ON {table} AFTER INSERT,
    ADD BLOCK PREDICATE {predicateFunction}({tenantCol.EscCol}) ON {table} AFTER UPDATE
    WITH (STATE = ON);
END;";
        }

        /// <inheritdoc />
        public override string GenerateDropNativeTenantPolicySql(TableMapping mapping)
        {
            if (mapping.TenantColumn == null)
                throw new NormConfigurationException(
                    $"Entity '{mapping.Type.Name}' does not map tenant column required for native SQL Server RLS.");

            var baseTableName = mapping.TableName.Contains('.', StringComparison.Ordinal)
                ? mapping.TableName[(mapping.TableName.LastIndexOf('.') + 1)..]
                : mapping.TableName;
            var predicateFunction = Escape("dbo.fn_norm_rls_" + baseTableName);
            var policy = Escape("dbo.sp_norm_rls_" + baseTableName);
            return $@"
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'sp_norm_rls_{baseTableName}' AND SCHEMA_NAME(schema_id) = N'dbo')
    DROP SECURITY POLICY {policy};

IF OBJECT_ID(N'dbo.fn_norm_rls_{baseTableName}', N'IF') IS NOT NULL
    DROP FUNCTION {predicateFunction};";
        }

        /// <summary>
        /// Ensures that the provided <see cref="DbConnection"/> is a SQL Server
        /// connection compatible with this provider.
        /// </summary>
        /// <param name="connection">The connection instance to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not a <see cref="SqlConnection"/>.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (_isDialectOnly) return; // foreign parameter factory => dialect-only mode, foreign connection is intentional
            if (connection is not SqlConnection)
                throw new InvalidOperationException("A SqlConnection is required for SqlServerProvider.");
        }

        /// <summary>
        /// Checks whether the SQL Server provider can operate by connecting to a local instance
        /// and verifying the server version meets the minimum requirement (SQL Server 2016+).
        /// </summary>
        /// <returns><c>true</c> if SQL Server is reachable and meets the minimum version; otherwise, <c>false</c>.</returns>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString =
                "Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=True;Connect Timeout=1";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var parts = versionStr.Split('.');
                var version = new Version(int.Parse(parts[0]), int.Parse(parts[1]));
                return version >= MinimumSqlServerVersion;
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
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
            return cmd.ExecuteScalar() as string;
        }

        /// <summary>
        /// Creates a transaction savepoint using SQL Server's <see cref="SqlTransaction.Save(string)"/> API.
        /// Checks the CancellationToken before and after executing so that pre-cancelled
        /// tokens correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The transaction on which to create the savepoint.</param>
        /// <param name="name">Name of the savepoint.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqlTransaction sqlTransaction)
            {
                sqlTransaction.Save(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Rolls back the transaction to the specified savepoint.
        /// Checks the CancellationToken before and after executing so that pre-cancelled
        /// tokens correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqlTransaction sqlTransaction)
            {
                sqlTransaction.Rollback(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Releasing a savepoint is a no-op on SQL Server: the engine has no RELEASE SAVEPOINT statement and
        /// releases savepoints automatically when the transaction commits (rolling back to an outer savepoint
        /// still discards inner ones). This matches EF Core's SQL Server behaviour — the work done since the
        /// savepoint is kept and the savepoint simply stops being an explicit rollback target.
        /// </summary>
        /// <param name="transaction">The transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to release (unused; SQL Server auto-releases).</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task ReleaseSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqlTransaction)
                return Task.CompletedTask;
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }
    }
}
