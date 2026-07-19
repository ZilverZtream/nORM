using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class PostgresProvider
    {
        /// <summary>
        /// Determines whether a <see cref="DbException"/> represents a "table does not exist" error.
        /// PostgreSQL uses SQLSTATE <c>42P01</c> ("undefined_table") for this condition.
        /// Falls back to the base message-based heuristic when SqlState is unavailable.
        /// </summary>
        /// <param name="ex">The exception to inspect.</param>
        /// <returns><c>true</c> when the error definitively indicates the table is missing; otherwise <c>false</c>.</returns>
        public override bool IsObjectNotFoundError(DbException ex)
        {
            if (string.Equals(ex.SqlState, PgErrorUndefinedTable, StringComparison.Ordinal))
                return true;

            return base.IsObjectNotFoundError(ex);
        }

        /// <summary>
        /// Ensures that the provided connection is compatible with the PostgreSQL provider
        /// by verifying its runtime type.
        /// </summary>
        /// <param name="connection">The connection instance to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not an Npgsql connection.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (_isDialectOnly) return; // foreign parameter factory → dialect-only mode, foreign connection is intentional
            if (connection.GetType().FullName != "Npgsql.NpgsqlConnection")
                throw new NormConfigurationException("An NpgsqlConnection is required for PostgresProvider.");
        }

        /// <summary>
        /// Determines whether the PostgreSQL provider can be used by attempting to load
        /// the Npgsql driver and connect to a local database.
        /// </summary>
        /// <returns><c>true</c> if PostgreSQL is reachable and meets version requirements; otherwise, <c>false</c>.</returns>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Host=localhost;Database=postgres;Username=postgres;Password=;Timeout=1";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SHOW server_version";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr.Split(' ')[0]);
                return version >= MinimumPostgresVersion;
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
            cmd.CommandText = "SHOW server_version";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SHOW server_version";
            return cmd.ExecuteScalar() as string;
        }

        /// <summary>
        /// Creates a transaction savepoint using Npgsql's save or savepoint APIs if available.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The transaction on which to create the savepoint.</param>
        /// <param name="name">Identifier for the savepoint.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("CreateSavepoint", new[] { typeof(string) });
            if (saveMethod != null)
            {
                try
                {
                    saveMethod.Invoke(transaction, new object[] { name });
                    ct.ThrowIfCancellationRequested();
                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Save indicates the transaction type
                    // does not support savepoints — map to NormUnsupportedFeatureException for a stable API.
                    if (ex.InnerException is NotSupportedException)
                        throw new NormUnsupportedFeatureException(
                            $"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.", ex.InnerException);
                    if (ex.InnerException != null)
                        throw ex.InnerException;
                    throw;
                }
            }
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Rolls back a transaction to the specified savepoint.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">Transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var rollbackMethod = transaction.GetType().GetMethod("Rollback", new[] { typeof(string) }) ??
                                 transaction.GetType().GetMethod("RollbackToSavepoint", new[] { typeof(string) });
            if (rollbackMethod != null)
            {
                try
                {
                    rollbackMethod.Invoke(transaction, new object[] { name });
                    ct.ThrowIfCancellationRequested();
                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Rollback indicates the transaction type
                    // does not support savepoints — map to NormUnsupportedFeatureException for a stable API.
                    if (ex.InnerException is NotSupportedException)
                        throw new NormUnsupportedFeatureException(
                            $"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.", ex.InnerException);
                    if (ex.InnerException != null)
                        throw ex.InnerException;
                    throw;
                }
            }
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Releases a previously created savepoint (RELEASE SAVEPOINT) via the ADO.NET
        /// <c>DbTransaction.Release(string)</c> API. Checks the CancellationToken before executing so a
        /// pre-cancelled token throws <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">Transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to release.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task ReleaseSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var releaseMethod = transaction.GetType().GetMethod("Release", new[] { typeof(string) }) ??
                                transaction.GetType().GetMethod("ReleaseSavepoint", new[] { typeof(string) });
            if (releaseMethod != null)
            {
                try
                {
                    releaseMethod.Invoke(transaction, new object[] { name });
                    ct.ThrowIfCancellationRequested();
                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Release indicates the transaction type
                    // does not support releasing savepoints — map to NormUnsupportedFeatureException.
                    if (ex.InnerException is NotSupportedException)
                        throw new NormUnsupportedFeatureException(
                            $"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.", ex.InnerException);
                    if (ex.InnerException != null)
                        throw ex.InnerException;
                    throw;
                }
            }
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }
    }
}
