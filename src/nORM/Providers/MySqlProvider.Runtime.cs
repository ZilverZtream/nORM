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
    public sealed partial class MySqlProvider
    {
        /// <summary>
        /// Validates that the supplied connection is compatible with the MySQL provider.
        /// </summary>
        /// <param name="connection">The connection to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not a MySQL connection.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (_isDialectOnly) return; // foreign parameter factory ? dialect-only mode, foreign connection is intentional
            var name = connection.GetType().FullName;
            if (name != "MySqlConnector.MySqlConnection" && name != "MySql.Data.MySqlClient.MySqlConnection")
                throw new NormConfigurationException("A MySqlConnection is required for MySqlProvider. Please install MySqlConnector or MySql.Data.");
        }

        /// <summary>
        /// Checks whether the necessary MySQL client libraries are available and that a
        /// modern MySQL server (version 8.0 or higher) can be reached.
        /// </summary>
        public override async Task<bool> IsAvailableAsync()
        {
            var type =
                Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector") ??
                Type.GetType("MySql.Data.MySqlClient.MySqlConnection, MySql.Data");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString =
                "Server=localhost;Database=test;User=root;Password=;Allow User Variables=true";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SELECT VERSION()";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr.Split('-')[0]);
                return version >= new Version(8, 0);
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

        /// <summary>
        /// Creates a transaction savepoint to allow partial rollbacks when supported by the underlying MySQL driver.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/> rather than silently proceeding.
        /// </summary>
        /// <param name="transaction">Active transaction to create the savepoint on.</param>
        /// <param name="name">Name of the savepoint.</param>
        /// <param name="ct">Cancellation token for the asynchronous operation.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken - a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("CreateSavepoint", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("Savepoint", new[] { typeof(string) });
            if (saveMethod != null)
            {
                try
                {
                    saveMethod.Invoke(transaction, new object[] { name });

                    // Check after the sync call in case cancellation arrived mid-operation.
                    ct.ThrowIfCancellationRequested();

                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // TargetInvocationException wraps the actual database exception, making it harder to handle.
                    // NotSupportedException from a base DbTransaction.Save indicates the transaction type
                    // does not support savepoints - map to NormUnsupportedFeatureException for a stable API.
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
        /// Rolls back the transaction to a previously created savepoint.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/> rather than silently proceeding.
        /// </summary>
        /// <param name="transaction">The transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Cancellation token for the asynchronous operation.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken - a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var rollbackMethod = transaction.GetType().GetMethod("Rollback", new[] { typeof(string) }) ??
                                 transaction.GetType().GetMethod("RollbackToSavepoint", new[] { typeof(string) });
            if (rollbackMethod != null)
            {
                try
                {
                    rollbackMethod.Invoke(transaction, new object[] { name });

                    // Check after the sync call in case cancellation arrived mid-operation.
                    ct.ThrowIfCancellationRequested();

                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Rollback indicates the transaction type
                    // does not support savepoints - map to NormUnsupportedFeatureException for a stable API.
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
        public override Task ReleaseSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken - a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var releaseMethod = transaction.GetType().GetMethod("Release", new[] { typeof(string) }) ??
                                transaction.GetType().GetMethod("ReleaseSavepoint", new[] { typeof(string) });
            if (releaseMethod != null)
            {
                try
                {
                    releaseMethod.Invoke(transaction, new object[] { name });

                    // Check after the sync call in case cancellation arrived mid-operation.
                    ct.ThrowIfCancellationRequested();

                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Release indicates the transaction type
                    // does not support releasing savepoints - map to NormUnsupportedFeatureException.
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

        /// <inheritdoc />
        protected override async Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT VERSION()";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT VERSION()";
            return cmd.ExecuteScalar() as string;
        }
    }
}
