using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace nORM.Core
{
    /// <summary>
    /// Manages a single database connection in an asynchronous and thread-safe manner.
    /// </summary>
    public class AsyncConnectionManager
    {
        private readonly SemaphoreSlim _connectionSemaphore;
        private readonly DbConnection _connection;
        private readonly ILogger _logger;

        public AsyncConnectionManager(DbConnection connection, int maxConcurrency, ILogger logger)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _connectionSemaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<T> ExecuteWithConnectionAsync<T>(Func<DbConnection, CancellationToken, Task<T>> operation, CancellationToken ct = default)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));
            await _connectionSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await EnsureConnectionOpenAsync(ct).ConfigureAwait(false);
                return await operation(_connection, ct).ConfigureAwait(false);
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        /// <summary>
        /// Opens the underlying <see cref="DbConnection"/> if it is not already open.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous open operation.</param>
        /// <returns>A task that completes once the connection is confirmed open.</returns>
        private async Task EnsureConnectionOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
            {
                try
                {
                    await _connection.OpenAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to open database connection");
                    throw;
                }
            }
        }
    }
}
