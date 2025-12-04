using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Providers;

namespace nORM.Core
{
    /// <summary>
    /// Provides connection management with support for read replicas and failover.
    /// </summary>
    /// <remarks>
    /// IMPORTANT: This class no longer uses custom connection pooling. Instead, it relies on
    /// the database provider's native connection pooling (configured via connection string).
    /// This eliminates redundant "double pooling" and reduces resource overhead.
    ///
    /// ConnectionManager provides:
    /// - Round-robin load balancing across read replicas
    /// - Automatic failover when a node becomes unhealthy
    /// - Health monitoring of database nodes
    ///
    /// All connections are created fresh and should be disposed by the caller, which returns
    /// them to the ADO.NET provider's connection pool.
    /// </remarks>
    public class ConnectionManager : IDisposable
    {
        private readonly DatabaseTopology _topology;
        private readonly DatabaseProvider _provider;
        private readonly ILogger _logger;
        private readonly PeriodicTimer _healthCheckTimer;
        private readonly Task _healthCheckTask;
        private readonly SemaphoreSlim _failoverSemaphore = new(1, 1);
        private readonly SemaphoreSlim _healthCheckSemaphore = new(1, 1);
        private readonly CancellationTokenSource _disposeCts = new();

        private bool _disposed;

        private volatile DatabaseTopology.DatabaseNode? _currentPrimary;
        private volatile List<DatabaseTopology.DatabaseNode> _availableReadReplicas = new();
        private int _readReplicaIndex;

        private readonly object _circuitBreakerLock = new();
        // RACE CONDITION FIX (TASK 11): Lock for safe access to _availableReadReplicas
        private readonly object _replicaListLock = new();
        private int _consecutiveFailures;
        private DateTime _nextRetry = DateTime.MinValue;
        private readonly TimeSpan _baseDelay = TimeSpan.FromSeconds(1);
        private readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Initializes a new <see cref="ConnectionManager"/> that manages database connections
        /// across a topology of nodes. Starts an optional background health check loop.
        /// </summary>
        /// <param name="topology">Topology describing available database nodes.</param>
        /// <param name="provider">Database provider responsible for creating connections.</param>
        /// <param name="logger">Logger used for diagnostic output.</param>
        /// <param name="healthCheckInterval">Optional interval between health check executions.</param>
        public ConnectionManager(DatabaseTopology topology, DatabaseProvider provider, ILogger logger, TimeSpan? healthCheckInterval = null)
        {
            _topology = topology ?? throw new ArgumentNullException(nameof(topology));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            DeterminePrimaryNode();
            UpdateAvailableReadReplicas();

            var interval = healthCheckInterval ?? TimeSpan.FromSeconds(30);
            _healthCheckTimer = new PeriodicTimer(interval);
            _healthCheckTask = Task.Run(() => HealthCheckLoopAsync(_disposeCts.Token));
        }

        /// <summary>
        /// Factory method used to create new <see cref="DbConnection"/> instances for
        /// a given connection string using the configured provider.
        /// </summary>
        /// <param name="connectionString">The database connection string.</param>
        /// <returns>A newly created <see cref="DbConnection"/>.</returns>
        private DbConnection CreateConnection(string connectionString)
        {
            return DbConnectionFactory.Create(connectionString, _provider);
        }

        /// <summary>
        /// Determines which database node should act as the primary (write) node. The
        /// first node explicitly marked as <c>Primary</c> is selected, falling back to
        /// the first node in the topology if none are marked.
        /// </summary>
        private void DeterminePrimaryNode()
        {
            // Choose the first node marked as Primary role; fallback to first node
            var primary = _topology.Nodes.FirstOrDefault(n => n.Role == DatabaseTopology.DatabaseRole.Primary)
                          ?? _topology.Nodes.FirstOrDefault();
            _currentPrimary = primary;
        }

        /// <summary>
        /// Refreshes the list of healthy read-replica nodes ordered by priority. This
        /// list is consulted when selecting a connection for read operations.
        /// </summary>
        private void UpdateAvailableReadReplicas()
        {
            // RACE CONDITION FIX (TASK 11): Synchronize write to _availableReadReplicas
            lock (_replicaListLock)
            {
                _availableReadReplicas = _topology.Nodes
                    .Where(n => n.Role == DatabaseTopology.DatabaseRole.ReadReplica && n.IsHealthy)
                    .OrderBy(n => n.Priority)
                    .ToList();
            }
        }

        /// <summary>
        /// Retrieves an open connection to the primary node for write operations.
        /// Implements a simple circuit breaker and failover mechanism.
        /// The caller must dispose the returned connection.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An open <see cref="DbConnection"/> suitable for write operations.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no primary node is available.</exception>
        public async Task<DbConnection> GetWriteConnectionAsync(CancellationToken ct = default)
        {
            lock (_circuitBreakerLock)
            {
                if (DateTime.UtcNow < _nextRetry)
                    throw new InvalidOperationException("Circuit breaker open: delaying write connection attempts.");
            }

            var primary = _currentPrimary;
            if (primary == null)
            {
                await TriggerFailoverAsync(ct).ConfigureAwait(false);
                primary = _currentPrimary;
                if (primary == null)
                    throw new InvalidOperationException("No primary node available.");
            }

            DbConnection? cn = null;
            try
            {
                cn = CreateConnection(primary.ConnectionString);
                await cn.OpenAsync(ct).ConfigureAwait(false);
                ResetCircuitBreaker();
                return cn;
            }
            catch (OperationCanceledException)
            {
                // CONNECTION LEAK FIX: Dispose failed connection before re-throwing
                // Don't trip circuit breaker on timeout/cancellation.
                // Timeout is a query-level issue, not a connection-level failure.
                cn?.Dispose();
                throw;
            }
            catch (DbException ex)
            {
                // CONNECTION LEAK FIX: Dispose failed connection before re-throwing
                cn?.Dispose();
                // RELIABILITY FIX (TASK 9): Check specific error codes instead of message strings
                // Message-based detection is fragile (localization, provider changes)
                if (IsTransientDatabaseError(ex))
                {
                    RegisterFailure();
                    _logger.LogError(ex, "Failed to acquire write connection due to transient database error");
                }
                throw;
            }
            catch (System.Net.Sockets.SocketException ex)
            {
                // CONNECTION LEAK FIX: Dispose failed connection before re-throwing
                cn?.Dispose();
                // RELIABILITY FIX (TASK 9): Detect network failures by exception type
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to network socket error");
                throw;
            }
            catch (System.IO.IOException ex) when (IsNetworkIOException(ex))
            {
                // CONNECTION LEAK FIX: Dispose failed connection before re-throwing
                cn?.Dispose();
                // RELIABILITY FIX (TASK 9): Some network errors manifest as IOException
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to network I/O error");
                throw;
            }
            catch (Exception ex)
            {
                // CONNECTION LEAK FIX: Dispose failed connection before re-throwing
                cn?.Dispose();
                // Other exceptions (e.g., application errors, auth failures) don't trip the circuit breaker
                _logger.LogError(ex, "Failed to acquire write connection (non-connection error)");
                throw;
            }
        }

        /// <summary>
        /// Retrieves an open connection optimized for read operations, using available read replicas
        /// and falling back to the primary node when necessary.
        /// The caller must dispose the returned connection.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An open <see cref="DbConnection"/> suitable for read operations.</returns>
        public async Task<DbConnection> GetReadConnectionAsync(CancellationToken ct = default)
        {
            // Synchronize read of _availableReadReplicas
            List<DatabaseTopology.DatabaseNode> replicas;
            lock (_replicaListLock)
            {
                replicas = _availableReadReplicas;
            }
            if (replicas.Count == 0)
            {
                _logger.LogWarning("No healthy read replicas available; using primary for read");
                return await GetWriteConnectionAsync(ct).ConfigureAwait(false);
            }

            var replica = SelectOptimalReadReplica(replicas);
            DbConnection? cn = null;
            try
            {
                cn = CreateConnection(replica.ConnectionString);
                await cn.OpenAsync(ct).ConfigureAwait(false);
                return cn;
            }
            catch (OperationCanceledException)
            {
                // CONNECTION LEAK FIX (TASK 2): Dispose failed connection before re-throwing
                cn?.Dispose();
                throw;
            }
            catch (Exception ex)
            {
                // CONNECTION LEAK FIX (TASK 2): Dispose failed connection before falling back
                cn?.Dispose();
                _logger.LogWarning(ex, "Failed to acquire connection from read replica {Replica}", replica.ConnectionString);
                replica.IsHealthy = false;
                UpdateAvailableReadReplicas();
                return await GetWriteConnectionAsync(ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Chooses the next read-replica to use based on a simple round-robin
        /// algorithm. The method is thread-safe and distributes load evenly across
        /// replicas.
        /// </summary>
        /// <param name="replicas">The list of currently available replicas.</param>
        /// <returns>The selected replica node.</returns>
        private DatabaseTopology.DatabaseNode SelectOptimalReadReplica(IReadOnlyList<DatabaseTopology.DatabaseNode> replicas)
        {
            // INTEGER OVERFLOW FIX: Cast to uint to handle overflow (negative numbers) gracefully
            // After ~2.1 billion increments, _readReplicaIndex overflows to negative.
            // Casting to uint treats it as unsigned, preventing negative modulo results.
            var count = replicas.Count; // Cache count to prevent multiple property access
            var index = (uint)Interlocked.Increment(ref _readReplicaIndex);
            return replicas[(int)(index % (uint)count)];
        }

        private void RegisterFailure()
        {
            lock (_circuitBreakerLock)
            {
                _consecutiveFailures++;
                var delay = TimeSpan.FromMilliseconds(Math.Min(_baseDelay.TotalMilliseconds * Math.Pow(2, _consecutiveFailures), _maxDelay.TotalMilliseconds));
                _nextRetry = DateTime.UtcNow + delay;
            }
        }

        private void ResetCircuitBreaker()
        {
            lock (_circuitBreakerLock)
            {
                _consecutiveFailures = 0;
                _nextRetry = DateTime.MinValue;
            }
        }

        /// <summary>
        /// Determines if a database exception represents a transient error that should trip the circuit breaker.
        /// RELIABILITY FIX (TASK 9): Uses error codes instead of message parsing for robust detection.
        /// </summary>
        /// <param name="ex">The database exception to check.</param>
        /// <returns>True if the exception represents a transient connection/network error.</returns>
        private static bool IsTransientDatabaseError(DbException ex)
        {
            // Check for SQL Server specific errors
            if (ex.GetType().Name == "SqlException")
            {
                var numberProp = ex.GetType().GetProperty("Number");
                if (numberProp != null && numberProp.GetValue(ex) is int errorNumber)
                {
                    // SQL Server transient errors:
                    // -2: Timeout
                    // 53: Named Pipes Provider error (network)
                    // 64: Error located on server
                    // 233: Connection initialization error
                    // 10053: Transport-level error (broken connection)
                    // 10054: Existing connection forcibly closed by remote host
                    // 10060: Connection timeout
                    // 40197: Service error processing request
                    // 40501: Service busy
                    // 40613: Database unavailable
                    return errorNumber is -2 or 53 or 64 or 233 or 10053 or 10054 or 10060 or 40197 or 40501 or 40613;
                }
            }

            // Check inner exception for SocketException (more reliable than message parsing)
            return HasSocketExceptionInChain(ex);
        }

        /// <summary>
        /// Checks if an IOException represents a network-related error.
        /// </summary>
        private static bool IsNetworkIOException(System.IO.IOException ex)
        {
            // Check if inner exception is a SocketException
            return HasSocketExceptionInChain(ex);
        }

        /// <summary>
        /// Recursively checks the exception chain for a SocketException.
        /// </summary>
        private static bool HasSocketExceptionInChain(Exception ex)
        {
            var current = ex;
            while (current != null)
            {
                if (current is System.Net.Sockets.SocketException)
                    return true;
                current = current.InnerException;
            }
            return false;
        }

        /// <summary>
        /// Continuously performs periodic health checks on all configured database nodes
        /// until cancellation is requested.
        /// </summary>
        /// <param name="token">Token that cancels the loop and any in-progress checks.</param>
        private async Task HealthCheckLoopAsync(CancellationToken token)
        {
            try
            {
                await PerformHealthChecksAsync(token).ConfigureAwait(false);

                while (await _healthCheckTimer.WaitForNextTickAsync(token).ConfigureAwait(false))
                {
                    await PerformHealthChecksAsync(token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        /// <summary>
        /// Executes a single round of health checks against all nodes in the database topology.
        /// </summary>
        /// <param name="token">Token used to cancel the health check operation.</param>
        private async Task PerformHealthChecksAsync(CancellationToken token)
        {
            await _healthCheckSemaphore.WaitAsync(token).ConfigureAwait(false);
            try
            {
                foreach (var node in _topology.Nodes)
                {
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    try
                    {
                        using var cn = CreateConnection(node.ConnectionString);
                        await cn.OpenAsync(token).ConfigureAwait(false);
                        using var pingCmd = cn.CreateCommand();
                        pingCmd.CommandText = "SELECT 1";
                        pingCmd.CommandType = CommandType.Text;
                        using var reader = await pingCmd.ExecuteReaderAsync(token).ConfigureAwait(false);
                        while (await reader.ReadAsync(token).ConfigureAwait(false)) { }
                        sw.Stop();
                        node.IsHealthy = true;
                        node.LastHealthCheck = DateTime.UtcNow;
                        node.AverageLatency = sw.Elapsed;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch
                    {
                        sw.Stop();
                        node.IsHealthy = false;
                        node.LastHealthCheck = DateTime.UtcNow;
                    }
                }

                // If current primary went unhealthy, attempt failover
                var primary = _currentPrimary;
                if (primary is not null && !primary.IsHealthy)
                {
                    await TriggerFailoverAsync(token).ConfigureAwait(false);
                }

                UpdateAvailableReadReplicas();
            }
            finally
            {
                _healthCheckSemaphore.Release();
            }
        }

        /// <summary>
        /// Elects a new primary node when the existing primary is determined to be unhealthy.
        /// </summary>
        /// <param name="ct">Token used to cancel the failover process.</param>
        private async Task TriggerFailoverAsync(CancellationToken ct)
        {
            await _failoverSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                // Prefer a healthy node with Primary role; otherwise any healthy node
                var healthy = _topology.Nodes.Where(n => n.IsHealthy).ToList();
                var newPrimary = healthy.FirstOrDefault(n => n.Role == DatabaseTopology.DatabaseRole.Primary)
                                 ?? healthy.FirstOrDefault();

                if (newPrimary != null)
                {
                    _logger.LogWarning("Failing over to {ConnectionString}", newPrimary.ConnectionString);
                    _currentPrimary = newPrimary;
                }
                else
                {
                    _logger.LogError("Failover attempted but no healthy nodes found");
                }
            }
            finally
            {
                _failoverSemaphore.Release();
            }
        }

        /// <summary>
        /// Disposes the manager, terminating background tasks and releasing unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;

            // Signal cancellation and properly wait for health check task
            _disposeCts.Cancel();
            _healthCheckTimer.Dispose();

            try
            {
                // RELIABILITY FIX: Use Task.WaitAsync instead of Task.Wait to avoid potential deadlock
                // Task.Wait() can deadlock in certain SynchronizationContext scenarios (e.g., WPF, WinForms)
                // WaitAsync with timeout is safer and works correctly in all contexts
                if (!_healthCheckTask.WaitAsync(TimeSpan.FromSeconds(10)).GetAwaiter().GetResult())
                {
                    // Task didn't complete in time, but we've already canceled it.
                    // The cancellation token will eventually cause it to exit.
                }
            }
            catch
            {
                // Ignore exceptions during shutdown (task was likely canceled)
            }

            _disposeCts.Dispose();

            // No need to dispose connection pools - we rely on ADO.NET provider pooling now.
            // Connections are disposed by callers, which returns them to the provider pool.

            // Only dispose semaphores after the health check task has been given a chance to complete
            _failoverSemaphore.Dispose();
            _healthCheckSemaphore.Dispose();
        }
    }
}
