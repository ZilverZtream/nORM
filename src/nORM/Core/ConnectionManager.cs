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
    /// NOTE (TASK 5): ConnectionManager uses <see cref="ConnectionPool"/> internally for managing
    /// connections to multiple database nodes (primary + read replicas). This is one of the legitimate
    /// use cases for custom pooling, as it enables:
    /// - Round-robin load balancing across read replicas
    /// - Automatic failover when a node becomes unhealthy
    /// - Per-node connection pooling with different connection strings
    ///
    /// However, be aware that provider-level pooling (in the connection string) still applies,
    /// creating a two-tier pooling architecture. For simple single-database scenarios without
    /// read replicas, prefer using DbContext directly with provider pooling only.
    /// </remarks>
    public class ConnectionManager : IDisposable
    {
        private readonly DatabaseTopology _topology;
        private readonly DatabaseProvider _provider;
        private readonly ILogger _logger;
        private readonly PeriodicTimer _healthCheckTimer;
        private readonly Task _healthCheckTask;
        private readonly SemaphoreSlim _failoverSemaphore = new(1, 1);
        private readonly SemaphoreSlim _poolInitSemaphore = new(1, 1);
        private readonly SemaphoreSlim _healthCheckSemaphore = new(1, 1);
        private readonly CancellationTokenSource _disposeCts = new();

        private bool _disposed;

        private readonly ConcurrentDictionary<string, ConnectionPool> _connectionPools = new();
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
        /// across a topology of nodes. Connection pools are created for each node and an
        /// optional background health check loop is started.
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

            InitializeConnectionPools();
            DeterminePrimaryNode();
            UpdateAvailableReadReplicas();

            var interval = healthCheckInterval ?? TimeSpan.FromSeconds(30);
            _healthCheckTimer = new PeriodicTimer(interval);
            _healthCheckTask = Task.Run(() => HealthCheckLoopAsync(_disposeCts.Token));
        }

        /// <summary>
        /// Initializes a connection pool for each node defined in the database topology.
        /// This method is guarded by a semaphore to ensure pools are created only once
        /// in multi-threaded scenarios.
        /// </summary>
        private void InitializeConnectionPools()
        {
            _poolInitSemaphore.Wait();
            try
            {
                foreach (var node in _topology.Nodes)
                {
                    _connectionPools.GetOrAdd(node.ConnectionString, cs => new ConnectionPool(() => CreateConnection(cs)));
                }
            }
            finally
            {
                _poolInitSemaphore.Release();
            }
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

            var pool = _connectionPools[primary.ConnectionString];
            try
            {
                var cn = await pool.RentAsync(ct).ConfigureAwait(false);
                ResetCircuitBreaker();
                return cn;
            }
            catch (OperationCanceledException)
            {
                // CIRCUIT BREAKER FIX (TASK 17): Don't trip circuit breaker on timeout/cancellation.
                // Timeout is a query-level issue, not a connection-level failure.
                throw;
            }
            catch (DbException ex)
            {
                // CIRCUIT BREAKER FIX (TASK 17): Only trip on connection-level DB exceptions
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to database error");
                throw;
            }
            catch (Exception ex) when (ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase))
            {
                // CIRCUIT BREAKER FIX (TASK 17): Trip on network/connection-related exceptions
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to network/connection issue");
                throw;
            }
            catch (Exception ex)
            {
                // Other exceptions (e.g., application errors) don't trip the circuit breaker
                _logger.LogError(ex, "Failed to acquire write connection (non-connection error)");
                throw;
            }
        }

        /// <summary>
        /// Retrieves an open connection optimized for read operations, using available read replicas
        /// and falling back to the primary node when necessary.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An open <see cref="DbConnection"/> suitable for read operations.</returns>
        public async Task<DbConnection> GetReadConnectionAsync(CancellationToken ct = default)
        {
            // RACE CONDITION FIX (TASK 11): Synchronize read of _availableReadReplicas
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
            var pool = _connectionPools[replica.ConnectionString];
            try
            {
                return await pool.RentAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
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
            var index = Interlocked.Increment(ref _readReplicaIndex);
            return replicas[index % replicas.Count];
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
                    var pool = _connectionPools[node.ConnectionString];
                    DbConnection? cn = null;
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    try
                    {
                        cn = await pool.RentAsync(token).ConfigureAwait(false);
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
                    finally
                    {
                        if (cn != null)
                        {
                            try { pool.Return(cn); } catch { /* ignore */ }
                        }
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
        /// Disposes the manager and all pooled connections, terminating background tasks
        /// and releasing unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            _disposeCts.Cancel();
            _healthCheckTimer.Dispose();
            try
            {
                // Avoid indefinite blocking on shutdown
                Task.WhenAny(_healthCheckTask, Task.Delay(TimeSpan.FromSeconds(5))).GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore exceptions during shutdown
            }
            _disposeCts.Dispose();

            foreach (var pool in _connectionPools.Values)
            {
                pool.Dispose();
            }

            _failoverSemaphore.Dispose();
            _poolInitSemaphore.Dispose();
            _healthCheckSemaphore.Dispose();
        }
    }
}
