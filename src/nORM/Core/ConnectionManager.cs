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
        private int _consecutiveFailures;
        private DateTime _nextRetry = DateTime.MinValue;
        private readonly TimeSpan _baseDelay = TimeSpan.FromSeconds(1);
        private readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(30);

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

        private DbConnection CreateConnection(string connectionString)
        {
            return DbConnectionFactory.Create(connectionString, _provider);
        }

        private void DeterminePrimaryNode()
        {
            // Choose the first node marked as Primary role; fallback to first node
            var primary = _topology.Nodes.FirstOrDefault(n => n.Role == DatabaseTopology.DatabaseRole.Primary)
                          ?? _topology.Nodes.FirstOrDefault();
            _currentPrimary = primary;
        }

        private void UpdateAvailableReadReplicas()
        {
            _availableReadReplicas = _topology.Nodes
                .Where(n => n.Role == DatabaseTopology.DatabaseRole.ReadReplica && n.IsHealthy)
                .OrderBy(n => n.Priority)
                .ToList();
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
                throw;
            }
            catch (Exception ex)
            {
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection");
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
            var replicas = _availableReadReplicas;
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
