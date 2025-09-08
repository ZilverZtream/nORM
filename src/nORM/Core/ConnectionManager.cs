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
            _currentPrimary = _topology.Nodes
                .Where(n => n.Role == DatabaseTopology.DatabaseRole.Primary || n.Role == DatabaseTopology.DatabaseRole.SecondaryMaster)
                .OrderBy(n => n.Priority)
                .FirstOrDefault(n => n.IsHealthy);
        }

        private void UpdateAvailableReadReplicas()
        {
            _availableReadReplicas = _topology.Nodes
                .Where(n => n.Role == DatabaseTopology.DatabaseRole.ReadReplica && n.IsHealthy)
                .OrderBy(n => n.Priority)
                .ToList();
        }

        public async Task<DbConnection> GetWriteConnectionAsync(CancellationToken ct = default)
        {
            lock (_circuitBreakerLock)
            {
                if (DateTime.UtcNow < _nextRetry)
                    throw new NormConnectionException("Circuit breaker is open; skipping connection attempt");
            }

            var primary = _currentPrimary;
            if (primary == null || !primary.IsHealthy)
            {
                await FailoverToPrimaryAsync(ct).ConfigureAwait(false);
                primary = _currentPrimary;
                if (primary == null)
                {
                    RegisterFailure();
                    throw new NormConnectionException("No healthy primary database available");
                }
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

        public async Task<DbConnection> GetReadConnectionAsync(CancellationToken ct = default)
        {
            var replicas = _availableReadReplicas;
            if (!replicas.Any())
            {
                _logger.LogWarning("No healthy read replicas available, using primary for read operation");
                return await GetWriteConnectionAsync(ct).ConfigureAwait(false);
            }

            var replica = SelectOptimalReadReplica(replicas);
            var pool = _connectionPools[replica.ConnectionString];
            try
            {
                return await pool.RentAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to connect to read replica {ConnectionString}, falling back to primary", replica.ConnectionString);
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
                var delayTicks = (long)(_baseDelay.Ticks * Math.Pow(2, _consecutiveFailures - 1));
                if (delayTicks > _maxDelay.Ticks)
                    delayTicks = _maxDelay.Ticks;
                var delay = TimeSpan.FromTicks(delayTicks);
                _nextRetry = DateTime.UtcNow + delay;
                _logger.LogWarning("Write connection failure. Circuit breaker open for {Delay}", delay);
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

        private async Task PerformHealthChecksAsync(CancellationToken token)
        {
            if (_disposed || token.IsCancellationRequested)
                return;

            if (!await _healthCheckSemaphore.WaitAsync(0, token).ConfigureAwait(false))
                return;

            try
            {
                await MonitorNodeHealthAsync(token).ConfigureAwait(false);

                if (_disposed || token.IsCancellationRequested)
                    return;

                await _failoverSemaphore.WaitAsync(token).ConfigureAwait(false);
                try
                {
                    RefreshTopologyState();
                }
                finally
                {
                    _failoverSemaphore.Release();
                }
            }
            catch (OperationCanceledException)
            {
                // Cancellation requested; exit gracefully
            }
            finally
            {
                _healthCheckSemaphore.Release();
            }
        }

        private async Task MonitorNodeHealthAsync(CancellationToken token)
        {
            if (_disposed || token.IsCancellationRequested)
                return;

            try
            {
                foreach (var node in _topology.Nodes)
                {
                    if (_disposed || token.IsCancellationRequested)
                        break;

                    var pool = _connectionPools[node.ConnectionString];
                    DbConnection? cn = null;
                    try
                    {
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        cn = await pool.RentAsync(token).ConfigureAwait(false);
                        sw.Stop();
                        node.IsHealthy = true;
                        node.LastHealthCheck = DateTime.UtcNow;
                        node.AverageLatency = sw.Elapsed;
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation requested; exit iteration after cleanup
                        break;
                    }
                    catch
                    {
                        node.IsHealthy = false;
                        node.LastHealthCheck = DateTime.UtcNow;
                    }
                    finally
                    {
                        if (cn != null)
                            await cn.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Cancellation requested; exit gracefully
            }
        }

        private void RefreshTopologyState()
        {
            DeterminePrimaryNode();
            UpdateAvailableReadReplicas();
        }

        private async Task FailoverToPrimaryAsync(CancellationToken ct)
        {
            await _failoverSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                RefreshTopologyState();
                if (_currentPrimary == null)
                {
                    _logger.LogError("Failover attempted but no healthy primary nodes found");
                }
            }
            finally
            {
                _failoverSemaphore.Release();
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            _disposeCts.Cancel();
            _healthCheckTimer.Dispose();
            try
            {
                _healthCheckTask.Wait();
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
