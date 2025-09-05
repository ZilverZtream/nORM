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
        private readonly Timer _healthCheckTimer;
        private readonly SemaphoreSlim _failoverSemaphore = new(1, 1);
        private readonly SemaphoreSlim _poolInitSemaphore = new(1, 1);
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

        public ConnectionManager(DatabaseTopology topology, DatabaseProvider provider, ILogger logger)
        {
            _topology = topology ?? throw new ArgumentNullException(nameof(topology));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            InitializeConnectionPools();
            DeterminePrimaryNode();
            UpdateAvailableReadReplicas();

            _healthCheckTimer = new Timer(PerformHealthChecks, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));
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

        private async void PerformHealthChecks(object? state)
        {
            if (_disposed || _disposeCts.IsCancellationRequested)
                return;

            await MonitorNodeHealthAsync(_disposeCts.Token).ConfigureAwait(false);

            if (_disposed || _disposeCts.IsCancellationRequested)
                return;

            RefreshTopologyState();
        }

        private async Task MonitorNodeHealthAsync(CancellationToken token)
        {
            if (_disposed || token.IsCancellationRequested)
                return;

            try
            {
                await Parallel.ForEachAsync(_topology.Nodes, token, async (node, ct) =>
                {
                    if (_disposed || ct.IsCancellationRequested)
                        return;

                    var pool = _connectionPools[node.ConnectionString];
                    try
                    {
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        await using var cn = await pool.RentAsync(ct).ConfigureAwait(false);
                        sw.Stop();
                        node.IsHealthy = true;
                        node.LastHealthCheck = DateTime.UtcNow;
                        node.AverageLatency = sw.Elapsed;
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation requested; exit iteration
                    }
                    catch
                    {
                        node.IsHealthy = false;
                        node.LastHealthCheck = DateTime.UtcNow;
                    }
                }).ConfigureAwait(false);
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
            _disposeCts.Dispose();

            foreach (var pool in _connectionPools.Values)
            {
                pool.Dispose();
            }

            _failoverSemaphore.Dispose();
            _poolInitSemaphore.Dispose();
        }
    }
}
