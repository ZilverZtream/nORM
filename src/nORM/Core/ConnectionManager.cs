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
            var primary = _currentPrimary;
            if (primary == null || !primary.IsHealthy)
            {
                await FailoverToPrimaryAsync(ct).ConfigureAwait(false);
                primary = _currentPrimary;
                if (primary == null)
                    throw new NormConnectionException("No healthy primary database available");
            }

            var pool = _connectionPools[primary.ConnectionString];
            return await pool.RentAsync(ct).ConfigureAwait(false);
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
            foreach (var node in _topology.Nodes)
            {
                if (_disposed || token.IsCancellationRequested)
                    break;

                var pool = _connectionPools[node.ConnectionString];
                try
                {
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    await using var cn = await pool.RentAsync(token).ConfigureAwait(false);
                    sw.Stop();
                    node.IsHealthy = true;
                    node.LastHealthCheck = DateTime.UtcNow;
                    node.AverageLatency = sw.Elapsed;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch
                {
                    node.IsHealthy = false;
                    node.LastHealthCheck = DateTime.UtcNow;
                }
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
