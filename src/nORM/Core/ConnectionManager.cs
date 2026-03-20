using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
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
        // ── Named constants ─────────────────────────────────────────────────
        private const int DisposeWaitTimeoutSeconds = 10;
        private const int RedactedHashPrefixLength = 8;
        private const int MaxExponentialBackoffExponent = 30;
        private const string HealthCheckPingSql = "SELECT 1";

        private readonly DatabaseTopology _topology;
        private readonly DatabaseProvider _provider;
        private readonly ILogger _logger;
        private readonly PeriodicTimer _healthCheckTimer;
        private readonly Task _healthCheckTask;
        private readonly SemaphoreSlim _failoverSemaphore = new(1, 1);
        private readonly SemaphoreSlim _healthCheckSemaphore = new(1, 1);
        private readonly CancellationTokenSource _disposeCts = new();

        private volatile bool _disposed;

        private volatile DatabaseTopology.DatabaseNode? _currentPrimary;
        private volatile List<DatabaseTopology.DatabaseNode> _availableReadReplicas = new();
        private int _readReplicaIndex;

        private readonly object _circuitBreakerLock = new();
        // Synchronizes writes to _availableReadReplicas so concurrent readers always
        // observe a fully constructed list reference (publish/acquire pattern).
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

            if (_topology.Nodes.Count == 0)
                _logger.LogWarning("ConnectionManager created with an empty topology; no nodes are available");

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
            // Synchronize write to _availableReadReplicas so concurrent readers always
            // observe a fully constructed list reference.
            lock (_replicaListLock)
            {
                _availableReadReplicas = _topology.Nodes
                    .Where(n => n.Role == DatabaseTopology.DatabaseRole.ReadReplica && n.IsHealthy)
                    .OrderBy(n => n.Priority)
                    .ToList();
            }
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ConnectionManager));
        }

        /// <summary>
        /// Retrieves an open connection to the primary node for write operations.
        /// Implements a simple circuit breaker and failover mechanism.
        /// The caller must dispose the returned connection.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An open <see cref="DbConnection"/> suitable for write operations.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when this instance has been disposed.</exception>
        /// <exception cref="InvalidOperationException">Thrown when no primary node is available.</exception>
        public async Task<DbConnection> GetWriteConnectionAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();

            lock (_circuitBreakerLock)
            {
                if (DateTime.UtcNow < _nextRetry)
                    throw new NormConnectionException("Circuit breaker is open: delaying write connection attempts.");
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
                // Dispose failed connection before re-throwing.
                // Don't trip circuit breaker on timeout/cancellation —
                // timeout is a query-level issue, not a connection-level failure.
                cn?.Dispose();
                throw;
            }
            catch (DbException ex)
            {
                cn?.Dispose();
                // Trip the circuit breaker for all database connection errors
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to database error");
                throw;
            }
            catch (System.Net.Sockets.SocketException ex)
            {
                cn?.Dispose();
                // Detect network failures by exception type
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to network socket error");
                throw;
            }
            catch (System.IO.IOException ex) when (IsNetworkIOException(ex))
            {
                cn?.Dispose();
                // Some network errors manifest as IOException with a SocketException inner
                RegisterFailure();
                _logger.LogError(ex, "Failed to acquire write connection due to network I/O error");
                throw;
            }
            catch (Exception ex)
            {
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
        /// <exception cref="ObjectDisposedException">Thrown when this instance has been disposed.</exception>
        public async Task<DbConnection> GetReadConnectionAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();

            // Snapshot the current replica list reference under the lock. The snapshot
            // is intentionally stale after this point: a concurrent health-check thread
            // may swap _availableReadReplicas to a new list while we iterate. This is
            // safe because (1) List<T> is never mutated in-place — updates always publish
            // a new list instance, (2) the worst case is selecting a replica that was
            // just marked unhealthy, which will fail-and-fallback to the primary below,
            // and (3) avoiding a lock around the entire connection attempt prevents
            // head-of-line blocking when a replica is slow to respond.
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
                // Dispose failed connection before re-throwing
                cn?.Dispose();
                throw;
            }
            catch (Exception ex)
            {
                // Dispose failed connection before falling back to primary
                cn?.Dispose();
                _logger.LogWarning(ex, "Failed to acquire connection from read replica {Replica}", RedactConnectionString(replica.ConnectionString));
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
        /// <exception cref="ArgumentException">Thrown when <paramref name="replicas"/> is empty.</exception>
        private DatabaseTopology.DatabaseNode SelectOptimalReadReplica(IReadOnlyList<DatabaseTopology.DatabaseNode> replicas)
        {
            var count = replicas.Count;
            if (count == 0)
                throw new ArgumentException("Replica list must not be empty.", nameof(replicas));

            // After ~2.1 billion increments, _readReplicaIndex overflows to negative.
            // Casting to uint treats it as unsigned, preventing negative modulo results.
            var index = (uint)Interlocked.Increment(ref _readReplicaIndex);
            return replicas[(int)(index % (uint)count)];
        }

        private void RegisterFailure()
        {
            lock (_circuitBreakerLock)
            {
                _consecutiveFailures++;
                // Cap the exponent to prevent Math.Pow overflow: 2^31 exceeds
                // int.MaxValue and at 2^1024 double overflows to Infinity. With
                // _baseDelay=1s, 2^30 already far exceeds _maxDelay so the Math.Min
                // clamp is the effective bound, but capping the exponent avoids
                // degenerate floating-point behavior if _consecutiveFailures wraps
                // negative after int overflow (Math.Pow(2, negative) -> near-zero delay).
                var cappedFailures = Math.Min(_consecutiveFailures, MaxExponentialBackoffExponent);
                var delay = TimeSpan.FromMilliseconds(Math.Min(_baseDelay.TotalMilliseconds * Math.Pow(2, cappedFailures), _maxDelay.TotalMilliseconds));
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
        /// Determines if a database exception represents a transient error (network,
        /// timeout, or service-unavailable). Checks SQL Server error codes via
        /// reflection, then falls back to walking the inner-exception chain for
        /// <see cref="System.Net.Sockets.SocketException"/>.
        /// </summary>
        /// <param name="ex">The database exception to check.</param>
        /// <returns>True if the exception represents a transient connection/network error.</returns>
        internal static bool IsTransientDatabaseError(DbException ex)
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
        /// Checks whether an <see cref="System.IO.IOException"/> is caused by a network
        /// error by walking its inner-exception chain for a
        /// <see cref="System.Net.Sockets.SocketException"/>.
        /// </summary>
        private static bool IsNetworkIOException(System.IO.IOException ex)
        {
            return HasSocketExceptionInChain(ex);
        }

        // ── Connection-string credential redaction ──────────────────────────

        private static readonly HashSet<string> _sensitiveConnStrKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            "password", "pwd", "user password",
            "access token", "accesstoken", "token", "secret"
        };

        /// <summary>
        /// Returns a copy of <paramref name="connectionString"/> with sensitive values
        /// (Password, Token, Secret, etc.) replaced by <c>***</c>. Safe to log.
        /// </summary>
        internal static string RedactConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return string.Empty;

            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
                foreach (var key in _sensitiveConnStrKeys)
                {
                    if (builder.ContainsKey(key))
                        builder[key] = "***";
                }
                return builder.ConnectionString;
            }
            catch (ArgumentException)
            {
                // Malformed connection string -- emit a SHA-256 hash prefix rather than the raw value.
                var hash = SHA256.HashData(Encoding.UTF8.GetBytes(connectionString));
                return $"[redacted:{Convert.ToHexString(hash)[..RedactedHashPrefixLength]}]";
            }
            catch (FormatException)
            {
                // Some providers throw FormatException for invalid connection strings.
                var hash = SHA256.HashData(Encoding.UTF8.GetBytes(connectionString));
                return $"[redacted:{Convert.ToHexString(hash)[..RedactedHashPrefixLength]}]";
            }
        }

        /// <summary>
        /// Walks the inner-exception chain looking for a
        /// <see cref="System.Net.Sockets.SocketException"/>.
        /// </summary>
        /// <param name="ex">The root exception to inspect.</param>
        /// <returns><c>true</c> if a <see cref="System.Net.Sockets.SocketException"/> is found in the chain.</returns>
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
            while (true)
            {
                try
                {
                    await PerformHealthChecksAsync(token).ConfigureAwait(false);

                    if (!await _healthCheckTimer.WaitForNextTickAsync(token).ConfigureAwait(false))
                        break;
                }
                catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
                {
                    break; // Normal shutdown
                }
                catch (Exception ex)
                {
                    // Log and continue -- don't let health check loop die silently
                    try { _logger.LogWarning(ex, "Health check loop encountered an error"); }
                    catch { /* logging itself must not kill the loop */ }
                }
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
                        pingCmd.CommandText = HealthCheckPingSql;
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
                    catch (Exception ex)
                    {
                        sw.Stop();
                        node.IsHealthy = false;
                        node.LastHealthCheck = DateTime.UtcNow;
                        _logger.LogDebug(ex, "Health check failed for node {Node}", RedactConnectionString(node.ConnectionString));
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
                    _logger.LogWarning("Failing over to {ConnectionString}", RedactConnectionString(newPrimary.ConnectionString));
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
                // Use Task.WaitAsync instead of Task.Wait to avoid potential deadlock.
                // Task.Wait() can deadlock in certain SynchronizationContext scenarios (e.g., WPF, WinForms).
                // WaitAsync with timeout is safer and works correctly in all contexts.
                _healthCheckTask.WaitAsync(TimeSpan.FromSeconds(DisposeWaitTimeoutSeconds)).GetAwaiter().GetResult();
            }
            catch (TimeoutException)
            {
                // Task didn't complete in time, but we've already canceled it.
                // The cancellation token will eventually cause it to exit.
            }
            catch (OperationCanceledException)
            {
                // Expected during normal shutdown -- the health-check task was cancelled.
            }
            catch (Exception ex)
            {
                // Unexpected error during shutdown; log at debug level rather than swallowing silently.
                try { _logger.LogDebug(ex, "Exception during ConnectionManager health-check task shutdown"); }
                catch { /* logging must not throw during dispose */ }
            }

            _disposeCts.Dispose();

            // No need to dispose connection pools -- we rely on ADO.NET provider pooling now.
            // Connections are disposed by callers, which returns them to the provider pool.

            // Only dispose semaphores when the health task has actually terminated.
            // If the task is still running (timed out above), it holds references to both
            // semaphores and will call Release() in its finally block -- disposing them here
            // would produce ObjectDisposedException in the still-running background code.
            // SemaphoreSlim is a tiny managed object; GC handles it safely in the rare timeout path.
            if (_healthCheckTask.IsCompleted)
            {
                _failoverSemaphore.Dispose();
                _healthCheckSemaphore.Dispose();
            }

            GC.SuppressFinalize(this);
        }
    }
}
