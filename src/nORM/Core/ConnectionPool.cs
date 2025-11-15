using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
namespace nORM.Core
{
    /// <summary>
    /// Thread-safe pool managing database connections.
    /// </summary>
    /// <remarks>
    /// ARCHITECTURAL WARNING (TASK 5): This custom connection pool creates a second pooling layer
    /// on top of the database provider's built-in connection pooling (ADO.NET provider pooling).
    ///
    /// **Double Pooling Issues:**
    /// - SQL Server, PostgreSQL, MySQL, etc. already have efficient built-in pooling via connection strings
    /// - Having two pooling layers leads to resource inefficiency and unpredictable behavior
    /// - Connection limits become confusing (nORM MaxPoolSize + Provider Max Pool Size)
    /// - Memory and handle consumption is higher than necessary
    ///
    /// **Recommended Approach:**
    /// For most applications, rely solely on provider connection pooling:
    ///
    /// 1. **SQL Server**: Use connection string parameters (Max Pool Size, Min Pool Size, etc.)
    ///    Example: "Server=.;Database=MyDb;Integrated Security=true;Max Pool Size=100;Min Pool Size=10;"
    ///
    /// 2. **PostgreSQL (Npgsql)**: Use connection string parameters (Maximum Pool Size, Minimum Pool Size)
    ///    Example: "Host=localhost;Database=mydb;Maximum Pool Size=100;Minimum Pool Size=10;"
    ///
    /// 3. **MySQL**: Use connection string parameters (Maximum Pool Size, Minimum Pool Size)
    ///    Example: "Server=localhost;Database=mydb;Maximum Pool Size=100;Minimum Pool Size=10;"
    ///
    /// **When Custom Pooling Might Be Useful:**
    /// - Multi-tenant scenarios with many different connection strings (provider pooling per connection string)
    /// - Read replica load balancing (see <see cref="ConnectionManager"/>)
    /// - Explicit memory-aware connection limiting across multiple databases
    ///
    /// **Migration Path:**
    /// Consider refactoring to use provider pooling directly and remove this custom layer.
    /// The ConnectionManager class is better suited for topology-aware scenarios (read replicas, failover).
    /// </remarks>
    [Obsolete("Consider using provider-level connection pooling instead of custom pooling to avoid double-pooling overhead. " +
              "Configure pooling via connection string parameters. This class will be evaluated for deprecation in future versions.")]
    public sealed class ConnectionPool : IAsyncDisposable, IDisposable
    {
        private readonly Func<DbConnection> _connectionFactory;
        // PERFORMANCE FIX (TASK 15): Use ConcurrentBag instead of ConcurrentQueue to avoid
        // churning during cleanup. ConcurrentBag allows iteration without dequeue/enqueue cycles.
        private readonly ConcurrentBag<PooledItem> _pool = new();
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private readonly Timer _cleanupTimer;
        private readonly int _minSize;
        private readonly int _maxSize;
        private readonly TimeSpan _idleLifetime;
        private int _created;
        private int _returning;

        // ADD: reentrancy guard for CleanupCallback (fixes CS0103)
        private int _cleanupRunning;

        // PERFORMANCE FIX (TASK 4): Async synchronization primitive to avoid polling
        private readonly SemaphoreSlim _availableSemaphore;



        private sealed class PooledItem
        {
            public DbConnection Connection { get; }
            public DateTime LastUsed { get; set; }

            public PooledItem(DbConnection connection)
            {
                Connection = connection ?? throw new ArgumentNullException(nameof(connection));
                LastUsed = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionPool"/> class.
        /// </summary>
        /// <param name="connectionFactory">Factory used to create new <see cref="DbConnection"/> instances.</param>
        /// <param name="options">Optional pool configuration.</param>
        public ConnectionPool(Func<DbConnection> connectionFactory, ConnectionPoolOptions? options = null)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            var opts = options ?? new ConnectionPoolOptions();
            if (opts.MaxPoolSize <= 0) throw new ArgumentOutOfRangeException(nameof(options.MaxPoolSize));
            if (opts.MinPoolSize < 0 || opts.MinPoolSize > opts.MaxPoolSize)
                throw new ArgumentOutOfRangeException(nameof(options.MinPoolSize));

            _minSize = opts.MinPoolSize;
            _maxSize = opts.MaxPoolSize;
            _idleLifetime = opts.ConnectionIdleLifetime;

            // PERFORMANCE FIX (TASK 4): Initialize semaphore for wait-free connection availability signaling
            _availableSemaphore = new SemaphoreSlim(_maxSize, _maxSize);

            // Pre-warm
            if (_minSize > 0)
            {
                _ = Task.Run(async () =>
                {
                    var tasks = new List<Task>(_minSize);
                    for (int i = 0; i < _minSize; i++)
                    {
                        tasks.Add(Task.Run(async () =>
                        {
                            DbConnection? cn = null;
                            try
                            {
                                cn = _connectionFactory();
                                await cn.OpenAsync().ConfigureAwait(false);
                                _pool.Add(new PooledItem(cn));
                                Interlocked.Increment(ref _created);
                                cn = null;
                            }
                            catch (Exception ex)
                            {
                                try { cn?.Dispose(); } catch { }
                                System.Diagnostics.Debug.WriteLine($"[nORM] Pre-warm connection failed: {ex.Message}");
                            }
                        }));
                    }
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                });
            }

            _cleanupTimer = new Timer(CleanupCallback, null, _idleLifetime, _idleLifetime);
        }

        /// <summary>
        /// Rents a connection from the pool.
        /// </summary>
        public async Task<DbConnection> RentAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();

            while (true)
            {
                if (_pool.TryTake(out var item))
                {
                    // Ensure it's open and usable
                    if (item.Connection.State != ConnectionState.Open)
                    {
                        try
                        {
                            await item.Connection.OpenAsync(ct).ConfigureAwait(false);
                            item.LastUsed = DateTime.UtcNow;
                            return item.Connection;
                        }
                        catch
                        {
                            item.Connection.Dispose();
                            Interlocked.Decrement(ref _created);
                            // Signal that a connection slot is available again
                            _availableSemaphore.Release();
                            continue;
                        }
                    }

                    item.LastUsed = DateTime.UtcNow;
                    return item.Connection;
                }

                // Maybe create a new one
                if (Volatile.Read(ref _created) < _maxSize && HasSufficientMemory())
                {
                    if (Interlocked.Increment(ref _created) <= _maxSize)
                    {
                        try
                        {
                            // Acquire semaphore slot before creating connection
                            await _availableSemaphore.WaitAsync(ct).ConfigureAwait(false);
                            var cn = _connectionFactory();
                            await cn.OpenAsync(ct).ConfigureAwait(false);
                            return cn;
                        }
                        catch
                        {
                            Interlocked.Decrement(ref _created);
                            _availableSemaphore.Release();
                            throw;
                        }
                    }

                    Interlocked.Decrement(ref _created);
                }

                // PERFORMANCE FIX (TASK 4): Wait efficiently instead of polling with Task.Delay
                // Park the thread until a connection is returned to the pool
                await _availableSemaphore.WaitAsync(ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns a connection to the pool.
        /// </summary>
        public void Return(DbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            ThrowIfDisposed();

            Interlocked.Increment(ref _returning);
            try
            {
                _pool.Add(new PooledItem(connection) { LastUsed = DateTime.UtcNow });
                // PERFORMANCE FIX (TASK 4): Signal waiting threads that a connection is available
                _availableSemaphore.Release();
            }
            finally
            {
                Interlocked.Decrement(ref _returning);
            }
        }

        private void CleanupCallback(object? state)
        {
            if (_disposed) return;

            // ADD: reentrancy guard  System.Threading.Timer may overlap callbacks
            if (Interlocked.Exchange(ref _cleanupRunning, 1) == 1) return;

            try
            {
                // PERFORMANCE FIX (TASK 15): Iterate ConcurrentBag without dequeue/enqueue churn
                // ConcurrentBag allows enumeration without modification, avoiding queue churning
                var threshold = DateTime.UtcNow - _idleLifetime;
                int currentCreated = Volatile.Read(ref _created);
                int maxToDispose = currentCreated - _minSize;
                if (maxToDispose <= 0) return;

                var itemsToDispose = new List<PooledItem>();

                // Collect items that need disposal without removing them yet
                foreach (var item in _pool)
                {
                    if (item.LastUsed < threshold && itemsToDispose.Count < maxToDispose)
                    {
                        itemsToDispose.Add(item);
                    }
                }

                // Now remove and dispose collected items
                foreach (var item in itemsToDispose)
                {
                    if (_pool.TryTake(out var taken) && ReferenceEquals(taken, item))
                    {
                        try { item.Connection.Dispose(); } catch { }
                        Interlocked.Decrement(ref _created);
                    }
                    else if (taken != null)
                    {
                        // Put back if we took wrong item
                        _pool.Add(taken);
                    }
                }
            }
            finally
            {
                // ADD: release reentrancy guard
                Volatile.Write(ref _cleanupRunning, 0);
            }
        }

        private static bool HasSufficientMemory()
        {
            var info = GC.GetGCMemoryInfo();
            var available = info.TotalAvailableMemoryBytes;
            if (available <= 0) return true;
            var used = GC.GetTotalMemory(false);
            // Keep at least 10MB of free memory before creating new connections
            return available - used > 10 * 1024 * 1024;
        }

        private bool _disposed;

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ConnectionPool));
        }

        /// <summary>
        /// Disposes the connection pool and all connections currently tracked by the pool.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _cleanupTimer.Dispose();
            while (_pool.TryTake(out var item))
            {
                try { item.Connection.Dispose(); } catch { }
            }
            _semaphore.Dispose();
            // PERFORMANCE FIX (TASK 4): Dispose the availability semaphore
            _availableSemaphore.Dispose();
        }

        /// <summary>
        /// Asynchronously disposes the connection pool and all pooled connections.
        /// </summary>
        /// <returns>A task that completes when disposal is finished.</returns>
        public async ValueTask DisposeAsync()
        {
            Dispose();
            await Task.CompletedTask;
        }

        // A simple pooled wrapper for external ownership scenarios.
        private sealed class PooledDbConnection : DbConnection
        {
            private readonly ConnectionPool _pool;
            private DbConnection? _inner;
            private bool _disposed;

            // Replace lock(this) with a private lock object (best practice)
            private readonly object _disposeLock = new();

            public PooledDbConnection(ConnectionPool pool, DbConnection inner)
            {
                _pool = pool;
                _inner = inner;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing && !_disposed)
                {
                    lock (_disposeLock)
                    {
                        if (!_disposed)
                        {
                            var cn = Interlocked.Exchange(ref _inner, null);
                            if (cn != null)
                            {
                                try
                                {
                                    _pool.Return(cn);
                                }
                                catch
                                {
                                    try { cn.Dispose(); } catch { }
                                }
                            }
                            _disposed = true;
                        }
                    }
                }
            }

            /// <summary>
            /// Asynchronously disposes the pooled database connection and
            /// returns the underlying connection back to the pool.
            /// The method simply delegates to the synchronous
            /// Dispose implementation and returns a
            /// completed <see cref="ValueTask"/> for convenience.
            /// </summary>
            /// <returns>A completed task representing the asynchronous dispose operation.</returns>
            public override ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }

            // DbConnection abstract members delegated to _inner (guarded)
            [AllowNull] // applies to the setter parameter
            public override string ConnectionString
            {
                get => _inner!.ConnectionString!;
                set => _inner!.ConnectionString = value;
            }
            public override string Database => _inner!.Database;
            public override string DataSource => _inner!.DataSource;
            public override string ServerVersion => _inner!.ServerVersion;
            public override ConnectionState State => _inner!.State;

            /// <summary>
            /// Changes the current database for the underlying connection.
            /// </summary>
            /// <param name="databaseName">The name of the database to switch to.</param>
            public override void ChangeDatabase(string databaseName) => _inner!.ChangeDatabase(databaseName);

            /// <summary>
            /// Closes the underlying connection and returns it to the pool.
            /// </summary>
            public override void Close() => _inner!.Close();

            /// <summary>
            /// Opens the underlying connection.
            /// </summary>
            public override void Open() => _inner!.Open();

            /// <summary>
            /// Opens the underlying database connection asynchronously.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
            /// <returns>A task that completes when the connection has been opened.</returns>
            public override Task OpenAsync(CancellationToken cancellationToken) => _inner!.OpenAsync(cancellationToken);


            /// <summary>
            /// Begins a database transaction on the underlying connection with the
            /// specified isolation level. The resulting transaction is not pooled and
            /// must be disposed by the caller.
            /// </summary>
            /// <param name="isolationLevel">The isolation level to apply to the transaction.</param>
            /// <returns>A <see cref="DbTransaction"/> representing the started transaction.</returns>
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => _inner!.BeginTransaction(isolationLevel);

            /// <summary>
            /// Creates a command object associated with the underlying connection.
            /// </summary>
            /// <returns>A new <see cref="DbCommand"/> instance.</returns>
            protected override DbCommand CreateDbCommand() => _inner!.CreateCommand();
        }
    }
}
