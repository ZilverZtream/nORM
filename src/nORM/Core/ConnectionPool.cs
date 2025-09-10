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
    public sealed class ConnectionPool : IAsyncDisposable, IDisposable
    {
        private readonly Func<DbConnection> _connectionFactory;
        private readonly ConcurrentQueue<PooledItem> _pool = new();
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private readonly Timer _cleanupTimer;
        private readonly int _minSize;
        private readonly int _maxSize;
        private readonly TimeSpan _idleLifetime;
        private int _created;
        private int _returning;

        // ADD: reentrancy guard for CleanupCallback (fixes CS0103)
        private int _cleanupRunning;



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
                                _pool.Enqueue(new PooledItem(cn));
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
                if (_pool.TryDequeue(out var item))
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
                            var cn = _connectionFactory();
                            await cn.OpenAsync(ct).ConfigureAwait(false);
                            return cn;
                        }
                        catch
                        {
                            Interlocked.Decrement(ref _created);
                            throw;
                        }
                    }

                    Interlocked.Decrement(ref _created);
                }

                // Otherwise, wait for someone to return
                await Task.Delay(10, ct).ConfigureAwait(false);
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
                _pool.Enqueue(new PooledItem(connection) { LastUsed = DateTime.UtcNow });
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
                var threshold = DateTime.UtcNow - _idleLifetime;
                var itemsToRequeue = new List<PooledItem>();
                int currentCreated = Volatile.Read(ref _created);
                int maxToDispose = currentCreated - _minSize;
                if (maxToDispose <= 0) return;

                int checkedCount = 0;
                const int MaxCheckFactor = 2;
                int maxCheck = maxToDispose * MaxCheckFactor;

                while (checkedCount < maxCheck && maxToDispose > 0 && _pool.TryDequeue(out var item))
                {
                    checkedCount++;

                    if (item.LastUsed < threshold)
                    {
                        try { item.Connection.Dispose(); } catch { }
                        Interlocked.Decrement(ref _created);
                        maxToDispose--;
                        continue;
                    }

                    itemsToRequeue.Add(item);
                }

                foreach (var item in itemsToRequeue)
                    _pool.Enqueue(item);
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

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _cleanupTimer.Dispose();
            while (_pool.TryDequeue(out var item))
            {
                try { item.Connection.Dispose(); } catch { }
            }
            _semaphore.Dispose();
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
            public override void ChangeDatabase(string databaseName) => _inner!.ChangeDatabase(databaseName);
            public override void Close() => _inner!.Close();
            public override void Open() => _inner!.Open();
            public override Task OpenAsync(CancellationToken cancellationToken) => _inner!.OpenAsync(cancellationToken);
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => _inner!.BeginTransaction(isolationLevel);
            protected override DbCommand CreateDbCommand() => _inner!.CreateCommand();
        }
    }
}
