using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        private readonly object _poolLock = new();
        private readonly SemaphoreSlim _semaphore;
        private readonly Timer _cleanupTimer;
        private readonly int _minSize;
        private readonly TimeSpan _idleLifetime;
        private int _created;
        private bool _disposed;

        private sealed class PooledItem
        {
            public DbConnection Connection { get; }
            public DateTime LastUsed { get; set; }

            public PooledItem(DbConnection connection)
            {
                Connection = connection;
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

            _semaphore = new SemaphoreSlim(opts.MaxPoolSize, opts.MaxPoolSize);
            _minSize = opts.MinPoolSize;
            _idleLifetime = opts.ConnectionIdleLifetime;

            // Pre-warm pool with minimum number of connections
            for (int i = 0; i < _minSize; i++)
            {
                var cn = connectionFactory();
                cn.Open();
                _pool.Enqueue(new PooledItem(cn));
                Interlocked.Increment(ref _created);
            }

            _cleanupTimer = new Timer(CleanupCallback, null, _idleLifetime, _idleLifetime);
        }

        /// <summary>
        /// Rents a connection from the pool.
        /// </summary>
        public async Task<DbConnection> RentAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            await _semaphore.WaitAsync(ct).ConfigureAwait(false);
            PooledItem? item = null;
            lock (_poolLock)
            {
                _pool.TryDequeue(out item);
            }

            if (item != null)
            {
                try
                {
                    if (item.Connection.State != ConnectionState.Open)
                        await item.Connection.OpenAsync(ct).ConfigureAwait(false);
                }
                catch
                {
                    item.Connection.Dispose();
                    Interlocked.Decrement(ref _created);
                    return await RentAsync(ct).ConfigureAwait(false);
                }
                item.LastUsed = DateTime.UtcNow;
                return new PooledDbConnection(this, item.Connection);
            }

            var cn = _connectionFactory();
            await cn.OpenAsync(ct).ConfigureAwait(false);
            Interlocked.Increment(ref _created);
            return new PooledDbConnection(this, cn);
        }

        private void Return(DbConnection connection)
        {
            lock (_poolLock)
            {
                if (_disposed)
                {
                    connection.Dispose();
                    return;
                }

                if (connection.State == ConnectionState.Open)
                {
                    _pool.Enqueue(new PooledItem(connection));
                }
                else
                {
                    connection.Dispose();
                    Interlocked.Decrement(ref _created);
                }
            }
            _semaphore.Release();
        }

        private void CleanupCallback(object? state)
        {
            if (_disposed) return;
            var threshold = DateTime.UtcNow - _idleLifetime;

            var itemsToRequeue = new List<PooledItem>();
            lock (_poolLock)
            {
                while (_created > _minSize && _pool.TryDequeue(out var item))
                {
                    if (item.LastUsed < threshold)
                    {
                        item.Connection.Dispose();
                        Interlocked.Decrement(ref _created);
                    }
                    else
                    {
                        itemsToRequeue.Add(item);
                    }
                }

                foreach (var item in itemsToRequeue)
                    _pool.Enqueue(item);
            }
        }

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
                item.Connection.Dispose();
            }
            _semaphore.Dispose();
        }

        public async ValueTask DisposeAsync()
        {
            Dispose();
            await Task.CompletedTask.ConfigureAwait(false);
        }

        private sealed class PooledDbConnection : DbConnection
        {
            private readonly ConnectionPool _pool;
            private DbConnection? _inner;

            public PooledDbConnection(ConnectionPool pool, DbConnection inner)
            {
                _pool = pool;
                _inner = inner;
            }

            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
                => Inner.BeginTransaction(isolationLevel);

            public override void ChangeDatabase(string database) => Inner.ChangeDatabase(database);

            public override void Close() => Dispose();

            public override void Open() => Inner.Open();

            public override Task OpenAsync(CancellationToken cancellationToken) => Inner.OpenAsync(cancellationToken);

            [System.Diagnostics.CodeAnalysis.AllowNull]
            public override string ConnectionString
            {
                get => Inner.ConnectionString;
                set => Inner.ConnectionString = value;
            }

            public override string Database => Inner.Database;
            public override string DataSource => Inner.DataSource;
            public override string ServerVersion => Inner.ServerVersion;
            public override ConnectionState State => Inner.State;
            protected override DbCommand CreateDbCommand() => Inner.CreateCommand();

            private DbConnection Inner => _inner ?? throw new ObjectDisposedException(nameof(PooledDbConnection));

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    var cn = Interlocked.Exchange(ref _inner, null);
                    if (cn != null)
                        _pool.Return(cn);
                }
            }

            public override ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}
