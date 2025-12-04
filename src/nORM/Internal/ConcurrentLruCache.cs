using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

#nullable enable

namespace nORM.Internal
{
    /// <summary>
    /// Concurrent LRU cache with optional TTL. Expired entries are removed on access.
    /// Eviction on insert is size-based only (no TTL prune on insert) to minimize churn in hot paths.
    /// </summary>
    public class ConcurrentLruCache<TKey, TValue> : IDisposable where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheItem>> _cache = new();
        private readonly LinkedList<CacheItem> _lruList = new();
        // PERFORMANCE FIX (TASK 2): Use ReaderWriterLockSlim for better read concurrency
        // Multiple readers can access the LRU list simultaneously, while writes are exclusive
        private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);

        private int _maxSize;
        private readonly TimeSpan? _timeToLive;
        private long _hits;
        private long _misses;

        // PERFORMANCE FIX (TASK 6): Lazy LRU using timestamps instead of list manipulation
        // Update LastAccessed with Interlocked (no lock required), defer structural promotion
        // This allows true lock-free reads while maintaining approximate LRU semantics

        /// <summary>
        /// Initializes a new concurrent LRU cache.
        /// </summary>
        /// <param name="maxSize">Maximum number of items the cache can hold.</param>
        /// <param name="timeToLive">Optional time-to-live for cache entries.</param>
        public ConcurrentLruCache(int maxSize = 1000, TimeSpan? timeToLive = null)
        {
            if (maxSize <= 0) throw new ArgumentOutOfRangeException(nameof(maxSize));
            _maxSize = maxSize;
            _timeToLive = timeToLive;
        }

        /// <summary>
        /// Sets the maximum number of entries the cache can hold. If the new size is
        /// smaller than the current number of items, least recently used entries are evicted.
        /// </summary>
        /// <param name="maxSize">The new cache capacity.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxSize"/> is less than or equal to zero.</exception>
        public void SetMaxSize(int maxSize)
        {
            if (maxSize <= 0) throw new ArgumentOutOfRangeException(nameof(maxSize));
            _lock.EnterWriteLock();
            try
            {
                _maxSize = maxSize;
                while (_lruList.Count > _maxSize)
                {
                    var last = _lruList.Last!;
                    _lruList.RemoveLast();
                    _cache.TryRemove(last.Value.Key, out _);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Clears all entries from the cache and resets hit/miss statistics.
        /// </summary>
        public void Clear()
        {
            _lock.EnterWriteLock();
            try
            {
                _cache.Clear();
                _lruList.Clear();
                Interlocked.Exchange(ref _hits, 0);
                Interlocked.Exchange(ref _misses, 0);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Attempts to retrieve a value from the cache. If found and not expired the entry's
        /// last accessed timestamp is updated lock-free using Interlocked operations.
        /// </summary>
        /// <param name="key">Key of the cached item.</param>
        /// <param name="value">When this method returns, contains the cached value if found.</param>
        /// <returns><c>true</c> if the value was found in the cache; otherwise <c>false</c>.</returns>
        public bool TryGet(TKey key, out TValue value)
        {
            // PERFORMANCE FIX (TASK 6): Lock-free read path with Interlocked timestamp update
            // This allows thousands of concurrent reads without blocking
            if (_cache.TryGetValue(key, out var node))
            {
                var item = node.Value;

                // Check expiration without locking
                if (!IsExpired(item))
                {
                    // PERFORMANCE FIX (TASK 6): Update LastAccessed using Interlocked (no lock!)
                    // Convert DateTime to long ticks for atomic update
                    var nowTicks = DateTime.UtcNow.Ticks;
                    Interlocked.Exchange(ref item.LastAccessedTicks, nowTicks);

                    Interlocked.Increment(ref _hits);
                    value = item.Value;
                    return true;
                }

                // Expired: remove (requires write lock, but this is rare)
                _lock.EnterWriteLock();
                try
                {
                    // Re-check after acquiring lock (double-check pattern)
                    if (_cache.TryGetValue(key, out var nodeToRemove) && nodeToRemove.List != null)
                    {
                        _lruList.Remove(nodeToRemove);
                        _cache.TryRemove(key, out _);
                    }
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }

            Interlocked.Increment(ref _misses);
            value = default!;
            return false;
        }

        /// <summary>
        /// Adds or returns existing value. The factory runs only if missing/expired.
        /// </summary>
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (valueFactory is null) throw new ArgumentNullException(nameof(valueFactory));
            if (TryGet(key, out var existing))
                return existing;

            var created = valueFactory(key); // compute outside lock
            Set(key, created);
            return created;
        }

        /// <summary>
        /// Sets/replaces a value. TTL override and dependencies are accepted for call-site compatibility.
        /// </summary>
        public void Set(TKey key, TValue value, TimeSpan? ttlOverride = null, IReadOnlyList<string>? dependencies = null)
        {
            var nowUtc = DateTimeOffset.UtcNow;
            var item = new CacheItem(key, value, nowUtc, ttlOverride, nowUtc.UtcDateTime.Ticks);

            _lock.EnterWriteLock();
            try
            {
                if (_cache.TryGetValue(key, out var existing))
                {
                    if (existing.List != null)
                        _lruList.Remove(existing);
                    var newNode = new LinkedListNode<CacheItem>(item);
                    _cache[key] = newNode;
                    _lruList.AddFirst(newNode);
                }
                else
                {
                    var newNode = new LinkedListNode<CacheItem>(item);
                    _cache[key] = newNode;
                    _lruList.AddFirst(newNode);
                }

                // PERFORMANCE FIX (TASK 3): Sample tail portion to choose eviction candidate
                // LRU CACHE EVICTION FIX: Scale sample window with cache size for better hit rate
                // Fixed window of 20 is insufficient for large caches (only 2% of 1000-entry cache)
                if (_lruList.Count > _maxSize)
                {
                    // Sample 10% of cache entries with min=20, max=100 for balanced performance
                    // This adapts to cache size: small caches sample more %, large caches cap at 100
                    var sampleWindow = Math.Min(100, Math.Max(20, _maxSize / 10));

                    LinkedListNode<CacheItem>? oldestNode = null;
                    long oldestTicks = long.MaxValue;

                    // Walk sampled nodes starting from the tail (LRU end)
                    var node = _lruList.Last;
                    for (int i = 0; i < sampleWindow && node != null; i++)
                    {
                        var ticks = Interlocked.Read(ref node.Value.LastAccessedTicks);
                        if (ticks < oldestTicks)
                        {
                            oldestTicks = ticks;
                            oldestNode = node;
                        }

                        node = node.Previous;
                    }

                    // Fallback: if sampling didn't find anything, evict the last node (tail of list)
                    if (oldestNode == null)
                        oldestNode = _lruList.Last;

                    if (oldestNode != null)
                    {
                        _lruList.Remove(oldestNode);
                        _cache.TryRemove(oldestNode.Value.Key, out _);
                    }
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets the total number of cache hits.
        /// </summary>
        public long Hits => Interlocked.Read(ref _hits);

        /// <summary>
        /// Gets the total number of cache misses.
        /// </summary>
        public long Misses => Interlocked.Read(ref _misses);

        /// <summary>
        /// Gets the ratio of hits to total lookups.
        /// </summary>
        public double HitRate => Hits + Misses == 0 ? 0 : (double)Hits / (Hits + Misses);

        /// <summary>
        /// Determines whether the specified cache <paramref name="item"/> has
        /// exceeded its time-to-live and should be considered expired.
        /// </summary>
        /// <param name="item">The cache entry to inspect.</param>
        /// <returns><c>true</c> if the entry is expired; otherwise <c>false</c>.</returns>
        private bool IsExpired(CacheItem item)
        {
            var ttl = item.TtlOverride ?? _timeToLive;
            return ttl.HasValue && DateTimeOffset.UtcNow - item.Created > ttl.Value;
        }

        /// <summary>
        /// Represents a cache entry along with its creation time, optional TTL override,
        /// and last accessed timestamp (stored as ticks for Interlocked updates).
        /// </summary>
        /// <remarks>
        /// <para><strong>Key</strong>: The key associated with the cached value.</para>
        /// <para><strong>Value</strong>: The cached value.</para>
        /// <para><strong>Created</strong>: Timestamp indicating when the entry was created.</para>
        /// <para><strong>TtlOverride</strong>: Optional TTL overriding the cache's default.</para>
        /// <para><strong>LastAccessedTicks</strong>: Mutable field storing last access time as ticks. Updated via Interlocked for lock-free reads.
        /// PERFORMANCE FIX (TASK 6): This allows updating access time without acquiring write locks.</para>
        /// </remarks>
        private sealed class CacheItem
        {
            public TKey Key { get; }
            public TValue Value { get; }
            public DateTimeOffset Created { get; }
            public TimeSpan? TtlOverride { get; }
            public long LastAccessedTicks;

            public CacheItem(TKey key, TValue value, DateTimeOffset created, TimeSpan? ttlOverride, long lastAccessedTicks)
            {
                Key = key;
                Value = value;
                Created = created;
                TtlOverride = ttlOverride;
                LastAccessedTicks = lastAccessedTicks;
            }
        }

        /// <summary>
        /// Disposes the cache and releases the reader-writer lock.
        /// </summary>
        public void Dispose()
        {
            _lock?.Dispose();
        }
    }
}
