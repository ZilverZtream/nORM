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
    public class ConcurrentLruCache<TKey, TValue> where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheItem>> _cache = new();
        private readonly LinkedList<CacheItem> _lruList = new();
        private readonly object _lock = new();

        private int _maxSize;
        private readonly TimeSpan? _timeToLive;
        private long _hits;
        private long _misses;

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
            lock (_lock)
            {
                _maxSize = maxSize;
                while (_lruList.Count > _maxSize)
                {
                    var last = _lruList.Last!;
                    _lruList.RemoveLast();
                    _cache.TryRemove(last.Value.Key, out _);
                }
            }
        }

        /// <summary>
        /// Clears all entries from the cache and resets hit/miss statistics.
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _cache.Clear();
                _lruList.Clear();
                Interlocked.Exchange(ref _hits, 0);
                Interlocked.Exchange(ref _misses, 0);
            }
        }

        /// <summary>
        /// Attempts to retrieve a value from the cache. If found and not expired the entry is
        /// promoted to the most recently used position.
        /// </summary>
        /// <param name="key">Key of the cached item.</param>
        /// <param name="value">When this method returns, contains the cached value if found.</param>
        /// <returns><c>true</c> if the value was found in the cache; otherwise <c>false</c>.</returns>
        public bool TryGet(TKey key, out TValue value)
        {
            // Lock-free lookup, then guarded promotion & TTL check
            if (_cache.TryGetValue(key, out var node))
            {
                lock (_lock)
                {
                    if (node.List != null)
                    {
                        if (!IsExpired(node.Value))
                        {
                            _lruList.Remove(node);
                            _lruList.AddFirst(node);
                            Interlocked.Increment(ref _hits);
                            value = node.Value.Value;
                            return true;
                        }

                        // Expired: remove
                        _lruList.Remove(node);
                        _cache.TryRemove(node.Value.Key, out _);
                    }
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
            var item = new CacheItem(key, value, DateTimeOffset.UtcNow, ttlOverride);

            lock (_lock)
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

                // Size-based eviction only (no TTL prune here to avoid extra churn)
                if (_lruList.Count > _maxSize)
                {
                    var last = _lruList.Last!;
                    _lruList.RemoveLast();
                    _cache.TryRemove(last.Value.Key, out _);
                }
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
        /// Represents a cache entry along with its creation time and optional
        /// time-to-live override.
        /// </summary>
        /// <param name="Key">The key associated with the cached value.</param>
        /// <param name="Value">The cached value.</param>
        /// <param name="Created">Timestamp indicating when the entry was created.</param>
        /// <param name="TtlOverride">Optional TTL overriding the cache's default.</param>
        private readonly record struct CacheItem(TKey Key, TValue Value, DateTimeOffset Created, TimeSpan? TtlOverride);
    }
}
