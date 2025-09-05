using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

#nullable enable

namespace nORM.Internal
{
    public class ConcurrentLruCache<TKey, TValue> where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheItem>> _cache = new();
        private readonly LinkedList<CacheItem> _lruList = new();
        // Protects access to _lruList, which is not thread-safe.
        private readonly object _lock = new();
        private readonly int _maxSize;
        private readonly TimeSpan? _timeToLive;
        private long _hits;
        private long _misses;

        public ConcurrentLruCache(int maxSize, TimeSpan? timeToLive = null)
        {
            _maxSize = maxSize;
            _timeToLive = timeToLive;
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> factory)
        {
            // First, try a lock-free read for the common case.
            if (_cache.TryGetValue(key, out var existingNode))
            {
                lock (_lock)
                {
                    // The node might have been removed by another thread between TryGetValue and the lock.
                    // A simple check is to see if it still has a list.
                    if (existingNode.List != null)
                    {
                        if (!IsExpired(existingNode.Value))
                        {
                            _lruList.Remove(existingNode);
                            _lruList.AddFirst(existingNode);
                            Interlocked.Increment(ref _hits);
                            return existingNode.Value.Value;
                        }

                        _lruList.Remove(existingNode);
                        _cache.TryRemove(existingNode.Value.Key, out _);
                    }
                }
            }

            // Key doesn't exist or was just evicted, we need to add it.
            lock (_lock)
            {
                // Double-check if another thread added it while we were waiting for the lock.
                if (_cache.TryGetValue(key, out existingNode))
                {
                    if (!IsExpired(existingNode.Value))
                    {
                        _lruList.Remove(existingNode);
                        _lruList.AddFirst(existingNode);
                        Interlocked.Increment(ref _hits);
                        return existingNode.Value.Value;
                    }

                    _lruList.Remove(existingNode);
                    _cache.TryRemove(existingNode.Value.Key, out _);
                }

                // We are the first, create and add the new item.
                var value = factory(key);
                var newNode = new LinkedListNode<CacheItem>(new CacheItem(key, value, DateTimeOffset.UtcNow));

                _cache[key] = newNode; // Use indexer now that we are in a lock.
                _lruList.AddFirst(newNode);

                if (_lruList.Count > _maxSize)
                {
                    var lastNode = _lruList.Last!;
                    _lruList.RemoveLast();
                    _cache.TryRemove(lastNode.Value.Key, out _);
                }

                Interlocked.Increment(ref _misses);
                return value;
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            if (_cache.TryGetValue(key, out var existingNode))
            {
                lock (_lock)
                {
                    if (existingNode.List != null)
                    {
                        if (!IsExpired(existingNode.Value))
                        {
                            _lruList.Remove(existingNode);
                            _lruList.AddFirst(existingNode);
                            value = existingNode.Value.Value;
                            Interlocked.Increment(ref _hits);
                            return true;
                        }

                        _lruList.Remove(existingNode);
                        _cache.TryRemove(existingNode.Value.Key, out _);
                    }
                }
            }

            Interlocked.Increment(ref _misses);
            value = default!;
            return false;
        }

        public void Clear()
        {
            lock (_lock)
            {
                _cache.Clear();
                _lruList.Clear();
            }
        }

        public long Hits => Interlocked.Read(ref _hits);
        public long Misses => Interlocked.Read(ref _misses);
        public double HitRate => Hits + Misses == 0 ? 0 : (double)Hits / (Hits + Misses);

        private bool IsExpired(CacheItem item)
            => _timeToLive.HasValue && DateTimeOffset.UtcNow - item.Created > _timeToLive.Value;

        private record CacheItem(TKey Key, TValue Value, DateTimeOffset Created);
    }
}

