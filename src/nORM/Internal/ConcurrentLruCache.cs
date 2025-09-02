using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

#nullable enable

namespace nORM.Internal
{
    public class ConcurrentLruCache<TKey, TValue> where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheItem>> _cache = new();
        private readonly LinkedList<CacheItem> _lruList = new();
        private readonly object _lock = new();
        private readonly int _maxSize;

        public ConcurrentLruCache(int maxSize)
        {
            _maxSize = maxSize;
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> factory)
        {
            if (_cache.TryGetValue(key, out var node))
            {
                lock (_lock)
                {
                    _lruList.Remove(node);
                    _lruList.AddFirst(node);
                }
                return node.Value.Value;
            }

            var value = factory(key);
            var newNode = new LinkedListNode<CacheItem>(new CacheItem(key, value));

            lock (_lock)
            {
                if (_cache.TryAdd(key, newNode))
                {
                    _lruList.AddFirst(newNode);

                    if (_lruList.Count > _maxSize)
                    {
                        var lastNode = _lruList.Last!;
                        _lruList.RemoveLast();
                        _cache.TryRemove(lastNode.Value.Key, out _);
                    }
                }
            }

            return value;
        }

        private record CacheItem(TKey Key, TValue Value);
    }
}

