using System.Collections.Concurrent;
using System.Collections.Generic;

#nullable enable

namespace nORM.Internal
{
    /// <summary>
    /// A thread-safe bounded FIFO cache that evicts the oldest entries when capacity is exceeded.
    ///
    /// Design notes:
    /// - FIFO eviction: the entry that was inserted first is evicted first. Not LRU, but
    ///   dramatically simpler and still provides the key guarantee: memory is bounded.
    /// - Concurrency: uses <see cref="ConcurrentDictionary{TKey,TValue}"/> for O(1) read/write
    ///   and <see cref="ConcurrentQueue{T}"/> for FIFO insertion order. No global lock required.
    /// - False evictions: if a key is updated (not added), no new queue entry is created, so
    ///   eviction order does not shift. Keys that are already evicted from the dict but still in
    ///   the queue are silently skipped during eviction.
    /// - Intended for internal use in query-plan caches where the key space is logically bounded
    ///   by the number of distinct LINQ expression shapes in the application.
    /// </summary>
    internal sealed class BoundedCache<TKey, TValue> where TKey : notnull
    {
        private readonly int _maxSize;
        private readonly ConcurrentDictionary<TKey, TValue> _dict;
        private readonly ConcurrentQueue<TKey> _insertionOrder;

        internal BoundedCache(int maxSize, IEqualityComparer<TKey>? comparer = null)
        {
            _maxSize = maxSize;
            _dict = comparer != null
                ? new ConcurrentDictionary<TKey, TValue>(comparer)
                : new ConcurrentDictionary<TKey, TValue>();
            _insertionOrder = new ConcurrentQueue<TKey>();
        }

        /// <summary>Attempts to retrieve the value for the given key. O(1).</summary>
        internal bool TryGet(TKey key,
            [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out TValue value)
            => _dict.TryGetValue(key, out value!);

        /// <summary>
        /// Adds the entry if the key is not already present, then evicts the oldest entries
        /// until the dictionary is within <see cref="MaxSize"/>. O(1) amortized.
        /// </summary>
        internal void Set(TKey key, TValue value)
        {
            if (_dict.TryAdd(key, value))
            {
                _insertionOrder.Enqueue(key);
                // Evict oldest until within capacity.
                while (_dict.Count > _maxSize && _insertionOrder.TryDequeue(out var evictKey))
                    _dict.TryRemove(evictKey, out _);
            }
        }

        /// <summary>Approximate current number of entries. May be slightly stale in concurrent scenarios.</summary>
        internal int Count => _dict.Count;

        /// <summary>The maximum number of entries before eviction begins.</summary>
        internal int MaxSize => _maxSize;

        /// <summary>Removes all entries. Used in tests.</summary>
        internal void Clear()
        {
            _dict.Clear();
            while (_insertionOrder.TryDequeue(out _)) { }
        }
    }
}
