using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.ObjectPool;
using nORM.Internal;

#nullable enable

namespace nORM.Core
{
    public static partial class NormValidator
    {
        private static readonly ObjectPool<HashSet<object>> HashSetPool =
            new DefaultObjectPool<HashSet<object>>(new HashSetPolicy(), Environment.ProcessorCount * 2);

        /// <summary>
        /// Validates a single entity instance to ensure it does not contain excessively deep
        /// or cyclic graphs that could lead to stack overflows or performance issues.
        /// </summary>
        /// <typeparam name="T">Type of the entity being validated.</typeparam>
        /// <param name="entity">Entity instance to validate.</param>
        /// <param name="parameterName">Name of the parameter for exception messages.</param>
        public static void ValidateEntity<T>(T entity, string parameterName = "entity") where T : class
        {
            if (entity == null)
                throw new ArgumentNullException(parameterName);

            var visited = RentHashSet();
            try
            {
                ValidateEntityGraph(entity!, visited, parameterName);
            }
            finally
            {
                ReturnHashSet(visited);
            }
        }

        /// <summary>
        /// Retrieves a <see cref="HashSet{Object}"/> instance from the object pool
        /// used to track visited entities during validation. The set is configured for
        /// reference equality to correctly handle duplicate object references.
        /// </summary>
        /// <returns>A rented hash set instance.</returns>
        private static HashSet<object> RentHashSet() => HashSetPool.Get();

        /// <summary>
        /// Returns a previously rented hash set to the pool for reuse.
        /// </summary>
        /// <param name="set">The hash set to return.</param>
        private static void ReturnHashSet(HashSet<object> set) => HashSetPool.Return(set);

        private sealed class HashSetPolicy : PooledObjectPolicy<HashSet<object>>
        {
            /// <summary>
            /// Creates a new <see cref="HashSet{T}"/> configured with reference equality for tracking visited entities.
            /// </summary>
            /// <returns>A fresh hash set instance.</returns>
            public override HashSet<object> Create()
                => new HashSet<object>(ReferenceEqualityComparer.Instance);

            /// <summary>
            /// Resets the given hash set so it can be reused by the pool.
            /// </summary>
            /// <param name="obj">The hash set to reset.</param>
            /// <returns>Always <c>true</c> to indicate the object may be reused.</returns>
            public override bool Return(HashSet<object> obj)
            {
                obj.Clear();
                return true;
            }
        }

        /// <summary>
        /// Cache of properties whose declared type could contain entity graph references.
        /// Excludes value types and strings, avoiding ~90% of GetValue reflection calls
        /// for typical flat entities (e.g., BenchmarkUser with int/string/DateTime/bool/double).
        /// </summary>
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> NavigablePropertyCache = new();
        private static PropertyInfo[] GetNavigableProperties(Type type)
            => NavigablePropertyCache.GetOrAdd(type, t =>
                t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && p.PropertyType.IsClass && p.PropertyType != typeof(string) && p.PropertyType != typeof(byte[]))
                    .ToArray());

        private static void ValidateEntityGraph(object rootEntity, HashSet<object> visited, string rootPath)
        {
            var stack = new Stack<(object Entity, int Depth, string Path)>();
            stack.Push((rootEntity, 0, rootPath));

            while (stack.Count > 0)
            {
                var (entity, depth, path) = stack.Pop();

                if (depth > MaxEntityDepth)
                    throw new ArgumentException($"Entity graph exceeds maximum depth of {MaxEntityDepth} at {path}");

                // Check if the popped entity itself is IEnumerable to validate nested collections.
                if (entity is IEnumerable enumerable && entity is not string)
                {
                    ValidateCollection(enumerable, path);

                    foreach (var item in enumerable)
                    {
                        if (item == null) continue;

                        var itemType = item.GetType();
                        if (itemType.IsClass && itemType != typeof(string))
                        {
                            // Increment depth for collection items to count them as a deeper level in the graph
                            stack.Push((item, depth + 1, $"{path}[{itemType.Name}]"));
                        }
                    }
                    continue;
                }

                // Allow circular references without throwing errors by stopping
                // validation when an entity has already been visited. This prevents
                // infinite loops in graphs with cycles while still validating the
                // remainder of the object graph.
                if (!visited.Add(entity))
                    continue;

                // Only inspect properties whose declared type is a reference type
                // (excluding string/byte[]). For flat entities with only value-type + string
                // properties, this array is empty — skipping all GetValue reflection calls.
                var properties = GetNavigableProperties(entity.GetType());

                foreach (var prop in properties)
                {
                    var value = prop.GetValue(entity);
                    if (value == null) continue;

                    var propPath = $"{path}.{prop.Name}";

                    // Push all non-null class values to stack for validation
                    // IEnumerable check will happen when item is popped
                    stack.Push((value, depth + 1, propPath));
                }
            }
        }

        private static void ValidateCollection(IEnumerable collection, string path)
        {
            // Use ICollection.Count when available for an O(1) check instead of enumerating.
            if (collection is ICollection coll)
            {
                if (coll.Count > MaxCollectionSize)
                    throw new ArgumentException($"Collection at {path} exceeds maximum size of {MaxCollectionSize}");
                return;
            }

            // Fallback to iteration for non-ICollection types
            var count = 0;
            foreach (var _ in collection)
            {
                if (++count > MaxCollectionSize)
                    throw new ArgumentException($"Collection at {path} exceeds maximum size of {MaxCollectionSize}");
            }
        }
    }
}
