using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;

namespace nORM.SourceGeneration
{
    /// <summary>
    /// Provides a thread-safe cache for compiled data reader materializers used during
    /// query execution. Materializers convert rows from a <see cref="DbDataReader"/> into
    /// strongly typed objects.
    /// </summary>
    public static class CompiledMaterializerStore
    {
        private static readonly ConcurrentLruCache<Type, (Delegate Typed, Func<DbDataReader, CancellationToken, Task<object>> Untyped)> _map = new(maxSize: 500);

        /// <summary>
        /// Registers a materializer delegate for the specified entity <paramref name="type"/>.
        /// </summary>
        /// <param name="type">Entity type the materializer produces.</param>
        /// <param name="materializer">Function that converts a <see cref="DbDataReader"/> row into an entity instance.</param>
        public static void Add(Type type, Func<DbDataReader, object> materializer)
            => _map.GetOrAdd(type, _ => (materializer, (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(materializer(reader));
            }));

        /// <summary>
        /// Registers a materializer delegate for the generic entity type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Entity type the materializer produces.</typeparam>
        /// <param name="materializer">Function that converts a <see cref="DbDataReader"/> row into an entity instance.</param>
        public static void Add<T>(Func<DbDataReader, T> materializer)
            => _map.GetOrAdd(typeof(T), _ => (materializer, (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult((object)materializer(reader)!);
            }));

        /// <summary>
        /// Attempts to retrieve a previously registered untyped materializer for the given entity type.
        /// </summary>
        /// <param name="type">Entity type to look up.</param>
        /// <param name="materializer">When this method returns, contains the materializer if found.</param>
        /// <returns><c>true</c> if a materializer is cached; otherwise, <c>false</c>.</returns>
        public static bool TryGet(Type type, out Func<DbDataReader, CancellationToken, Task<object>> materializer)
        {
            if (_map.TryGet(type, out var entry))
            {
                materializer = entry.Untyped;
                return true;
            }
            materializer = null!;
            return false;
        }

        /// <summary>
        /// Retrieves a strongly typed materializer for <typeparamref name="T"/>. Throws if none has been registered.
        /// </summary>
        /// <typeparam name="T">Entity type to retrieve.</typeparam>
        /// <returns>A delegate that asynchronously materializes an entity of type <typeparamref name="T"/>.</returns>
        /// <exception cref="KeyNotFoundException">Thrown if no materializer is registered for <typeparamref name="T"/>.</exception>
        public static Func<DbDataReader, CancellationToken, Task<T>> Get<T>()
        {
            if (!_map.TryGet(typeof(T), out var entry))
                throw new KeyNotFoundException($"Materializer for {typeof(T)} not found.");
            return (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(((Func<DbDataReader, T>)entry.Typed)(reader));
            };
        }
    }
}
