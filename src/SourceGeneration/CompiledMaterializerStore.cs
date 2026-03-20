using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;

namespace nORM.SourceGeneration
{
    /// <summary>
    /// Provides a thread-safe LRU cache for compiled data reader materializers used during
    /// query execution. Materializers convert rows from a <see cref="DbDataReader"/> into
    /// strongly typed objects.
    /// </summary>
    /// <remarks>
    /// The cache is keyed by <c>(Type, tableName)</c> so that the same CLR type registered
    /// under different model mappings (different table names) each gets its own materializer.
    /// Without the table-name discriminator the first registered materializer silently wins,
    /// producing wrong hydration for the second model.
    /// </remarks>
    public static class CompiledMaterializerStore
    {
        /// <summary>
        /// Maximum number of materializer entries retained in the LRU cache before eviction.
        /// </summary>
        private const int DefaultMaxCacheSize = 500;

        private static readonly ConcurrentLruCache<(Type, string), (Delegate Typed, Func<DbDataReader, CancellationToken, Task<object>> Untyped)> _map = new(maxSize: DefaultMaxCacheSize);

        /// <summary>Returns the table name used as cache-key discriminator for <paramref name="type"/>.</summary>
        private static string GetTableName(Type type)
            => type.GetCustomAttribute<TableAttribute>(inherit: false)?.Name ?? type.Name;

        /// <summary>
        /// Registers a materializer delegate for the specified entity <paramref name="type"/>.
        /// Uses the <c>[Table]</c> attribute name (or the CLR type name) as the model discriminator.
        /// </summary>
        /// <param name="type">Entity type the materializer produces.</param>
        /// <param name="materializer">Function that converts a <see cref="DbDataReader"/> row into an entity instance.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="type"/> or <paramref name="materializer"/> is <c>null</c>.</exception>
        public static void Add(Type type, Func<DbDataReader, object> materializer)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(materializer);
            _map.GetOrAdd((type, GetTableName(type)), _ => (materializer, (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(materializer(reader));
            }));
        }

        /// <summary>
        /// Registers a materializer delegate for the generic entity type <typeparamref name="T"/>.
        /// Uses the <c>[Table]</c> attribute name (or the CLR type name) as the model discriminator.
        /// </summary>
        /// <typeparam name="T">Entity type the materializer produces.</typeparam>
        /// <param name="materializer">Function that converts a <see cref="DbDataReader"/> row into an entity instance.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="materializer"/> is <c>null</c>.</exception>
        public static void Add<T>(Func<DbDataReader, T> materializer)
        {
            ArgumentNullException.ThrowIfNull(materializer);
            _map.GetOrAdd((typeof(T), GetTableName(typeof(T))), _ => (materializer, (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult((object)materializer(reader)!);
            }));
        }

        /// <summary>
        /// Registers a materializer delegate for the generic entity type <typeparamref name="T"/>
        /// under an explicit <paramref name="tableName"/> discriminator. Use this overload when the
        /// table name is known at compile time (e.g., source-generated queries) to support multi-model
        /// scenarios where the same CLR type maps to different tables.
        /// </summary>
        /// <typeparam name="T">Entity type the materializer produces.</typeparam>
        /// <param name="tableName">Table name used as the model discriminator in the cache key.</param>
        /// <param name="materializer">Function that converts a <see cref="DbDataReader"/> row into an entity instance.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="tableName"/> or <paramref name="materializer"/> is <c>null</c>.</exception>
        public static void Add<T>(string tableName, Func<DbDataReader, T> materializer)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(materializer);
            _map.GetOrAdd((typeof(T), tableName), _ => (materializer, (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult((object)materializer(reader)!);
            }));
        }

        /// <summary>
        /// Attempts to retrieve a previously registered untyped materializer for the given entity type,
        /// using the <c>[Table]</c> attribute name (or CLR type name) as the model discriminator.
        /// </summary>
        /// <param name="type">Entity type to look up.</param>
        /// <param name="materializer">When this method returns, contains the materializer if found.</param>
        /// <returns><c>true</c> if a materializer is cached; otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="type"/> is <c>null</c>.</exception>
        public static bool TryGet(Type type, out Func<DbDataReader, CancellationToken, Task<object>> materializer)
        {
            ArgumentNullException.ThrowIfNull(type);
            return TryGet(type, GetTableName(type), out materializer);
        }

        /// <summary>
        /// Attempts to retrieve a previously registered untyped materializer for the given entity type
        /// and explicit table name. Use this overload when the query mapping may differ from the
        /// <c>[Table]</c> attribute (e.g., multi-model scenarios with the same CLR type).
        /// </summary>
        /// <param name="type">Entity type to look up.</param>
        /// <param name="tableName">Table name used as the model discriminator.</param>
        /// <param name="materializer">When this method returns, contains the materializer if found.</param>
        /// <returns><c>true</c> if a materializer is cached for the given type/table combination; otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="type"/> or <paramref name="tableName"/> is <c>null</c>.</exception>
        public static bool TryGet(Type type, string tableName, out Func<DbDataReader, CancellationToken, Task<object>> materializer)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(tableName);
            if (_map.TryGet((type, tableName), out var entry))
            {
                materializer = entry.Untyped;
                return true;
            }
            materializer = null!;
            return false;
        }

        /// <summary>
        /// Retrieves a strongly typed materializer for <typeparamref name="T"/>. Throws if none has been registered.
        /// Uses the <c>[Table]</c> attribute name (or CLR type name) as the model discriminator.
        /// </summary>
        /// <typeparam name="T">Entity type to retrieve.</typeparam>
        /// <returns>A delegate that asynchronously materializes an entity of type <typeparamref name="T"/>.</returns>
        /// <exception cref="KeyNotFoundException">Thrown if no materializer is registered for <typeparamref name="T"/>.</exception>
        public static Func<DbDataReader, CancellationToken, Task<T>> Get<T>()
            => Get<T>(GetTableName(typeof(T)));

        /// <summary>
        /// Retrieves a strongly typed materializer for <typeparamref name="T"/> registered under the
        /// specified <paramref name="tableName"/>. Throws if none has been registered.
        /// Use this overload when the table name is known at compile time (e.g., source-generated queries).
        /// </summary>
        /// <typeparam name="T">Entity type to retrieve.</typeparam>
        /// <param name="tableName">Table name used as the model discriminator in the cache key.</param>
        /// <returns>A delegate that asynchronously materializes an entity of type <typeparamref name="T"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="tableName"/> is <c>null</c>.</exception>
        /// <exception cref="KeyNotFoundException">Thrown if no materializer is registered for the given type/table combination.</exception>
        /// <exception cref="InvalidCastException">Thrown if the registered materializer delegate type does not match <typeparamref name="T"/>.</exception>
        public static Func<DbDataReader, CancellationToken, Task<T>> Get<T>(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            if (!_map.TryGet((typeof(T), tableName), out var entry))
                throw new KeyNotFoundException($"Materializer for {typeof(T)} (table '{tableName}') not found.");
            var typedDelegate = entry.Typed as Func<DbDataReader, T>
                ?? throw new InvalidCastException($"Registered materializer for {typeof(T)} (table '{tableName}') has incompatible delegate type {entry.Typed.GetType()}.");
            return (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(typedDelegate(reader));
            };
        }
    }
}
