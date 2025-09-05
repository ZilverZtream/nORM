using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;

namespace nORM.SourceGeneration
{
    public static class CompiledMaterializerStore
    {
        private static readonly ConcurrentLruCache<Type, (Delegate Typed, Func<DbDataReader, CancellationToken, Task<object>> Untyped)> _map = new(maxSize: 500);

        public static void Add(Type type, Func<DbDataReader, object> materializer)
            => _map.GetOrAdd(type, _ => (materializer, (reader, ct) => Task.FromResult(materializer(reader))));

        public static void Add<T>(Func<DbDataReader, T> materializer)
            => _map.GetOrAdd(typeof(T), _ => (materializer, (reader, ct) => Task.FromResult((object)materializer(reader)!)));

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

        public static Func<DbDataReader, CancellationToken, Task<T>> Get<T>()
        {
            if (!_map.TryGet(typeof(T), out var entry))
                throw new KeyNotFoundException($"Materializer for {typeof(T)} not found.");
            return (reader, ct) => Task.FromResult(((Func<DbDataReader, T>)entry.Typed)(reader));
        }
    }
}
