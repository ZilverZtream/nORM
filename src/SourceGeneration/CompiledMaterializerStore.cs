using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.SourceGeneration
{
    public static class CompiledMaterializerStore
    {
        private static readonly ConcurrentDictionary<Type, (Delegate Typed, Func<DbDataReader, CancellationToken, Task<object>> Untyped)> _map = new();

        public static void Add(Type type, Func<DbDataReader, object> materializer)
            => _map[type] = (materializer, (reader, ct) => Task.FromResult(materializer(reader)));

        public static void Add<T>(Func<DbDataReader, T> materializer)
            => _map[typeof(T)] = (materializer, (reader, ct) => Task.FromResult((object)materializer(reader)!));

        public static bool TryGet(Type type, out Func<DbDataReader, CancellationToken, Task<object>> materializer)
        {
            if (_map.TryGetValue(type, out var entry))
            {
                materializer = entry.Untyped;
                return true;
            }
            materializer = null!;
            return false;
        }

        public static Func<DbDataReader, CancellationToken, Task<T>> Get<T>()
            => (reader, ct) => Task.FromResult(((Func<DbDataReader, T>)_map[typeof(T)].Typed)(reader));
    }
}
