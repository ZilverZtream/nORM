using System;
using System.Collections.Concurrent;
using System.Data.Common;

namespace nORM.SourceGeneration
{
    public static class CompiledMaterializerStore
    {
        private static readonly ConcurrentDictionary<Type, (Delegate Typed, Func<DbDataReader, object> Untyped)> _map = new();

        public static void Add(Type type, Func<DbDataReader, object> materializer)
            => _map[type] = (materializer, materializer);

        public static void Add<T>(Func<DbDataReader, T> materializer)
            => _map[typeof(T)] = (materializer, reader => materializer(reader)!);

        public static bool TryGet(Type type, out Func<DbDataReader, object> materializer)
        {
            if (_map.TryGetValue(type, out var entry))
            {
                materializer = entry.Untyped;
                return true;
            }
            materializer = null!;
            return false;
        }

        public static Func<DbDataReader, T> Get<T>()
            => (Func<DbDataReader, T>)_map[typeof(T)].Typed;
    }
}
