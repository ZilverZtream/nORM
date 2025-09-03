using System;
using System.Collections.Concurrent;
using System.Data.Common;

namespace nORM.SourceGeneration
{
    public static class CompiledMaterializerStore
    {
        private static readonly ConcurrentDictionary<Type, Func<DbDataReader, object>> _map = new();

        public static void Add(Type type, Func<DbDataReader, object> materializer)
            => _map[type] = materializer;

        public static bool TryGet(Type type, out Func<DbDataReader, object> materializer)
            => _map.TryGetValue(type, out materializer!);

        public static Func<DbDataReader, T> Get<T>() => (Func<DbDataReader, T>)(object)_map[typeof(T)];
    }
}
