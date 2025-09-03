using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

#nullable enable

namespace nORM.Internal
{
    internal static class ShadowPropertyStore
    {
        private static readonly ConditionalWeakTable<object, Dictionary<string, object?>> _values = new();

        public static void Set(object entity, string name, object? value)
        {
            var dict = _values.GetOrCreateValue(entity);
            dict[name] = value;
        }

        public static object? Get(object entity, string name)
        {
            return _values.TryGetValue(entity, out var dict) && dict.TryGetValue(name, out var val) ? val : null;
        }
    }
}
