using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

#nullable enable

namespace nORM.Internal
{
    internal static class ShadowPropertyStore
    {
        private static readonly ConditionalWeakTable<object, ConcurrentDictionary<string, object?>> _values = new();

        /// <summary>
        /// Associates a value with a shadow property on the given entity instance.
        /// </summary>
        /// <param name="entity">The entity instance that carries the shadow property.</param>
        /// <param name="name">The shadow property name.</param>
        /// <param name="value">The value to store.</param>
        public static void Set(object entity, string name, object? value)
        {
            var dict = _values.GetOrCreateValue(entity);
            dict[name] = value;
        }

        /// <summary>
        /// Retrieves the value previously associated with the specified shadow property, if any.
        /// </summary>
        /// <param name="entity">The entity instance.</param>
        /// <param name="name">The shadow property name.</param>
        /// <returns>The stored value, or <c>null</c> if no value has been set.</returns>
        public static object? Get(object entity, string name)
        {
            return _values.TryGetValue(entity, out var dict) && dict.TryGetValue(name, out var val) ? val : null;
        }
    }
}
