using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace nORM.SourceGeneration
{
    /// <summary>
    /// Registry of source-generated property accessors (getters and setters) for
    /// <c>[GenerateMaterializer]</c> entities. Populated at module-initialization time by the
    /// source generator with direct-access lambdas — <c>o =&gt; ((TEntity)o).Prop</c> and
    /// <c>(o, v) =&gt; ((TEntity)o).Prop = (TProp)v</c> — that statically reference each property.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The write path reads and writes entity values through <see cref="System.Reflection"/>-built
    /// delegates (see <c>ColumnMappingCache.CreateOptimizedGetter/CreateOptimizedSetter</c>). Under a
    /// trimmed or NativeAOT publish, nothing else statically references those property accessors, so the
    /// trimmer removes them and the reflection delegates silently return defaults — corrupting writes.
    /// The generated lambdas registered here keep a static reference to every property accessor, so the
    /// trimmer preserves them and the write path is correct with no consumer <c>&lt;TrimmerRootAssembly&gt;</c>
    /// ceremony.
    /// </para>
    /// <para>
    /// This type is public because the source generator emits <c>GeneratedAccessors.Register(...)</c> into
    /// consumer assemblies, exactly like <see cref="CompiledMaterializerStore"/>. It performs no reflection
    /// and no dynamic code; every delegate it holds was produced by the compiler.
    /// </para>
    /// </remarks>
    public static class GeneratedAccessors
    {
        private sealed class AccessorSet
        {
            internal required IReadOnlyDictionary<string, Func<object, object?>> Getters { get; init; }
            internal required IReadOnlyDictionary<string, Action<object, object?>> Setters { get; init; }
        }

        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, AccessorSet> _map = new();

        /// <summary>
        /// Registers source-generated getters and setters for <paramref name="entityType"/>. Called from the
        /// generated <c>[ModuleInitializer]</c>. Idempotent: the first registration for a type wins, so a
        /// duplicate module initializer (e.g. the same entity referenced by multiple assemblies) is a no-op.
        /// </summary>
        /// <param name="entityType">The entity CLR type the accessors belong to.</param>
        /// <param name="getters">Property-name &#8594; boxed-value getter map.</param>
        /// <param name="setters">Property-name &#8594; boxed-value setter map.</param>
        /// <exception cref="ArgumentNullException">Thrown when any argument is <c>null</c>.</exception>
        public static void Register(
            Type entityType,
            IReadOnlyDictionary<string, Func<object, object?>> getters,
            IReadOnlyDictionary<string, Action<object, object?>> setters)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(getters);
            ArgumentNullException.ThrowIfNull(setters);
            _map.TryAdd(entityType, new AccessorSet { Getters = getters, Setters = setters });
        }

        /// <summary>
        /// Attempts to retrieve a source-generated getter for <paramref name="propertyName"/> on
        /// <paramref name="entityType"/>. Returns <c>false</c> when the type has no generated accessors
        /// (not a <c>[GenerateMaterializer]</c> entity) or the property was not source-generated (e.g. an
        /// owned/nested property), so the caller falls back to the reflection getter.
        /// </summary>
        public static bool TryGetGetter(Type entityType, string propertyName, [NotNullWhen(true)] out Func<object, object?>? getter)
        {
            if (entityType != null && _map.TryGetValue(entityType, out var set) &&
                set.Getters.TryGetValue(propertyName, out var g))
            {
                getter = g;
                return true;
            }
            getter = null;
            return false;
        }

        /// <summary>
        /// Attempts to retrieve a source-generated setter for <paramref name="propertyName"/> on
        /// <paramref name="entityType"/>. Returns <c>false</c> when unavailable, so the caller falls back
        /// to the reflection setter.
        /// </summary>
        public static bool TryGetSetter(Type entityType, string propertyName, [NotNullWhen(true)] out Action<object, object?>? setter)
        {
            if (entityType != null && _map.TryGetValue(entityType, out var set) &&
                set.Setters.TryGetValue(propertyName, out var s))
            {
                setter = s;
                return true;
            }
            setter = null;
            return false;
        }

        /// <summary>Returns <c>true</c> if any source-generated accessors are registered for <paramref name="entityType"/>.</summary>
        public static bool HasAccessors(Type entityType)
            => entityType != null && _map.ContainsKey(entityType);
    }
}
