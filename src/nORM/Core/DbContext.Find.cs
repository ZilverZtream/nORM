using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Finds an entity with the given primary key values, matching Entity Framework Core's
        /// <c>FindAsync</c>. If an entity with those key values is already tracked, it is returned
        /// immediately with no database round trip; otherwise the entity is queried by key and
        /// tracked (unless the query path is no-tracking). Returns <c>null</c> when no such row
        /// exists. Key values are supplied in mapped key order and coerced to the key CLR types
        /// (so <c>FindAsync&lt;T&gt;(5)</c> works when the key is a <see cref="long"/>).
        /// </summary>
        /// <typeparam name="T">The mapped entity type.</typeparam>
        /// <param name="keyValues">The primary-key values in mapped key order.</param>
        /// <returns>The tracked or freshly loaded entity, or <c>null</c> if none exists.</returns>
        [RequiresDynamicCode("nORM Find builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("nORM Find reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public ValueTask<T?> FindAsync<T>(params object?[] keyValues) where T : class
            => FindAsync<T>(keyValues, CancellationToken.None);

        /// <summary>
        /// Cancellation-aware overload of <see cref="FindAsync{T}(object?[])"/>, mirroring the
        /// Entity Framework Core <c>FindAsync&lt;T&gt;(object?[], CancellationToken)</c> signature.
        /// </summary>
        [RequiresDynamicCode("nORM Find builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("nORM Find reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public async ValueTask<T?> FindAsync<T>(object?[] keyValues, CancellationToken ct) where T : class
        {
            ArgumentNullException.ThrowIfNull(keyValues);
            var mapping = GetMapping(typeof(T));
            if (mapping.KeyColumns.Length == 0)
                throw new NormUsageException(
                    $"FindAsync requires a primary key, but entity type '{typeof(T).Name}' has none. " +
                    "Configure a key with [Key] or a fluent HasKey(...).");
            if (keyValues.Length != mapping.KeyColumns.Length)
                throw new NormUsageException(
                    $"FindAsync for '{typeof(T).Name}' expects {mapping.KeyColumns.Length} key value(s) " +
                    $"in mapped key order but received {keyValues.Length}.");

            // Coerce each supplied value to its key column's CLR type so FindAsync<T>(5) matches a
            // long key, a string matches a Guid key, and so on — the same coercion the temporal
            // history key path uses.
            var coerced = new object?[keyValues.Length];
            for (var i = 0; i < keyValues.Length; i++)
            {
                if (keyValues[i] is null)
                    throw new NormUsageException(
                        $"FindAsync for '{typeof(T).Name}' received a null value for key component " +
                        $"'{mapping.KeyColumns[i].Prop.Name}'. Primary-key values cannot be null.");
                coerced[i] = ConvertTemporalKeyValue(keyValues[i], mapping.KeyColumns[i].Prop.PropertyType);
            }

            // Identity-map first: a tracked entity with this key is returned without a round trip.
            // This is the whole point of Find over FirstOrDefault — and the ValueTask makes the hit
            // path allocation-free.
            var lookupKey = ChangeTracker.BuildLookupKey(mapping, coerced);
            if (lookupKey is not null && ChangeTracker.GetEntryByKey(typeof(T), lookupKey)?.Entity is T tracked)
                return tracked;

            // Not tracked: query by key. The predicate captures the key values through a holder so
            // the translator parameterizes them (@p) — Find(1) and Find(2) then share one cached
            // plan instead of baking the id as a literal and thrashing the plan cache.
            var predicate = BuildFindPredicate<T>(mapping, coerced);
            return await this.Query<T>().Where(predicate).FirstOrDefaultAsync(ct).ConfigureAwait(false);
        }

        [RequiresDynamicCode("nORM Find builds a key predicate expression tree; not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("nORM Find reflects over the mapped key metadata; trimming may remove the required members.")]
        private static Expression<Func<T, bool>> BuildFindPredicate<T>(TableMapping mapping, object?[] coerced)
        {
            var parameter = Expression.Parameter(typeof(T), "e");
            Expression? body = null;
            for (var i = 0; i < mapping.KeyColumns.Length; i++)
            {
                var keyCol = mapping.KeyColumns[i];
                var keyType = keyCol.Prop.PropertyType;

                // Read the captured value through a holder member access — the closure shape nORM
                // lifts into a compiled parameter — rather than Expression.Constant, which would
                // bake the key as a literal and give every distinct id its own plan-cache entry.
                var holderType = typeof(FindKeyHolder<>).MakeGenericType(keyType);
                var holder = Activator.CreateInstance(holderType)!;
                holderType.GetField(nameof(FindKeyHolder<object>.Value))!.SetValue(holder, coerced[i]);
                var valueExpr = Expression.Field(
                    Expression.Constant(holder, holderType),
                    nameof(FindKeyHolder<object>.Value));

                var comparison = Expression.Equal(Expression.Property(parameter, keyCol.Prop), valueExpr);
                body = body is null ? comparison : Expression.AndAlso(body, comparison);
            }

            return Expression.Lambda<Func<T, bool>>(body!, parameter);
        }

        /// <summary>Holds a captured key value so the Find predicate reads it as a closure
        /// (parameterized), not a baked constant.</summary>
        private sealed class FindKeyHolder<TValue>
        {
            public TValue Value = default!;
        }
    }
}
