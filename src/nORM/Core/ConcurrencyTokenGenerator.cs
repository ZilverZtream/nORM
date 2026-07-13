using System;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Produces client-managed concurrency-token values. Lives outside <see cref="DbContext"/>
    /// (which carries class-level trim/AOT taint) because the generation is pure value logic:
    /// bulk provider paths call it from unannotated contexts and must not trip the AOT gate.
    /// </summary>
    internal static class ConcurrencyTokenGenerator
    {
        /// <summary>
        /// Produces a fresh concurrency-token value that differs from <paramref name="oldValue"/>, for
        /// providers without a native rowversion. byte[] / Guid use a new GUID (globally unique so a
        /// stale writer's token can never collide); integer tokens increment. Other types are rejected
        /// with a clear message rather than silently failing to protect against lost updates.
        /// </summary>
        internal static object Next(Column tokenColumn, object? oldValue)
        {
            var t = Nullable.GetUnderlyingType(tokenColumn.Prop.PropertyType) ?? tokenColumn.Prop.PropertyType;
            if (t == typeof(byte[]))
            {
                // 8 bytes matches the conventional rowversion width (SQL Server ROWVERSION / VARBINARY(8));
                // 64 random bits make a stale writer's token collision negligible.
                var g = Guid.NewGuid().ToByteArray();
                return new[] { g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7] };
            }
            if (t == typeof(Guid)) return Guid.NewGuid();
            if (t == typeof(long)) return (oldValue is long l ? l : 0L) + 1L;
            if (t == typeof(ulong)) return (oldValue is ulong ul ? ul : 0UL) + 1UL;
            if (t == typeof(int)) return (oldValue is int i ? i : 0) + 1;
            if (t == typeof(uint)) return (oldValue is uint u ? u : 0U) + 1U;
            throw new NormConfigurationException(
                $"Concurrency token column '{tokenColumn.PropName}' has type '{t.Name}', which nORM cannot " +
                "auto-manage on a provider without a native rowversion. Use byte[], Guid, or an integer token, " +
                "or run on SQL Server (ROWVERSION).");
        }
    }
}
