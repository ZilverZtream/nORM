using System;
using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        private static readonly ConcurrentDictionary<(Type From, Type To), Func<object, object>> _conversionCache = new();

        private static bool IsSimpleType(Type type)
            // Include nullable primitives so projected subqueries (e.g. Select(x => x.NullableInt)) materialize correctly.
            => _simpleTypeCache.GetOrAdd(type, static t => t.IsPrimitive || t == typeof(decimal) || t == typeof(string)
                || (Nullable.GetUnderlyingType(t) is Type u && (u.IsPrimitive || u == typeof(decimal))));

        internal static (long Hits, long Misses, double HitRate) CacheStats
        {
            get
            {
                var syncHits = _syncCache.Hits;
                var syncMisses = _syncCache.Misses;
                var asyncHits = _asyncCache.Hits;
                var asyncMisses = _asyncCache.Misses;

                var totalHits = syncHits + asyncHits;
                var totalMisses = syncMisses + asyncMisses;
                var hitRate = totalHits + totalMisses > 0
                    ? (double)totalHits / (totalHits + totalMisses)
                    : 0.0;

                return (totalHits, totalMisses, hitRate);
            }
        }

        internal static (long SchemaHits, long SchemaMisses, double SchemaHitRate) SchemaCacheStats
            => (_schemaCache.Hits, _schemaCache.Misses, _schemaCache.HitRate);

        public static void PrecompileCommonPatterns<T>() where T : class
        {
            var key = typeof(T);
            if (!_fastMaterializers.ContainsKey(key))
            {
                _fastMaterializers[key] = CreateILMaterializer<T>();
            }
        }

        /// <summary>
        /// MM-2: Converts a boxed DB value (typically <see cref="long"/> from SQLite) to the
        /// specified enum type <typeparamref name="TEnum"/>. Calling <c>Convert.ChangeType</c>
        /// directly on an enum target type throws <see cref="InvalidCastException"/>; this helper
        /// converts to the enum's underlying integral type first, then calls
        /// <c>Enum.ToObject</c>.
        /// </summary>
        private static TEnum ConvertToEnum<TEnum>(object value) where TEnum : struct, Enum
        {
            var underlying = Enum.GetUnderlyingType(typeof(TEnum));
            var converted = Convert.ChangeType(value, underlying);
            return (TEnum)Enum.ToObject(typeof(TEnum), converted);
        }

        /// <summary>
        /// Converts a boxed DB value to <see cref="DateOnly"/>. Called from IL-emitted
        /// materializers where <c>Convert.ChangeType</c> cannot handle DateOnly.
        /// Public so source-generated materializers can call it (SG1 fix).
        /// </summary>
        public static DateOnly ConvertToDateOnly(object value)
        {
            if (value is DateTime dt) return DateOnly.FromDateTime(dt);
            if (value is string s)
            {
                // DateOnly.Parse handles "yyyy-MM-dd"; fall back to DateTime.Parse for
                // "yyyy-MM-dd HH:mm:ss" strings that some providers emit for DbType.Date.
                if (DateOnly.TryParse(s, CultureInfo.InvariantCulture, out var d)) return d;
                return DateOnly.FromDateTime(DateTime.Parse(s, CultureInfo.InvariantCulture));
            }
            return DateOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Converts a boxed DB value to <see cref="TimeOnly"/>. Called from IL-emitted
        /// materializers where <c>Convert.ChangeType</c> cannot handle TimeOnly.
        /// Public so source-generated materializers can call it (SG1 fix).
        /// </summary>
        public static TimeOnly ConvertToTimeOnly(object value)
        {
            if (value is TimeSpan ts) return TimeOnly.FromTimeSpan(ts);
            if (value is DateTime dt) return TimeOnly.FromDateTime(dt);
            if (value is string s) return TimeOnly.Parse(s, CultureInfo.InvariantCulture);
            return TimeOnly.FromTimeSpan((TimeSpan)Convert.ChangeType(value, typeof(TimeSpan), CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Computes a 64-bit projection hash by combining two independent 32-bit hashes
        /// from the <see cref="ExpressionFingerprint"/>. This reduces collision probability
        /// compared to a single 32-bit hash (roughly 1-in-2^64 vs 1-in-2^32 per pair).
        /// </summary>
        private static long ComputeProjectionHash(LambdaExpression projection)
        {
            var fp = ExpressionFingerprint.Compute(projection);
            // Use the fingerprint's own hash plus an extended hash with a known constant
            // to produce two independent 32-bit values combined into one 64-bit value.
            var h1 = (long)(uint)fp.GetHashCode();
            var h2 = (long)(uint)fp.Extend(unchecked((int)0xDEADBEEF)).GetHashCode();
            return (h2 << 32) | h1;
        }

        private static object? ConvertDbValue(object dbValue, Type targetType)
        {
            if (dbValue == null || dbValue is DBNull)
            {
                // NULL CHECK FIX: Handle Nullable<T> correctly without expensive Activator.CreateInstance
                // Nullable<T> is a value type, but should return null, not default(Nullable<T>)
                var underlyingNullable = Nullable.GetUnderlyingType(targetType);
                if (underlyingNullable != null)
                {
                    // This is Nullable<T>, return null directly (which is default(Nullable<T>))
                    return null;
                }

                // MAP-4: Throw for non-nullable value types — DB NULL into a non-nullable member
                // silently defaults to 0/false/etc., hiding data integrity issues.
                if (targetType.IsValueType)
                    throw new InvalidOperationException(
                        $"DB column returned NULL for non-nullable value type '{targetType.Name}'. " +
                        $"Mark the property as nullable (e.g. '{targetType.Name}?') or fix the data source.");

                // For reference types, null is acceptable
                return null;
            }

            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (dbValue.GetType() == underlyingType)
            {
                return dbValue;
            }

            // MM-2: Handle enum types before calling Convert.ChangeType, which throws for enums.
            if (underlyingType.IsEnum)
            {
                var enumUnderlying = Enum.GetUnderlyingType(underlyingType);
                var enumConverted = Convert.ChangeType(dbValue, enumUnderlying);
                return Enum.ToObject(underlyingType, enumConverted);
            }

            // MM1 fix: DateOnly/TimeOnly require explicit conversion because Convert.ChangeType doesn't handle them
            if (underlyingType == typeof(DateOnly))
            {
                if (dbValue is DateTime dt) return DateOnly.FromDateTime(dt);
                if (dbValue is string s)
                {
                    if (DateOnly.TryParse(s, CultureInfo.InvariantCulture, out var d)) return d;
                    return DateOnly.FromDateTime(DateTime.Parse(s, CultureInfo.InvariantCulture));
                }
                return DateOnly.FromDateTime(Convert.ToDateTime(dbValue, CultureInfo.InvariantCulture));
            }
            if (underlyingType == typeof(TimeOnly))
            {
                if (dbValue is TimeSpan ts) return TimeOnly.FromTimeSpan(ts);
                if (dbValue is DateTime dt) return TimeOnly.FromDateTime(dt);
                if (dbValue is string s) return TimeOnly.Parse(s, CultureInfo.InvariantCulture);
                return TimeOnly.FromTimeSpan((TimeSpan)Convert.ChangeType(dbValue, typeof(TimeSpan), CultureInfo.InvariantCulture));
            }

            // Use cached conversion delegate for better performance.
            // Cache key uses dbValue.GetType() (runtime type): correct because the same runtime type
            // always produces the same conversion path to the target type.
            var conversionKey = (dbValue.GetType(), underlyingType);
            var converter = _conversionCache.GetOrAdd(conversionKey, key =>
            {
                var (from, to) = key;
                try
                {
                    var param = Expression.Parameter(typeof(object), "value");
                    var convert = Expression.Convert(
                        Expression.Call(typeof(Convert), nameof(Convert.ChangeType),
                            null,
                            Expression.Convert(param, from),
                            Expression.Constant(to)),
                        typeof(object));
                    return Expression.Lambda<Func<object, object>>(convert, param).Compile();
                }
                catch (InvalidCastException)
                {
                    // Fallback to runtime conversion when expression tree compilation fails
                    return value => Convert.ChangeType(value, to);
                }
                catch (InvalidOperationException)
                {
                    // Fallback to runtime conversion when expression tree compilation fails
                    return value => Convert.ChangeType(value, to);
                }
            });

            try
            {
                return converter(dbValue);
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                throw new InvalidOperationException(
                    $"Failed to convert {dbValue.GetType().Name} value '{dbValue}' to {underlyingType.Name}: {ex.Message}", ex);
            }
        }

    }
}
