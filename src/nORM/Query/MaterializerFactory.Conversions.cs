using System;
using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        private static readonly ConcurrentDictionary<(Type From, Type To), Func<object, object>> _conversionCache = new();

        private static bool IsSimpleType(Type type)
            // Gates the single-value scalar-projection path (Select(x => x.Member)). Covers every value
            // the reader materializes as one column, including nullable variants — enum/Guid/date-time
            // family were previously excluded, so Select(p => p.EnumProp) fell through to the entity
            // constructor path and threw. string is a reference type so it is listed directly.
            => _simpleTypeCache.GetOrAdd(type, static t =>
            {
                if (t == typeof(string)) return true;
                var u = Nullable.GetUnderlyingType(t) ?? t;
                return u.IsPrimitive
                    || u.IsEnum
                    || u == typeof(decimal)
                    || u == typeof(Guid)
                    || u == typeof(DateTime)
                    || u == typeof(DateTimeOffset)
                    || u == typeof(DateOnly)
                    || u == typeof(TimeOnly)
                    || u == typeof(TimeSpan);
            });

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateOnly ConvertToDateOnly(object value)
        {
            if (value is DateOnly d0) return d0;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TimeOnly ConvertToTimeOnly(object value)
        {
            if (value is TimeOnly t0) return t0;
            if (value is TimeSpan ts) return TimeOnly.FromTimeSpan(ts);
            if (value is DateTime dt) return TimeOnly.FromDateTime(dt);
            if (value is string s) return TimeOnly.Parse(s, CultureInfo.InvariantCulture);
            return TimeOnly.FromTimeSpan((TimeSpan)Convert.ChangeType(value, typeof(TimeSpan), CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Converts a boxed DB value to <see cref="DateTimeOffset"/>. SQLite stores
        /// DateTimeOffset as TEXT and Expression.Convert(object, DateTimeOffset) doesn't
        /// know how to parse the string form.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTimeOffset ConvertToDateTimeOffset(object value)
        {
            if (value is DateTimeOffset dto) return dto;
            if (value is DateTime dt) return new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), TimeSpan.Zero);
            if (value is string s) return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture);
            return (DateTimeOffset)Convert.ChangeType(value, typeof(DateTimeOffset), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Converts a boxed DB value to <see cref="TimeSpan"/>. Providers vary: SQLite uses
        /// TEXT, some store ticks as INTEGER.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TimeSpan ConvertToTimeSpan(object value)
        {
            if (value is TimeSpan ts) return ts;
            if (value is string s) return TimeSpan.Parse(s, CultureInfo.InvariantCulture);
            if (value is long ticks) return new TimeSpan(ticks);
            if (value is int ticks32) return new TimeSpan(ticks32);
            // REAL (double / float) columns coming from a projection that emitted
            // julianday(end) - julianday(start)) * 86400 represent total seconds;
            // TimeSpan.FromSeconds reconstructs the duration with sub-second
            // precision (SQLite's julianday is a double, so the diff retains the
            // fractional component down to microseconds).
            if (value is double sec) return TimeSpan.FromSeconds(sec);
            if (value is float secF) return TimeSpan.FromSeconds(secF);
            // SqlServer/Postgres return DECIMAL for `int / 1000.0` style REAL-seconds
            // projections (numeric promotion rules differ from SQLite, which returns
            // double). Treat decimal as fractional seconds the same way.
            if (value is decimal secD) return TimeSpan.FromSeconds((double)secD);
            return (TimeSpan)Convert.ChangeType(value, typeof(TimeSpan), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Converts a boxed DB value to a <see cref="char"/>. SQLite stores char columns
        /// as single-character TEXT; Expression.Convert(string → char) throws at runtime,
        /// so the optimized reader call routes through this helper instead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static char ConvertToChar(object value)
        {
            if (value is char ch) return ch;
            if (value is string s)
            {
                if (s.Length == 0) return '\0';
                return s[0];
            }
            // Numeric storage (rare but possible): treat the value as a UTF-16 code unit.
            return Convert.ToChar(value, CultureInfo.InvariantCulture);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            // DateTimeOffset: providers commonly store as TEXT (SQLite) or DATETIMEOFFSET (SQL Server).
            // Convert.ChangeType doesn't know how to coerce string → DateTimeOffset, so handle here.
            if (underlyingType == typeof(DateTimeOffset))
            {
                if (dbValue is DateTimeOffset dto) return dto;
                if (dbValue is DateTime dt) return new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), TimeSpan.Zero);
                if (dbValue is string s) return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture);
            }
            // TimeSpan: stored as TEXT or numeric ticks; coerce both. REAL
            // values come from projection expressions that emit julianday
            // arithmetic (DateTime - DateTime), pre-multiplied to seconds.
            if (underlyingType == typeof(TimeSpan))
            {
                if (dbValue is TimeSpan ts2) return ts2;
                if (dbValue is string s2) return TimeSpan.Parse(s2, CultureInfo.InvariantCulture);
                if (dbValue is long ticks) return new TimeSpan(ticks);
                if (dbValue is int ticks32) return new TimeSpan(ticks32);
                if (dbValue is double sec) return TimeSpan.FromSeconds(sec);
                if (dbValue is float secF) return TimeSpan.FromSeconds(secF);
                if (dbValue is decimal secD) return TimeSpan.FromSeconds((double)secD);
            }
            // char: stored as single-character TEXT on TEXT-storage providers; numeric
            // storage isn't common but accept it via Convert.ToChar.
            if (underlyingType == typeof(char))
            {
                if (dbValue is char ch) return ch;
                if (dbValue is string s3) return s3.Length == 0 ? '\0' : s3[0];
                return Convert.ToChar(dbValue, CultureInfo.InvariantCulture);
            }
            // Guid: nORM stores it as a 36-char TEXT string (e.g. SqliteProvider writes ToString("D")); some
            // providers return a 16-byte BLOB. Convert.ChangeType can't coerce either (Guid is not
            // IConvertible), so a Guid column read through the generic path (e.g. an entity that also has a
            // value-converter column) would otherwise throw. Handle both representations here.
            if (underlyingType == typeof(Guid))
            {
                if (dbValue is string gs) return Guid.Parse(gs);
                if (dbValue is byte[] gb && gb.Length == 16) return new Guid(gb);
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
                    // Pass InvariantCulture: the database stores numeric/temporal scalars
                    // with invariant formatting, so a value read as a string (e.g. SQLite
                    // TEXT affinity) must not be reparsed under a comma-decimal locale —
                    // Convert.ChangeType(object, Type) uses CurrentCulture and would turn
                    // "1.5" into 15 (or throw) on Swedish/German machines.
                    var param = Expression.Parameter(typeof(object), "value");
                    var convert = Expression.Convert(
                        Expression.Call(typeof(Convert), nameof(Convert.ChangeType),
                            null,
                            Expression.Convert(param, from),
                            Expression.Constant(to),
                            Expression.Constant(CultureInfo.InvariantCulture, typeof(IFormatProvider))),
                        typeof(object));
                    return Expression.Lambda<Func<object, object>>(convert, param).Compile();
                }
                catch (InvalidCastException)
                {
                    // Fallback to runtime conversion when expression tree compilation fails
                    return value => Convert.ChangeType(value, to, CultureInfo.InvariantCulture);
                }
                catch (InvalidOperationException)
                {
                    // Fallback to runtime conversion when expression tree compilation fails
                    return value => Convert.ChangeType(value, to, CultureInfo.InvariantCulture);
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
