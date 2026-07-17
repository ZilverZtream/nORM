using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {
        /// <summary>
        /// THREAD STARVATION FIX: Synchronous cache execution wrapper.
        /// </summary>
        private TResult ExecuteWithCacheSync<TResult>(string cacheKey, IReadOnlyCollection<string> tables, TimeSpan expiration, Func<TResult> factory)
        {
            var cache = _ctx.Options.CacheProvider;
            // A non-positive expiration means "expires immediately": execute uncached instead of
            // letting the memory cache throw an opaque ArgumentOutOfRangeException AFTER the data
            // was already fetched. No TryGet either - an immediately-expired entry must not serve.
            if (cache == null || expiration <= TimeSpan.Zero)
                return factory();

            if (cache.TryGet(cacheKey, out TResult? cached))
                return cached!;

            var semaphore = _cacheLocks.GetOrAdd(cacheKey, _ => new SemaphoreSlim(1, 1));
            semaphore.Wait();
            try
            {
                if (!cache.TryGet(cacheKey, out cached))
                {
                    cached = factory();
                    cache.Set(cacheKey, cached!, expiration, tables);
                }
                return cached!;
            }
            finally
            {
                semaphore.Release();
                // Cleanup is intentionally deferred to the periodic CleanupCacheLocks timer.
                // Checking CurrentCount immediately after Release has a race where another thread could
                // Wait() between Release() and the check, causing removal of a semaphore still in use.
            }
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
                e = ((UnaryExpression)e).Operand;
            return e;
        }
        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, IReadOnlyCollection<string> tables, TimeSpan expiration, Func<Task<TResult>> factory, CancellationToken ct)
        {
            var cache = _ctx.Options.CacheProvider;
            // Non-positive expiration = "expires immediately": execute uncached (see sync twin).
            if (cache == null || expiration <= TimeSpan.Zero)
                return await factory().ConfigureAwait(false);
            if (cache.TryGet(cacheKey, out TResult? cached))
                return cached!;
            var semaphore = _cacheLocks.GetOrAdd(cacheKey, _ => new SemaphoreSlim(1, 1));
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (!cache.TryGet(cacheKey, out cached))
                {
                    cached = await factory().ConfigureAwait(false);
                    cache.Set(cacheKey, cached!, expiration, tables);
                }
                return cached!;
            }
            finally
            {
                semaphore.Release();
                // Cleanup is intentionally deferred to the periodic CleanupCacheLocks timer.
                // Checking CurrentCount immediately after Release has a race where another thread could
                // Wait() between Release() and the check, causing removal of a semaphore still in use.
            }
        }
        /// <summary>
        /// The result cache participates only OUTSIDE transactions. A read inside an active
        /// transaction observes uncommitted state - caching it lets a rollback poison later
        /// readers with never-committed rows, and serving a cached pre-transaction entry inside
        /// the transaction would hide its own writes. Covers both the explicit connection
        /// transaction and an ambient System.Transactions scope.
        /// </summary>
        private static readonly ConditionalWeakTable<DbConnection, object> s_connectionScopedDatabaseIds = new();

        private bool ResultCacheUsable(QueryPlan plan)
            => plan.IsCacheable
               && _ctx.Options.CacheProvider != null
               && _ctx.CurrentTransaction == null
               && System.Transactions.Transaction.Current == null;

        private string BuildCacheKeyFromPlan<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
        {
            var hasher = new XxHash128();
            AppendLengthPrefixedUtf8(hasher, plan.Sql.AsSpan());
            AppendLengthPrefixedUtf8(hasher, (typeof(TResult).AssemblyQualifiedName ?? typeof(TResult).FullName ?? typeof(TResult).Name).AsSpan());
            // Include a stable database identity in the result cache key so that two contexts
            // pointing to different databases with the same schema, query, and parameters
            // do not share a cache entry and return data from the wrong database.
            var dbIdentity = NormalizeConnectionStringForCacheKey(_ctx.Connection.ConnectionString);
            AppendUtf8(hasher, "DB".AsSpan());
            AppendLengthPrefixedUtf8(hasher, dbIdentity.AsSpan());
            // The connection string is a STALE identity once ChangeDatabase() has repointed the
            // live connection: providers update Connection.Database but keep the original
            // string, so a cacheable read after the swap would silently serve the PREVIOUS
            // database's rows. Key on the live database name as well (constant "main" for
            // SQLite, where the per-connection id below covers connection-private databases).
            AppendUtf8(hasher, "DBNAME".AsSpan());
            AppendLengthPrefixedUtf8(hasher, (_ctx.RawConnection.Database ?? string.Empty).AsSpan());
            // Connection-private databases (SQLite ':memory:' without shared cache) have
            // IDENTICAL connection strings but DIFFERENT data per connection - a shared cache
            // provider would serve one database's rows for another. Key them per connection
            // instance as well.
            if (_ctx.RawProvider.IsConnectionScopedDatabase(_ctx.Connection.ConnectionString))
            {
                var connectionId = (string)s_connectionScopedDatabaseIds.GetValue(
                    _ctx.Connection, static _ => Guid.NewGuid().ToString("N"));
                AppendUtf8(hasher, "CONNDB".AsSpan());
                AppendLengthPrefixedUtf8(hasher, connectionId.AsSpan());
            }
            if (_ctx.Options.TenantProvider != null)
            {
                var tenant = _ctx.GetRequiredTenantId("result cache key");
                AppendUtf8(hasher, "TENANT".AsSpan());
                AppendLengthPrefixedUtf8(hasher, tenant.ToString()!.AsSpan());
            }
            // Sort parameters deterministically so identical parameter sets produce the same hash
            // regardless of insertion order.
            foreach (var kvp in parameters.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                AppendUtf8(hasher, "PARAM".AsSpan());
                AppendLengthPrefixedUtf8(hasher, kvp.Key.AsSpan());
                if (kvp.Value is null)
                {
                    AppendByte(hasher, 0);
                    continue;
                }
                AppendByte(hasher, 1);
                AppendLengthPrefixedUtf8(hasher, (kvp.Value.GetType().AssemblyQualifiedName ?? kvp.Value.GetType().FullName ?? kvp.Value.GetType().Name).AsSpan());
                if (kvp.Value is byte[] bytesValue)
                {
                    AppendLengthPrefixedBytes(hasher, bytesValue);
                }
                // Temporal values MUST serialize at full (tick) precision: the general format the
                // IFormattable branch below uses drops fractional seconds for DateTime/DateTimeOffset
                // and drops seconds entirely for TimeOnly, so two Cacheable queries differing only by
                // a sub-second timestamp would collide on one key and the second would serve the
                // first's rows. The round-trip "O" format is exact to the tick and keeps Kind/offset.
                else if (kvp.Value is DateTime dtValue)
                {
                    AppendLengthPrefixedUtf8(hasher, dtValue.ToString("O", CultureInfo.InvariantCulture).AsSpan());
                }
                else if (kvp.Value is DateTimeOffset dtoValue)
                {
                    AppendLengthPrefixedUtf8(hasher, dtoValue.ToString("O", CultureInfo.InvariantCulture).AsSpan());
                }
                else if (kvp.Value is TimeOnly timeOnlyValue)
                {
                    AppendLengthPrefixedUtf8(hasher, timeOnlyValue.ToString("O", CultureInfo.InvariantCulture).AsSpan());
                }
                else if (kvp.Value is IFormattable formattable)
                {
                    AppendLengthPrefixedUtf8(hasher, formattable.ToString(null, CultureInfo.InvariantCulture)!.AsSpan());
                }
                else
                {
                    AppendLengthPrefixedUtf8(hasher, kvp.Value.ToString()!.AsSpan());
                }
            }
            Span<byte> hash = stackalloc byte[16];
            hasher.GetCurrentHash(hash);
            return Convert.ToHexString(hash);
        }
        /// <summary>
        /// Normalizes a connection string for use as a cache key component.
        /// Splits on ';', trims each pair, sorts case-insensitively, and rejoins so that
        /// different orderings of the same connection string map to the same key.
        /// Sensitive keys (credentials) are stripped so they never appear in cache key material.
        /// </summary>
        private static readonly HashSet<string> _sensitiveConnectionStringKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            "password", "pwd", "user password", "access token", "accesstoken", "token", "secret"
        };

        private static string NormalizeConnectionStringForCacheKey(string? cs)
        {
            if (string.IsNullOrEmpty(cs)) return string.Empty;
            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = cs };
                return string.Join(";",
                    builder.Keys.Cast<string>()
                        .Where(k => !_sensitiveConnectionStringKeys.Contains(k))
                        .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                        .Select(k => $"{k}={builder[k]}"));
            }
            catch (ArgumentException)
            {
                return Convert.ToHexString(
                    System.Security.Cryptography.SHA256.HashData(
                        System.Text.Encoding.UTF8.GetBytes(cs)));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendLengthPrefixedUtf8(XxHash128 hasher, ReadOnlySpan<char> value)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            AppendInt32(hasher, byteCount);
            AppendUtf8(hasher, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendLengthPrefixedBytes(XxHash128 hasher, ReadOnlySpan<byte> value)
        {
            AppendInt32(hasher, value.Length);
            hasher.Append(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendInt32(XxHash128 hasher, int value)
        {
            Span<byte> bytes = stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
            hasher.Append(bytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendUtf8(XxHash128 hasher, ReadOnlySpan<char> value)
        {
            if (value.IsEmpty)
                return;
            var byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount <= StackAllocUtf8Threshold)
            {
                Span<byte> buffer = stackalloc byte[byteCount];
                Encoding.UTF8.GetBytes(value, buffer);
                hasher.Append(buffer);
            }
            else
            {
                var rented = ArrayPool<byte>.Shared.Rent(byteCount);
                try
                {
                    var bytesWritten = Encoding.UTF8.GetBytes(value, rented);
                    hasher.Append(new ReadOnlySpan<byte>(rented, 0, bytesWritten));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented, clearArray: true);
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendByte(XxHash128 hasher, byte value)
        {
            Span<byte> b = stackalloc byte[1];
            b[0] = value;
            hasher.Append(b);
        }
    }
}
