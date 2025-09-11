using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Primitives;
using nORM.Enterprise;

namespace nORM.Core
{
    /// <summary>
    /// Default in-memory cache provider for nORM using Microsoft.Extensions.Caching.Memory.
    /// </summary>
    public class NormMemoryCacheProvider : IDbCacheProvider, IDisposable
    {
        private readonly MemoryCache _cache = new(new MemoryCacheOptions
        {
            SizeLimit = 10240
        });

        private readonly ConcurrentDictionary<string, CancellationTokenSource> _tagTokens = new();
        private readonly Func<object?>? _getTenantId;

        /// <summary>
        /// Creates a new <see cref="NormMemoryCacheProvider"/> optionally scoped by tenant.
        /// </summary>
        /// <param name="getTenantId">Delegate that returns the current tenant identifier or <c>null</c> for single-tenant usage.</param>
        public NormMemoryCacheProvider(Func<object?>? getTenantId = null)
        {
            _getTenantId = getTenantId;
        }

        /// <summary>
        /// Combines the provided tag with the current tenant identifier to produce a
        /// cache tag that is unique per tenant. When no tenant provider is configured
        /// the original tag is returned unchanged.
        /// </summary>
        /// <param name="tag">The tag to qualify.</param>
        /// <returns>A tenant-qualified tag string.</returns>
        private string QualifyTag(string tag)
        {
            if (_getTenantId == null)
                return tag;

            var tenant = _getTenantId();
            if (tenant == null)
                throw new InvalidOperationException("Tenant context required but not available");
            return $"TENANT:{tenant}:{tag}";
        }

        /// <summary>
        /// Attempts to retrieve a cached value.
        /// </summary>
        /// <param name="key">The unique cache key.</param>
        /// <param name="value">The cached value, if present.</param>
        /// <typeparam name="T">Type of the cached item.</typeparam>
        /// <returns><c>true</c> if the item was found; otherwise <c>false</c>.</returns>
        public bool TryGet<T>(string key, out T? value)
        {
            return _cache.TryGetValue(key, out value);
        }

        /// <summary>
        /// Stores a value in the cache with the specified expiration and tags.
        /// </summary>
        /// <param name="key">Key under which the value is stored.</param>
        /// <param name="value">Value to cache.</param>
        /// <param name="expiration">Absolute expiration for the cache entry.</param>
        /// <param name="tags">Tags used to group related cache entries.</param>
        /// <typeparam name="T">Type of the value being cached.</typeparam>
        public void Set<T>(string key, T value, TimeSpan expiration, IEnumerable<string> tags)
        {
            var options = new MemoryCacheEntryOptions()
                .SetSize(1)
                .SetAbsoluteExpiration(expiration);

            foreach (var tag in tags)
            {
                var qualified = QualifyTag(tag);
                var tokenSource = _tagTokens.GetOrAdd(qualified, _ => new CancellationTokenSource());
                options.AddExpirationToken(new CancellationChangeToken(tokenSource.Token));
            }

            _cache.Set(key, value, options);
        }

        /// <summary>
        /// Invalidates all cache entries associated with the specified tag.
        /// </summary>
        /// <param name="tag">Tag identifying the cached items to invalidate.</param>
        public void InvalidateTag(string tag)
        {
            var qualified = QualifyTag(tag);
            if (_tagTokens.TryRemove(qualified, out var tokenSource))
            {
                tokenSource.Cancel();
                tokenSource.Dispose();
            }
        }

        /// <summary>
        /// Releases resources held by the memory cache.
        /// </summary>
        public void Dispose() => _cache.Dispose();
    }
}
