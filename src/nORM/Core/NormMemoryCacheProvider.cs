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

        public NormMemoryCacheProvider(Func<object?>? getTenantId = null)
        {
            _getTenantId = getTenantId;
        }

        private string QualifyTag(string tag)
        {
            if (_getTenantId == null)
                return tag;

            var tenant = _getTenantId();
            if (tenant == null)
                throw new InvalidOperationException("Tenant context required but not available");
            return $"TENANT:{tenant}:{tag}";
        }

        public bool TryGet<T>(string key, out T? value)
        {
            return _cache.TryGetValue(key, out value);
        }

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
