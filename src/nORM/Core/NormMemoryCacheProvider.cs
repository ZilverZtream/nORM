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
                var tokenSource = _tagTokens.GetOrAdd(tag, _ => new CancellationTokenSource());
                options.AddExpirationToken(new CancellationChangeToken(tokenSource.Token));
            }

            _cache.Set(key, value, options);
        }

        public void InvalidateTag(string tag)
        {
            if (_tagTokens.TryRemove(tag, out var tokenSource))
            {
                tokenSource.Cancel();
                tokenSource.Dispose();
            }
        }

        public void Dispose() => _cache.Dispose();
    }
}
