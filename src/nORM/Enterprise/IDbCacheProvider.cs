using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Defines a caching mechanism that can be used by the framework to store and
    /// retrieve results of database operations.
    /// </summary>
    public interface IDbCacheProvider
    {
        /// <summary>
        /// Attempts to retrieve a cached value for the specified <paramref name="key"/>.
        /// </summary>
        /// <typeparam name="T">Type of the cached value.</typeparam>
        /// <param name="key">The unique identifier of the cache entry.</param>
        /// <param name="value">When this method returns, contains the cached value if it was found.</param>
        /// <returns><c>true</c> if the value exists in the cache; otherwise, <c>false</c>.</returns>
        bool TryGet<T>(string key, out T? value);

        /// <summary>
        /// Stores a value in the cache with the specified expiration policy and tags.
        /// </summary>
        /// <typeparam name="T">Type of the value being cached.</typeparam>
        /// <param name="key">The unique identifier for the cache entry.</param>
        /// <param name="value">The value to cache.</param>
        /// <param name="expiration">How long the entry should remain in the cache.</param>
        /// <param name="tags">A collection of tags used to later invalidate related entries.</param>
        void Set<T>(string key, T value, TimeSpan expiration, IEnumerable<string> tags);

        /// <summary>
        /// Invalidates all cache entries associated with the specified <paramref name="tag"/>.
        /// </summary>
        /// <param name="tag">The tag identifying which cache entries to remove.</param>
        void InvalidateTag(string tag);
    }
}
