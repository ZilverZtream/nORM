using System;

#nullable enable

namespace nORM.Enterprise
{
    public interface IDbCacheProvider
    {
        bool TryGet<T>(string key, out T? value);
        void Set<T>(string key, T value, TimeSpan expiration);
    }
}
