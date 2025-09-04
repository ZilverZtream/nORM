using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Enterprise;
using nORM.Execution;

#nullable enable

namespace nORM.Configuration
{
    public class DbContextOptions
    {
        private int _bulkBatchSize = 1000;
        private bool _temporalVersioningEnabled = false;

        public AdaptiveTimeoutManager.TimeoutConfiguration TimeoutConfiguration { get; set; } = new();

        [Obsolete("Use TimeoutConfiguration.BaseTimeout instead")]
        public TimeSpan CommandTimeout
        {
            get => TimeoutConfiguration.BaseTimeout;
            set => TimeoutConfiguration.BaseTimeout = value;
        }

        public int BulkBatchSize
        {
            get => _bulkBatchSize;
            set
            {
                if (value <= 0 || value > 10000)
                    throw new ArgumentOutOfRangeException(nameof(value), "BulkBatchSize must be between 1 and 10000");
                _bulkBatchSize = value;
            }
        }
        public IDbContextLogger? Logger { get; set; }
        public RetryPolicy? RetryPolicy { get; set; }
        public ITenantProvider? TenantProvider { get; set; }
        public string TenantColumnName { get; set; } = "TenantId";
        public Action<ModelBuilder>? OnModelCreating { get; set; }
        public bool UseBatchedBulkOps { get; set; } = false;
        public bool UsePreciseChangeTracking { get; set; } = false;
        public IList<IDbCommandInterceptor> CommandInterceptors { get; } = new List<IDbCommandInterceptor>();
        public IList<ISaveChangesInterceptor> SaveChangesInterceptors { get; } = new List<ISaveChangesInterceptor>();
        // Caching is opt-in and disabled by default
        public IDbCacheProvider? CacheProvider { get; set; } = null;
        public TimeSpan CacheExpiration { get; set; } = TimeSpan.FromMinutes(5);

        public DbContextOptions UseInMemoryCache()
        {
            this.CacheProvider = new NormMemoryCacheProvider();
            return this;
        }

        public DbContextOptions EnableTemporalVersioning()
        {
            _temporalVersioningEnabled = true;
            return this;
        }

        internal bool IsTemporalVersioningEnabled => _temporalVersioningEnabled;

        public IDictionary<Type, List<LambdaExpression>> GlobalFilters { get; } = new Dictionary<Type, List<LambdaExpression>>();

        public DbContextOptions AddGlobalFilter<TEntity>(Expression<Func<DbContext, TEntity, bool>> filter)
        {
            if (!GlobalFilters.TryGetValue(typeof(TEntity), out var list))
            {
                list = new List<LambdaExpression>();
                GlobalFilters[typeof(TEntity)] = list;
            }
            list.Add(filter);
            return this;
        }

        public DbContextOptions AddGlobalFilter<TEntity>(Expression<Func<TEntity, bool>> filter)
        {
            var ctxParam = Expression.Parameter(typeof(DbContext), "ctx");
            var lambda = Expression.Lambda<Func<DbContext, TEntity, bool>>(filter.Body, ctxParam, filter.Parameters[0]);
            return AddGlobalFilter(lambda);
        }

        public void Validate()
        {
            if (RetryPolicy != null)
            {
                if (RetryPolicy.MaxRetries < 0 || RetryPolicy.MaxRetries > 10)
                    throw new InvalidOperationException("MaxRetries must be between 0 and 10");
                if (RetryPolicy.BaseDelay <= TimeSpan.Zero)
                    throw new InvalidOperationException("BaseDelay must be positive");
            }

            if (TimeoutConfiguration.BaseTimeout <= TimeSpan.Zero || TimeoutConfiguration.BaseTimeout > TimeSpan.FromHours(1))
                throw new InvalidOperationException("BaseTimeout must be between 1 second and 1 hour");
        }
    }
}
