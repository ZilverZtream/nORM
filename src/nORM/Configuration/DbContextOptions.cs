using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Enterprise;

#nullable enable

namespace nORM.Configuration
{
    public class DbContextOptions
    {
        public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public int BulkBatchSize { get; set; } = 1000;
        public IDbContextLogger? Logger { get; set; }
        public RetryPolicy? RetryPolicy { get; set; }
        public ITenantProvider? TenantProvider { get; set; }
        public string TenantColumnName { get; set; } = "TenantId";
        public Action<ModelBuilder>? OnModelCreating { get; set; }
        public bool UseBatchedBulkOps { get; set; } = false;
        public IList<IDbCommandInterceptor> CommandInterceptors { get; } = new List<IDbCommandInterceptor>();
        public IList<ISaveChangesInterceptor> SaveChangesInterceptors { get; } = new List<ISaveChangesInterceptor>();
        public IDbCacheProvider? CacheProvider { get; set; }
        public TimeSpan CacheExpiration { get; set; } = TimeSpan.FromMinutes(5);

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
    }
}