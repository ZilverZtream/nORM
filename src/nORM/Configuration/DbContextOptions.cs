using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Enterprise;
using nORM.Execution;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Represents the configurable options for a <see cref="DbContext"/> instance.
    /// These settings control behavior such as command timeouts, logging, caching and
    /// how entities are tracked and filtered across the context.
    /// </summary>
    public class DbContextOptions
    {
        private int _bulkBatchSize = 1000;
        private bool _temporalVersioningEnabled = false;
        private int _maxGroupJoinSize = 100000;

        /// <summary>
        /// Gets or sets the timeout configuration used when executing database commands.
        /// This allows fine grained control over different operation types like queries,
        /// bulk operations or transactions.
        /// </summary>
        public AdaptiveTimeoutManager.TimeoutConfiguration TimeoutConfiguration { get; set; } = new();

        /// <summary>
        /// Gets or sets the maximum number of child entities allowed per group in a GroupJoin operation.
        /// This prevents out-of-memory errors when joining large datasets. Set to a higher value for
        /// datasets with legitimate large hierarchies, or to int.MaxValue to disable the limit entirely.
        /// Default: 100,000.
        /// </summary>
        public int MaxGroupJoinSize
        {
            get => _maxGroupJoinSize;
            set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(value), "MaxGroupJoinSize must be positive");
                _maxGroupJoinSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the base timeout applied to all commands.
        /// This property is obsolete and provided for backward compatibility. Use
        /// <see cref="AdaptiveTimeoutManager.TimeoutConfiguration.BaseTimeout"/> instead.
        /// </summary>
        [Obsolete("Use TimeoutConfiguration.BaseTimeout instead")]
        public TimeSpan CommandTimeout
        {
            get => TimeoutConfiguration.BaseTimeout;
            set => TimeoutConfiguration.BaseTimeout = value;
        }

        /// <summary>
        /// Gets or sets the number of records processed in each batch during bulk
        /// operations such as bulk insert, update or delete. The value must be between
        /// 1 and 10,000.
        /// </summary>
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

        /// <summary>
        /// Gets or sets the <see cref="ILogger"/> instance used to emit diagnostic
        /// messages from the framework.
        /// </summary>
        public ILogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the retry policy that governs how transient failures are
        /// handled during database operations.
        /// </summary>
        public RetryPolicy? RetryPolicy { get; set; }

        /// <summary>
        /// Gets or sets the tenant provider used to determine the current tenant for
        /// multi-tenant applications.
        /// </summary>
        public ITenantProvider? TenantProvider { get; set; }

        /// <summary>
        /// Gets or sets the name of the column used to store the tenant identifier in
        /// multi-tenant schemas. Defaults to <c>"TenantId"</c>.
        /// </summary>
        public string TenantColumnName { get; set; } = "TenantId";

        /// <summary>
        /// Gets or sets a callback invoked during model creation allowing additional
        /// configuration of the model.
        /// </summary>
        public Action<ModelBuilder>? OnModelCreating { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether bulk operations should be executed
        /// using smaller batched statements instead of a single large statement.
        /// </summary>
        public bool UseBatchedBulkOps { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether the change tracker should capture
        /// original values for all properties to allow precise change detection.
        /// </summary>
        public bool UsePreciseChangeTracking { get; set; } = false;

        /// <summary>
        /// Gets or sets a value determining whether entities should be marked as
        /// tracked eagerly upon materialization.
        /// </summary>
        public bool EagerChangeTracking { get; set; } = true;

        /// <summary>
        /// Gets or sets the default tracking behavior applied to queries executed
        /// against the context.
        /// </summary>
        public QueryTrackingBehavior DefaultTrackingBehavior { get; set; } = QueryTrackingBehavior.TrackAll;

        /// <summary>
        /// Gets the collection of command interceptors that will be invoked for every
        /// database command executed by the context.
        /// </summary>
        public IList<IDbCommandInterceptor> CommandInterceptors { get; } = new List<IDbCommandInterceptor>();

        /// <summary>
        /// Gets the collection of interceptors that run during <c>SaveChanges</c>
        /// operations.
        /// </summary>
        public IList<ISaveChangesInterceptor> SaveChangesInterceptors { get; } = new List<ISaveChangesInterceptor>();

        // Caching is opt-in and disabled by default
        /// <summary>
        /// Gets or sets the cache provider used to cache query results. When null,
        /// caching is disabled.
        /// </summary>
        public IDbCacheProvider? CacheProvider { get; set; } = null;

        /// <summary>
        /// Gets or sets the time period after which cached entries expire when
        /// <see cref="CacheProvider"/> is configured.
        /// </summary>
        public TimeSpan CacheExpiration { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Enables the built-in in-memory cache for query results using
        /// <see cref="NormMemoryCacheProvider"/>. The cache is scoped by tenant when a
        /// <see cref="ITenantProvider"/> is available.
        /// </summary>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions UseInMemoryCache()
        {
            this.CacheProvider = new NormMemoryCacheProvider(() => this.TenantProvider?.GetCurrentTenantId());
            return this;
        }

        /// <summary>
        /// Enables temporal versioning for all entities in the context. When enabled the
        /// provider generates history tables and triggers to track changes over time.
        /// </summary>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions EnableTemporalVersioning()
        {
            _temporalVersioningEnabled = true;
            return this;
        }

        internal bool IsTemporalVersioningEnabled => _temporalVersioningEnabled;

        /// <summary>
        /// Gets a dictionary of global query filters keyed by entity type. Each entry
        /// contains a list of lambda expressions that will be applied to queries for the
        /// corresponding entity.
        /// </summary>
        public IDictionary<Type, List<LambdaExpression>> GlobalFilters { get; } = new Dictionary<Type, List<LambdaExpression>>();

        /// <summary>
        /// Registers a global query filter that has access to both the current
        /// <see cref="DbContext"/> and the entity instance. The filter expression is
        /// applied to all queries for <typeparamref name="TEntity"/>.
        /// </summary>
        /// <typeparam name="TEntity">The entity type the filter applies to.</typeparam>
        /// <param name="filter">A lambda expression representing the filter.</param>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
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

        /// <summary>
        /// Registers a global query filter defined only in terms of the entity type.
        /// </summary>
        /// <typeparam name="TEntity">The entity type the filter applies to.</typeparam>
        /// <param name="filter">A lambda expression representing the filter.</param>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions AddGlobalFilter<TEntity>(Expression<Func<TEntity, bool>> filter)
        {
            var ctxParam = Expression.Parameter(typeof(DbContext), "ctx");
            var lambda = Expression.Lambda<Func<DbContext, TEntity, bool>>(filter.Body, ctxParam, filter.Parameters[0]);
            return AddGlobalFilter(lambda);
        }

        /// <summary>
        /// Validates the configured options ensuring all values fall within allowed ranges
        /// and required settings are provided. Throws <see cref="InvalidOperationException"/>
        /// if any configuration is invalid.
        /// </summary>
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

            if (string.IsNullOrWhiteSpace(TenantColumnName))
                throw new InvalidOperationException("TenantColumnName cannot be null or empty");

            if (CacheExpiration <= TimeSpan.Zero)
                throw new InvalidOperationException("CacheExpiration must be positive");

            if (CommandInterceptors.Any(i => i == null))
                throw new InvalidOperationException("CommandInterceptors cannot contain null entries");

            if (SaveChangesInterceptors.Any(i => i == null))
                throw new InvalidOperationException("SaveChangesInterceptors cannot contain null entries");
        }
    }
}
