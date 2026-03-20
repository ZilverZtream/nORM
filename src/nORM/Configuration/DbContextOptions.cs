using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Enterprise;
using nORM.Execution;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Gate E: Controls what happens when the provider cannot enlist in an ambient
    /// System.Transactions.TransactionScope. The default is <see cref="FailFast"/> so
    /// that callers are alerted rather than silently committing outside the scope.
    /// </summary>
    public enum AmbientTransactionEnlistmentPolicy
    {
        /// <summary>
        /// Throw <see cref="NormConfigurationException"/> when enlistment fails.
        /// This is the safe default — it ensures callers know their operations are NOT
        /// participating in the ambient scope and can choose how to handle it.
        /// </summary>
        FailFast,

        /// <summary>
        /// Log a warning and continue when enlistment fails. Operations will commit
        /// independently of the ambient TransactionScope. Use when targeting providers
        /// known to have limited ambient transaction support.
        /// </summary>
        BestEffort,

        /// <summary>
        /// Skip the EnlistTransaction call entirely. Use when the provider is known to
        /// not support it and the overhead or warnings are undesirable.
        /// </summary>
        Ignore
    }

    /// <summary>
    /// Represents the configurable options for a <see cref="DbContext"/> instance.
    /// These settings control behavior such as command timeouts, logging, caching and
    /// how entities are tracked and filtered across the context.
    /// </summary>
    public class DbContextOptions
    {
        /// <summary>Default number of records per bulk operation batch.</summary>
        internal const int DefaultBulkBatchSize = 1000;
        /// <summary>Maximum allowed bulk batch size.</summary>
        internal const int MaxBulkBatchSize = 10_000;
        /// <summary>Default maximum child entities per GroupJoin group (memory safety bound).</summary>
        internal const int DefaultMaxGroupJoinSize = 10_000;
        /// <summary>Default LINQ expression tree translation recursion depth limit.</summary>
        internal const int DefaultMaxRecursionDepth = 50;
        /// <summary>Absolute ceiling for recursion depth to prevent stack overflows from adversarial inputs.</summary>
        internal const int AbsoluteMaxRecursionDepth = 200;
        /// <summary>Maximum allowed retry attempts for transient failure recovery.</summary>
        internal const int MaxRetryAttempts = 10;
        /// <summary>Maximum allowed base timeout to prevent unbounded command durations.</summary>
        internal static readonly TimeSpan MaxBaseTimeout = TimeSpan.FromHours(1);
        /// <summary>Default cache entry expiration period.</summary>
        internal static readonly TimeSpan DefaultCacheExpiration = TimeSpan.FromMinutes(5);

        private int _bulkBatchSize = DefaultBulkBatchSize;
        private bool _temporalVersioningEnabled;
        // MEMORY SAFETY: Reduced default from 100,000 to 10,000 to prevent OOM.
        // 10 concurrent GroupJoins x 10k records x 1KB/record = ~100MB (safe for most environments).
        // Previous default (100k) could cause 1GB+ memory usage with concurrent queries.
        private int _maxGroupJoinSize = DefaultMaxGroupJoinSize;
        // Configurable recursion depth. Default 50 accommodates legitimate deep LINQ trees
        // built by report composers or dynamic filter builders.
        // Maximum 200 to prevent stack overflows from adversarial inputs.
        private int _maxRecursionDepth = DefaultMaxRecursionDepth;

        /// <summary>
        /// Gets or sets the timeout configuration used when executing database commands.
        /// This allows fine grained control over different operation types like queries,
        /// bulk operations or transactions.
        /// </summary>
        public AdaptiveTimeoutManager.TimeoutConfiguration TimeoutConfiguration { get; set; } = new();

        /// <summary>
        /// Gets or sets the maximum LINQ expression tree translation depth (recursion limit).
        /// Default: 50. Maximum: 200. Raise this value for complex query builders that produce
        /// deep nested LINQ trees (e.g., report composers with 30+ conditions). If you reach
        /// this limit, consider breaking the query into multiple simpler queries or using CTEs.
        /// </summary>
        public int MaxRecursionDepth
        {
            get => _maxRecursionDepth;
            set
            {
                if (value < 1 || value > AbsoluteMaxRecursionDepth)
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        $"MaxRecursionDepth must be between 1 and {AbsoluteMaxRecursionDepth}. " +
                        "If you need deeper nesting, consider breaking the query into multiple queries.");
                _maxRecursionDepth = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of child entities allowed per group in a GroupJoin operation.
        /// This prevents out-of-memory errors when joining large datasets. Set to a higher value for
        /// datasets with legitimate large hierarchies, or to int.MaxValue to disable the limit entirely.
        /// Default: 10,000 (reduced from 100,000 for better memory safety).
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
                if (value <= 0 || value > MaxBulkBatchSize)
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        $"BulkBatchSize must be between 1 and {MaxBulkBatchSize}.");
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
        public bool UseBatchedBulkOps { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the change tracker should capture
        /// original values for all properties to allow precise change detection.
        /// </summary>
        public bool UsePreciseChangeTracking { get; set; }

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
        public IDbCacheProvider? CacheProvider { get; set; }

        /// <summary>
        /// Gets or sets the time period after which cached entries expire when
        /// <see cref="CacheProvider"/> is configured.
        /// </summary>
        public TimeSpan CacheExpiration { get; set; } = DefaultCacheExpiration;

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
        /// Gate E: Gets or sets the policy applied when the ORM attempts to enlist the
        /// database connection in an ambient <see cref="System.Transactions.TransactionScope"/>
        /// but the provider throws during <c>EnlistTransaction()</c>.
        /// Default is <see cref="AmbientTransactionEnlistmentPolicy.FailFast"/> — this is the
        /// safest choice because it surfaces enlistment failures rather than silently committing
        /// outside the scope. Change to <see cref="AmbientTransactionEnlistmentPolicy.BestEffort"/>
        /// for providers with known enlistment limitations, or
        /// <see cref="AmbientTransactionEnlistmentPolicy.Ignore"/> to skip enlistment entirely.
        /// </summary>
        public AmbientTransactionEnlistmentPolicy AmbientTransactionPolicy { get; set; }
            = AmbientTransactionEnlistmentPolicy.FailFast;

        /// <summary>
        /// S1 enforcement: when <c>true</c>, <see cref="DbContext"/> will throw
        /// <see cref="NormConfigurationException"/> if the provider uses affected-row semantics
        /// (e.g. MySQL with <c>useAffectedRows=true</c>, the default) and an entity with an
        /// optimistic-concurrency token (<c>[Timestamp]</c>) is saved. Affected-row semantics
        /// cannot reliably detect conflicts where the concurrent writer sets the token to the
        /// same value; matched-row semantics (set <c>useAffectedRows=false</c> in the MySQL
        /// connection string and override the provider) are required for full OCC guarantees.
        ///
        /// <para>Default: <c>true</c> — throws <see cref="NormConfigurationException"/> when
        /// affected-row semantics are used with OCC tokens. Set to <c>false</c> to suppress the
        /// error and accept the known trade-off (S1), or add <c>useAffectedRows=false</c> to the
        /// MySQL connection string for full OCC guarantees.</para>
        /// </summary>
        public bool RequireMatchedRowOccSemantics { get; set; } = true;

        // C1: backing store is ConcurrentDictionary so outer dict ops are thread-safe.
        // Inner lists are replaced atomically (copy-on-write) in AddGlobalFilter so
        // readers in the query translation path always see a stable snapshot.
        private readonly ConcurrentDictionary<Type, List<LambdaExpression>> _globalFilters = new();

        /// <summary>
        /// Gets a dictionary of global query filters keyed by entity type. Each entry
        /// contains a list of lambda expressions that will be applied to queries for the
        /// corresponding entity.
        /// </summary>
        public IDictionary<Type, List<LambdaExpression>> GlobalFilters => _globalFilters;

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
            ArgumentNullException.ThrowIfNull(filter);
            // C1: copy-on-write so the query pipeline never sees a list mutated in place.
            _globalFilters.AddOrUpdate(
                typeof(TEntity),
                _ => new List<LambdaExpression> { filter },
                (_, existing) => new List<LambdaExpression>(existing) { filter });
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
            ArgumentNullException.ThrowIfNull(filter);
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
                if (RetryPolicy.MaxRetries < 0 || RetryPolicy.MaxRetries > MaxRetryAttempts)
                    throw new InvalidOperationException($"MaxRetries must be between 0 and {MaxRetryAttempts}.");
                if (RetryPolicy.BaseDelay <= TimeSpan.Zero)
                    throw new InvalidOperationException("BaseDelay must be positive.");
            }

            if (TimeoutConfiguration.BaseTimeout <= TimeSpan.Zero || TimeoutConfiguration.BaseTimeout > MaxBaseTimeout)
                throw new InvalidOperationException($"BaseTimeout must be positive and at most {MaxBaseTimeout.TotalHours} hour(s).");

            if (string.IsNullOrWhiteSpace(TenantColumnName))
                throw new InvalidOperationException("TenantColumnName cannot be null or empty.");

            if (CacheExpiration <= TimeSpan.Zero)
                throw new InvalidOperationException("CacheExpiration must be positive.");

            // Avoid LINQ allocation: iterate the list directly for null checks.
            foreach (var interceptor in CommandInterceptors)
            {
                if (interceptor == null)
                    throw new InvalidOperationException("CommandInterceptors cannot contain null entries.");
            }
            foreach (var interceptor in SaveChangesInterceptors)
            {
                if (interceptor == null)
                    throw new InvalidOperationException("SaveChangesInterceptors cannot contain null entries.");
            }
        }
    }
}
