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
    /// Controls how nORM handles projection expressions that cannot be fully
    /// translated to SQL but can be completed after server-side materialization.
    /// </summary>
    public enum ClientEvaluationPolicy
    {
        /// <summary>
        /// Allow the client-side projection tail and emit a diagnostic log entry
        /// when a logger is configured. This is the default v1 compatibility mode.
        /// </summary>
        Warn,

        /// <summary>
        /// Throw <see cref="NormUnsupportedFeatureException"/> instead of running
        /// any client-side projection tail.
        /// </summary>
        Throw,

        /// <summary>
        /// Allow the client-side projection tail without emitting the warning
        /// diagnostic.
        /// </summary>
        Allow
    }

    /// <summary>
    /// Controls whether a context permits provider-bound escape hatches or enforces
    /// nORM's strict provider mobility contract.
    /// </summary>
    public enum ProviderMobilityMode
    {
        /// <summary>
        /// Preserve the full public API surface, including explicit provider-bound
        /// escape hatches such as raw SQL, stored procedures and provider-native DDL.
        /// </summary>
        Compatibility,

        /// <summary>
        /// Permit only generated nORM paths whose semantics are translated,
        /// emulated or rejected deterministically by nORM before execution.
        /// Provider-bound escape hatches throw <see cref="NormUnsupportedFeatureException"/>.
        /// </summary>
        Strict
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
        private TemporalStorageMode _temporalStorageMode = TemporalStorageMode.NormManaged;
        private NativeTenantSecurityMode _nativeTenantSecurityMode = NativeTenantSecurityMode.Disabled;
        private ClientEvaluationPolicy _clientEvaluationPolicy = ClientEvaluationPolicy.Throw;
        private ProviderMobilityMode _providerMobilityMode = ProviderMobilityMode.Compatibility;

        /// <summary>
        /// Gets or sets the storage engine used when temporal versioning is enabled.
        /// The default is provider-neutral nORM-managed history tables and triggers.
        /// </summary>
        public TemporalStorageMode TemporalStorageMode
        {
            get => _temporalStorageMode;
            set
            {
                if (!Enum.IsDefined(typeof(TemporalStorageMode), value))
                    throw new NormConfigurationException($"Unsupported temporal storage mode '{value}'.");
                if (_providerMobilityMode == ProviderMobilityMode.Strict && value == TemporalStorageMode.ProviderNative)
                    throw new NormConfigurationException(
                        "Strict provider mobility requires nORM-managed temporal storage. Provider-native temporal storage is provider-bound.");
                _temporalStorageMode = value;
            }
        }
        // MEMORY SAFETY: Reduced default from 100,000 to 10,000 to prevent OOM.
        // 10 concurrent GroupJoins x 10k records x 1KB/record = ~100MB (safe for most environments).
        // Previous default (100k) could cause 1GB+ memory usage with concurrent queries.
        private int _maxGroupJoinSize = DefaultMaxGroupJoinSize;
        // Configurable recursion depth. Default 50 accommodates legitimate deep LINQ trees
        // built by report composers or dynamic filter builders.
        // Maximum 200 to prevent stack overflows from adversarial inputs.
        private int _maxRecursionDepth = DefaultMaxRecursionDepth;

        private int? _maxQueryJoinDepth;
        private int? _maxQueryWhereConditions;
        private int? _maxQueryParameterCount;
        private int? _maxQueryComplexityCost;

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
        /// Gets or sets an explicit upper bound on the number of joins a single query may
        /// contain before translation is rejected. When null (the default), the limit scales
        /// with available system memory (10 joins per GB, clamped between 1 and 16 GB), which
        /// means the same query can be admitted on one machine and rejected on a smaller one.
        /// Set an explicit value for deterministic admission across environments.
        /// </summary>
        public int? MaxQueryJoinDepth
        {
            get => _maxQueryJoinDepth;
            set
            {
                if (value is < 1)
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        "MaxQueryJoinDepth must be at least 1, or null to scale with available memory.");
                _maxQueryJoinDepth = value;
            }
        }

        /// <summary>
        /// Gets or sets an explicit upper bound on the number of WHERE conditions a single
        /// query may contain before translation is rejected. When null (the default), the
        /// limit scales with available system memory (50 conditions per GB, clamped between
        /// 1 and 16 GB). Set an explicit value for deterministic admission across environments,
        /// for example when dynamic filter builders or many global filters compose large predicates.
        /// </summary>
        public int? MaxQueryWhereConditions
        {
            get => _maxQueryWhereConditions;
            set
            {
                if (value is < 1)
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        "MaxQueryWhereConditions must be at least 1, or null to scale with available memory.");
                _maxQueryWhereConditions = value;
            }
        }

        /// <summary>
        /// Gets or sets an explicit upper bound on the number of parameters a single query may
        /// bind (including values expanded from local collections in Contains) before translation
        /// is rejected. When null (the default), the limit scales with available system memory
        /// (2000 parameters per GB, clamped between 1 and 16 GB). Providers additionally enforce
        /// their own hard parameter ceilings regardless of this setting.
        /// </summary>
        public int? MaxQueryParameterCount
        {
            get => _maxQueryParameterCount;
            set
            {
                if (value is < 1)
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        "MaxQueryParameterCount must be at least 1, or null to scale with available memory.");
                _maxQueryParameterCount = value;
            }
        }

        /// <summary>
        /// Gets or sets an explicit ceiling for the estimated complexity cost above which query
        /// translation is rejected. When null (the default), the ceiling scales with available
        /// system memory (10,000 per GB, clamped between 1 and 16 GB). The cost model weighs
        /// joins quadratically, so report-style queries with many joins are the most common
        /// reason to set an explicit ceiling. A warning is logged when a query exceeds half
        /// the effective ceiling.
        /// </summary>
        public int? MaxQueryComplexityCost
        {
            get => _maxQueryComplexityCost;
            set
            {
                if (value is < 1)
                    throw new ArgumentOutOfRangeException(nameof(value), value,
                        "MaxQueryComplexityCost must be at least 1, or null to scale with available memory.");
                _maxQueryComplexityCost = value;
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
        /// Gets or sets optional provider-native tenant security integration.
        /// When set to <see cref="NativeTenantSecurityMode.SessionContext"/>,
        /// nORM writes the current tenant ID into provider session state before
        /// generated commands execute. Database-native RLS policies can then use
        /// that value as defense in depth.
        /// </summary>
        public NativeTenantSecurityMode NativeTenantSecurityMode
        {
            get => _nativeTenantSecurityMode;
            set
            {
                if (!Enum.IsDefined(typeof(NativeTenantSecurityMode), value))
                    throw new NormConfigurationException($"Unsupported native tenant security mode '{value}'.");
                if (_providerMobilityMode == ProviderMobilityMode.Strict && value != NativeTenantSecurityMode.Disabled)
                    throw new NormConfigurationException(
                        "Strict provider mobility does not allow provider-native tenant security modes. Use generated-path tenant enforcement.");
                _nativeTenantSecurityMode = value;
            }
        }

        /// <summary>
        /// Gets or sets the provider-native session key used by
        /// <see cref="NativeTenantSecurityMode.SessionContext"/>.
        /// </summary>
        public string NativeTenantSessionKey { get; set; } = "norm.tenant_id";

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
        /// Gets or sets whether the store-generated-key convention (EF Core parity) is applied: a
        /// single-column integer primary key with no explicit value-generation config is treated as
        /// store-generated (generated when its value is default, honored when non-default) on providers that
        /// support it. Enabled by default. Set to <c>false</c> to keep such a key entirely client-set — the
        /// escape hatch for an existing table whose key column is NOT an identity/auto-increment/rowid column,
        /// or when the application manages all key values itself. Opting out only affects keys that have no
        /// explicit configuration; <c>[DatabaseGenerated]</c> and fluent <c>ValueGenerated*</c> always win.
        /// </summary>
        public bool UseStoreGeneratedKeyConvention { get; set; } = true;

        /// <summary>
        /// Gets or sets the default tracking behavior applied to queries executed
        /// against the context.
        /// </summary>
        public QueryTrackingBehavior DefaultTrackingBehavior { get; set; } = QueryTrackingBehavior.TrackAll;

        /// <summary>
        /// Gets or sets the policy used when an otherwise server-side query contains
        /// a projection tail that nORM can only evaluate on the client after
        /// materialization. Filters, joins, grouping, ordering and paging are still
        /// translated server-side before any allowed client projection is applied.
        /// </summary>
        public ClientEvaluationPolicy ClientEvaluationPolicy
        {
            get => _clientEvaluationPolicy;
            set
            {
                if (!Enum.IsDefined(typeof(ClientEvaluationPolicy), value))
                    throw new NormConfigurationException($"Unsupported client evaluation policy '{value}'.");
                if (_providerMobilityMode == ProviderMobilityMode.Strict && value == ClientEvaluationPolicy.Allow)
                    throw new NormConfigurationException(
                        "Strict provider mobility does not allow silent client evaluation. Use ClientEvaluationPolicy.Throw, or ClientEvaluationPolicy.Warn for explicit top-level projection tails after server filtering/paging.");
                _clientEvaluationPolicy = value;
            }
        }

        /// <summary>
        /// Gets or sets whether the context allows provider-bound escape hatches or
        /// enforces the strict provider mobility contract. The default preserves
        /// compatibility; release certification should use <see cref="ProviderMobilityMode.Strict"/>.
        /// </summary>
        public ProviderMobilityMode ProviderMobilityMode
        {
            get => _providerMobilityMode;
            set
            {
                if (!Enum.IsDefined(typeof(ProviderMobilityMode), value))
                    throw new NormConfigurationException($"Unsupported provider mobility mode '{value}'.");
                if (value == ProviderMobilityMode.Strict)
                {
                    if (_temporalStorageMode == TemporalStorageMode.ProviderNative)
                        throw new NormConfigurationException(
                            "Strict provider mobility requires nORM-managed temporal storage. Provider-native temporal storage is provider-bound.");
                    if (_nativeTenantSecurityMode != NativeTenantSecurityMode.Disabled)
                        throw new NormConfigurationException(
                            "Strict provider mobility does not allow provider-native tenant security modes. Use generated-path tenant enforcement.");
                    if (CommandInterceptors.Count > 0)
                        throw new NormConfigurationException(
                            "Strict provider mobility does not allow command interceptors because they can inspect, rewrite, or suppress generated DbCommand execution.");
                    if (_clientEvaluationPolicy == ClientEvaluationPolicy.Allow)
                        throw new NormConfigurationException(
                            "Strict provider mobility does not allow silent client evaluation. Use ClientEvaluationPolicy.Throw, or ClientEvaluationPolicy.Warn for explicit top-level projection tails after server filtering/paging.");
                }
                _providerMobilityMode = value;
            }
        }

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
        /// Creates an independent options instance with the same configuration values.
        /// Collection-backed settings are copied so generated contexts and other
        /// composition layers can extend callbacks without mutating the caller-owned
        /// options object.
        /// </summary>
        /// <returns>A cloned <see cref="DbContextOptions"/> instance.</returns>
        public DbContextOptions Clone()
        {
            var clone = new DbContextOptions
            {
                _bulkBatchSize = _bulkBatchSize,
                _temporalVersioningEnabled = _temporalVersioningEnabled,
                _temporalStorageMode = _temporalStorageMode,
                _nativeTenantSecurityMode = _nativeTenantSecurityMode,
                _clientEvaluationPolicy = _clientEvaluationPolicy,
                _providerMobilityMode = _providerMobilityMode,
                _maxGroupJoinSize = _maxGroupJoinSize,
                _maxRecursionDepth = _maxRecursionDepth,
                _maxQueryJoinDepth = _maxQueryJoinDepth,
                _maxQueryWhereConditions = _maxQueryWhereConditions,
                _maxQueryParameterCount = _maxQueryParameterCount,
                _maxQueryComplexityCost = _maxQueryComplexityCost,
                TimeoutConfiguration = new AdaptiveTimeoutManager.TimeoutConfiguration
                {
                    BaseTimeout = TimeoutConfiguration.BaseTimeout,
                    SimpleQueryTimeout = TimeoutConfiguration.SimpleQueryTimeout,
                    ComplexQueryTimeout = TimeoutConfiguration.ComplexQueryTimeout,
                    BulkOperationTimeout = TimeoutConfiguration.BulkOperationTimeout,
                    TransactionTimeout = TimeoutConfiguration.TransactionTimeout,
                    TimeoutMultiplierPerComplexity = TimeoutConfiguration.TimeoutMultiplierPerComplexity,
                    EnableAdaptiveTimeouts = TimeoutConfiguration.EnableAdaptiveTimeouts
                },
                Logger = Logger,
                RetryPolicy = RetryPolicy is null
                    ? null
                    : new RetryPolicy
                    {
                        MaxRetries = RetryPolicy.MaxRetries,
                        BaseDelay = RetryPolicy.BaseDelay,
                        ShouldRetry = RetryPolicy.ShouldRetry
                    },
                TenantProvider = TenantProvider,
                TenantColumnName = TenantColumnName,
                NativeTenantSessionKey = NativeTenantSessionKey,
                OnModelCreating = OnModelCreating,
                UseBatchedBulkOps = UseBatchedBulkOps,
                UsePreciseChangeTracking = UsePreciseChangeTracking,
                EagerChangeTracking = EagerChangeTracking,
                DefaultTrackingBehavior = DefaultTrackingBehavior,
                CacheProvider = CacheProvider,
                CacheExpiration = CacheExpiration,
                AmbientTransactionPolicy = AmbientTransactionPolicy,
                RequireMatchedRowOccSemantics = RequireMatchedRowOccSemantics
            };

            foreach (var interceptor in CommandInterceptors)
                clone.CommandInterceptors.Add(interceptor);
            foreach (var interceptor in SaveChangesInterceptors)
                clone.SaveChangesInterceptors.Add(interceptor);
            foreach (var (entityType, filters) in _globalFilters)
                clone._globalFilters[entityType] = new List<LambdaExpression>(filters);

            return clone;
        }

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
        /// Enforces nORM's strict provider mobility contract for this context.
        /// In strict mode, generated nORM query/write/temporal paths may translate,
        /// emulate or fail deterministically, while raw SQL, stored procedures,
        /// provider-native RLS and provider-native temporal storage are rejected.
        /// Client projection tails remain rejected by default; callers may opt
        /// into <see cref="Configuration.ClientEvaluationPolicy.Warn"/> for
        /// explicit top-level projection tails after server filters, ordering and
        /// paging have run.
        /// </summary>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions UseStrictProviderMobility()
        {
            ClientEvaluationPolicy = ClientEvaluationPolicy.Throw;
            ProviderMobilityMode = ProviderMobilityMode.Strict;
            return this;
        }

        /// <summary>
        /// Enforces nORM's strict provider mobility contract while selecting the
        /// client-evaluation policy for top-level projection tails. Strict mode
        /// permits <see cref="Configuration.ClientEvaluationPolicy.Throw"/> and
        /// <see cref="Configuration.ClientEvaluationPolicy.Warn"/> only; silent
        /// <see cref="Configuration.ClientEvaluationPolicy.Allow"/> remains a
        /// compatibility-mode setting.
        /// </summary>
        /// <param name="clientEvaluationPolicy">
        /// Either <see cref="Configuration.ClientEvaluationPolicy.Throw"/> to
        /// reject projection tails or <see cref="Configuration.ClientEvaluationPolicy.Warn"/>
        /// to log and apply top-level projection tails after server-side filters,
        /// ordering and paging.
        /// </param>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions UseStrictProviderMobility(ClientEvaluationPolicy clientEvaluationPolicy)
        {
            if (clientEvaluationPolicy == ClientEvaluationPolicy.Allow)
                throw new NormConfigurationException(
                    "Strict provider mobility does not allow silent client evaluation. Use ClientEvaluationPolicy.Throw, or ClientEvaluationPolicy.Warn for explicit top-level projection tails after server filtering/paging.");
            ClientEvaluationPolicy = clientEvaluationPolicy;
            ProviderMobilityMode = ProviderMobilityMode.Strict;
            return this;
        }

        /// <summary>
        /// Enables provider-native tenant session context integration for
        /// database-native RLS policies.
        /// </summary>
        /// <param name="sessionKey">Provider session key used to store the tenant ID.</param>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions EnableNativeTenantSessionContext(string sessionKey = "norm.tenant_id")
        {
            NativeTenantSecurityMode = NativeTenantSecurityMode.SessionContext;
            NativeTenantSessionKey = sessionKey;
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
            TemporalStorageMode = TemporalStorageMode.NormManaged;
            return this;
        }

        /// <summary>
        /// Enables temporal versioning using the selected storage mode.
        /// </summary>
        /// <param name="storageMode">Temporal storage engine to use.</param>
        /// <returns>The current <see cref="DbContextOptions"/> instance for chaining.</returns>
        public DbContextOptions EnableTemporalVersioning(TemporalStorageMode storageMode)
        {
            _temporalVersioningEnabled = true;
            TemporalStorageMode = storageMode;
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
        /// When <see langword="true"/> (default), using <c>[Timestamp]</c> OCC columns with a
        /// provider that uses affected-row semantics (e.g. MySQL with <c>useAffectedRows=true</c>)
        /// causes <see cref="NormConfigurationException"/> to be thrown at save time, preventing
        /// silent data-loss bugs.
        /// </summary>
        /// <remarks>
        /// <para><b>&#x26A0; ADVANCED UNSAFE MODE:</b> Setting this to <see langword="false"/> is an
        /// explicit opt-in to weakened OCC semantics. With affected-row MySQL semantics, a
        /// concurrent writer that updates a row to the <em>same</em> token value will NOT be
        /// detected as a conflict — the update will silently succeed even though the row was
        /// modified externally. This is a known data-integrity gap (audit finding SP1).</para>
        /// <para>Only disable this if you have verified that your workload cannot produce same-value
        /// token collisions, or if you are using a SELECT-then-verify strategy to compensate.
        /// Add <c>useAffectedRows=false</c> to the MySQL connection string for full OCC guarantees
        /// instead.</para>
        /// </remarks>
        public bool RequireMatchedRowOccSemantics { get; set; } = true;

        // C1: backing store is ConcurrentDictionary so outer dict ops are thread-safe.
        // Inner lists are replaced atomically (copy-on-write) in AddGlobalFilter so
        // readers in the query translation path always see a stable snapshot.
        // X1: IReadOnlyList prevents callers from mutating the inner list through the public API,
        // preserving the copy-on-write invariant that guards query-pipeline enumeration.
        private readonly ConcurrentDictionary<Type, IReadOnlyList<LambdaExpression>> _globalFilters = new();

        /// <summary>
        /// Gets a read-only view of global query filters keyed by entity type. Each entry
        /// contains a list of lambda expressions that will be applied to queries for the
        /// corresponding entity. Use <see cref="AddGlobalFilter{TEntity}(Expression{Func{TEntity, bool}})"/>
        /// to register new filters.
        /// </summary>
        public IReadOnlyDictionary<Type, IReadOnlyList<LambdaExpression>> GlobalFilters => _globalFilters;

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
                    throw new NormConfigurationException($"MaxRetries must be between 0 and {MaxRetryAttempts}.");
                if (RetryPolicy.BaseDelay <= TimeSpan.Zero)
                    throw new NormConfigurationException("BaseDelay must be positive.");
            }

            if (TimeoutConfiguration.BaseTimeout <= TimeSpan.Zero || TimeoutConfiguration.BaseTimeout > MaxBaseTimeout)
                throw new NormConfigurationException($"BaseTimeout must be positive and at most {MaxBaseTimeout.TotalHours} hour(s).");

            if (string.IsNullOrWhiteSpace(TenantColumnName))
                throw new NormConfigurationException("TenantColumnName cannot be null or empty.");
            if (!Enum.IsDefined(typeof(TemporalStorageMode), TemporalStorageMode))
                throw new NormConfigurationException($"Unsupported temporal storage mode '{TemporalStorageMode}'.");
            if (!Enum.IsDefined(typeof(ProviderMobilityMode), ProviderMobilityMode))
                throw new NormConfigurationException($"Unsupported provider mobility mode '{ProviderMobilityMode}'.");
            if (!Enum.IsDefined(typeof(ClientEvaluationPolicy), ClientEvaluationPolicy))
                throw new NormConfigurationException($"Unsupported client evaluation policy '{ClientEvaluationPolicy}'.");
            if (ProviderMobilityMode == ProviderMobilityMode.Strict)
            {
                if (ClientEvaluationPolicy == ClientEvaluationPolicy.Allow)
                    throw new NormConfigurationException(
                        "Strict provider mobility does not allow silent client evaluation. Use ClientEvaluationPolicy.Throw, or ClientEvaluationPolicy.Warn for explicit top-level projection tails after server filtering/paging.");
                if (TemporalStorageMode == TemporalStorageMode.ProviderNative)
                    throw new NormConfigurationException(
                        "Strict provider mobility requires nORM-managed temporal storage. Provider-native temporal storage is provider-bound.");
                if (NativeTenantSecurityMode != NativeTenantSecurityMode.Disabled)
                    throw new NormConfigurationException(
                        "Strict provider mobility does not allow provider-native tenant security modes. Use generated-path tenant enforcement.");
                if (CommandInterceptors.Count > 0)
                    throw new NormConfigurationException(
                        "Strict provider mobility does not allow command interceptors because they can inspect, rewrite, or suppress generated DbCommand execution.");
            }
            if (NativeTenantSecurityMode != NativeTenantSecurityMode.Disabled)
            {
                if (TenantProvider == null)
                    throw new NormConfigurationException(
                        "Native tenant security requires TenantProvider to be configured.");
                if (string.IsNullOrWhiteSpace(NativeTenantSessionKey))
                    throw new NormConfigurationException(
                        "NativeTenantSessionKey cannot be null or empty when native tenant security is enabled.");
            }

            if (CacheExpiration <= TimeSpan.Zero)
                throw new NormConfigurationException("CacheExpiration must be positive.");

            // Avoid LINQ allocation: iterate the list directly for null checks.
            foreach (var interceptor in CommandInterceptors)
            {
                if (interceptor == null)
                    throw new NormConfigurationException("CommandInterceptors cannot contain null entries.");
            }
            foreach (var interceptor in SaveChangesInterceptors)
            {
                if (interceptor == null)
                    throw new NormConfigurationException("SaveChangesInterceptors cannot contain null entries.");
            }
        }
    }
}
