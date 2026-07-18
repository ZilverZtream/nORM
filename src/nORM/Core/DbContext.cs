using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using nORM.Versioning;
#nullable enable
namespace nORM.Core
{
    /// <summary>
    /// Represents the primary entry point for interacting with a database using nORM.
    /// The <see cref="DbContext"/> manages the underlying <see cref="DbConnection"/>,
    /// maintains metadata such as <see cref="TableMapping"/> instances and coordinates
    /// change tracking, transactions and provider specific behavior. Instances are
    /// intended to be short lived and not thread safe.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("DbContext uses Expression-based query translation and reflection-built materializers; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("DbContext reflects over entity types to build mappings; trimming may remove the required members.")]
    public partial class DbContext : IDisposable, IAsyncDisposable
    {
        /// <summary>Maximum SQL length (in chars) before falling back to length-based complexity estimation.</summary>
        private const int SqlComplexityAnalysisMaxLength = 102_400; // 100 KB
        /// <summary>Divisor for base complexity from SQL string length in the fallback path.</summary>
        private const int SqlLengthComplexityDivisor = 100;
        /// <summary>Divisor for length-based complexity when SQL exceeds <see cref="SqlComplexityAnalysisMaxLength"/>.</summary>
        private const int LargeSqlLengthComplexityDivisor = 10_000;
        /// <summary>Baseline complexity floor for extremely large SQL strings (>100 KB).</summary>
        private const int LargeSqlBaselineComplexity = 20;
        /// <summary>Maximum complexity score cap to prevent timeout inflation from false positives.</summary>
        private const int MaxComplexityScore = 50;
        /// <summary>SQL Server error number indicating a deadlock victim (used by <see cref="UseDeadlockResilientSaveChanges"/>).</summary>
        private const int SqlServerDeadlockErrorNumber = 1205;
        /// <summary>Number of parameters reserved for internal use (e.g. tenant, timestamp) when computing batch sizes.</summary>
        private const int ParameterBudgetReserve = 10;
        /// <summary>Jitter range (+/-20%) applied to retry backoff delays to prevent thundering herd.</summary>
        private const double RetryJitterRange = 0.2;
        /// <summary>Maximum seconds to wait for the cleanup timer to drain during dispose.</summary>
        private const int CleanupTimerDrainTimeoutSeconds = 5;

        private readonly DbConnection _cn;
        // When false, Dispose/DisposeAsync must NOT close or dispose the connection
        // because it was passed in by the caller who retains ownership.
        private readonly bool _ownsConnection;
        private readonly DatabaseProvider _p;
        private readonly ConcurrentDictionary<Type, TableMapping> _m = new();
        /// <summary>Per-context fast-path SQL template cache. Keyed by entity type; stores provider+model-specific SELECT templates.</summary>
        internal readonly ConcurrentDictionary<Type, string> FastPathSqlCache = new();
        /// <summary>Pre-compiled regex for identifier validation. Matches word characters and spaces only.</summary>
        private static readonly System.Text.RegularExpressions.Regex s_safeIdentifierRegex =
            new(@"^[\w\s]+$", System.Text.RegularExpressions.RegexOptions.Compiled);
        /// <summary>
        /// SP1: Strict output-parameter name validator. Allows only letters, digits, and
        /// underscores (no spaces, no dots). Spaces/dots accepted by IsSafeIdentifier would
        /// produce invalid ADO.NET parameter names when prefixed with the provider's ParamPrefix.
        /// </summary>
        private static readonly System.Text.RegularExpressions.Regex s_safeOutputParamNameRegex =
            new(@"^[A-Za-z_][A-Za-z0-9_]*$", System.Text.RegularExpressions.RegexOptions.Compiled);

        private static bool IsSafeOutputParamName(string name)
            => !string.IsNullOrEmpty(name) && s_safeOutputParamNameRegex.IsMatch(name);
        /// <summary>Cached PropertyInfo for DbException.Number (provider-specific); avoids repeated reflection.</summary>
        private static readonly ConcurrentDictionary<Type, PropertyInfo?> s_numberPropertyCache = new();
        /// <summary>Cached MethodInfo for NormQueryable.Query&lt;T&gt;() generic method definition; avoids repeated reflection.</summary>
        private static readonly Lazy<MethodInfo> s_queryMethodInfo = new(() =>
            typeof(NormQueryable).GetMethods()
                .SingleOrDefault(m => m.Name == nameof(NormQueryable.Query) && m.IsGenericMethodDefinition)
                ?? throw new InvalidOperationException(
                    $"Could not find generic method '{nameof(NormQueryable.Query)}' on type '{nameof(NormQueryable)}'. " +
                    "The NormQueryable API surface may have changed."));
        /// <summary>Cached MethodInfo for NavigationPropertyExtensions.EnableLazyLoading&lt;T&gt;(); avoids per-call GetMethod reflection.</summary>
        private static readonly MethodInfo s_enableLazyLoadingMethod =
            typeof(NavigationPropertyExtensions).GetMethod(nameof(NavigationPropertyExtensions.EnableLazyLoading))!;
        /// <summary>
        /// Cached NormQueryProvider for this context. Avoids creating NormQueryProvider,
        /// IncludeProcessor, QueryExecutor, and BulkCudBuilder (4 heap allocations) on every Query&lt;T&gt;() call.
        /// </summary>
        private Query.NormQueryProvider? _cachedQueryProvider;
        /// <summary>Cached mapping hash for plan cache fingerprinting. Computed lazily once per context.
        /// Volatile to ensure visibility across threads when read after _mappingHashComputed is true.</summary>
        private volatile int _mappingHashCached;
        private volatile bool _mappingHashComputed;
        private readonly IExecutionStrategy _executionStrategy;
        private readonly AdaptiveTimeoutManager _timeoutManager;
        private readonly ModelBuilder _modelBuilder;
        private readonly DynamicEntityTypeGenerator _typeGenerator = new();
        // Static cache shared across all DbContext instances. Dynamic type generation is expensive
        // (uses Reflection.Emit), so sharing avoids recreating types per context.
        // Bounded LRU cache prevents unbounded memory growth as new dynamic types are generated.
        private static readonly ConcurrentLruCache<string, Lazy<Task<Type>>> _dynamicTypeCache = new(maxSize: 1000);
        private readonly LinkedList<WeakReference<IDisposable>> _disposables = new();
        private readonly object _disposablesLock = new();
        private readonly Timer _cleanupTimer;
        private bool _providerInitialized;
        private readonly SemaphoreSlim _providerInitLock = new(1, 1);
        private readonly object _providerInitSyncLock = new object(); // For synchronous initialization to avoid deadlock
        private string? _nativeTenantSessionAppliedKey;
        // A1 fix: replaced Lazy<Task> with an explicit double-checked lock so that the
        // CancellationToken passed to EnsureConnectionAsync is forwarded to TemporalManager.
        // Lazy<Task> fixes the factory at creation time with no way to accept a later ct.
        private readonly SemaphoreSlim _temporalInitLock = new(1, 1);
        private volatile bool _temporalInitComplete;
        private DbTransaction? _currentTransaction; // Access via Interlocked.* only
        private DbContextTransaction? _currentContextTransaction;
        // ConcurrentDictionary eliminates lock contention on the insert fast path.
        private readonly ConcurrentDictionary<(Type EntityType, bool HydrateGeneratedKeys), PreparedInsertCommand> _preparedInsertCache = new();
        private readonly ConcurrentDictionary<string, FastPathPreparedCommand> _fastPathPreparedCommandCache = new();
        private readonly bool _strictProviderMobility;
        private volatile bool _disposed;

        /// <summary>
        /// Gets the configuration options that control the behavior of this context
        /// instance including logging, retry policies and mapping conventions.
        /// </summary>
        public DbContextOptions Options { get; }

        /// <summary>
        /// Provides access to the tracking graph responsible for detecting changes
        /// on entities and orchestrating persistence operations.
        /// </summary>
        public ChangeTracker ChangeTracker { get; }

        /// <summary>
        /// Exposes database specific operations such as transaction management and
        /// command execution helpers.
        /// </summary>
        public DatabaseFacade Database { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbContext"/> class using an
        /// existing <see cref="DbConnection"/>. The context takes ownership of the
        /// connection and will not dispose it until the context itself is disposed.
        /// </summary>
        /// <param name="cn">The open or closed database connection.</param>
        /// <param name="p">The <see cref="DatabaseProvider"/> responsible for generating provider specific SQL.</param>
        /// <param name="options">Optional configuration controlling context behavior.</param>
        public DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options = null)
            : this(cn, p, options, ownsConnection: true) { }

        // Internal constructor that allows callers (e.g. migration runners) to pass an
        // externally-owned connection. When ownsConnection is false, Dispose/DisposeAsync will NOT
        // close or dispose the connection - the caller retains full ownership.
        internal DbContext(DbConnection cn, DatabaseProvider p, DbContextOptions? options, bool ownsConnection)
        {
            _cn = cn ?? throw new ArgumentNullException(nameof(cn));
            _ownsConnection = ownsConnection;
            _p = p ?? throw new ArgumentNullException(nameof(p));
            // IMPORTANT: Options is treated as effectively immutable after construction.
            // Mutating Options properties after the context is created leads to undefined behavior
            // because cached plans, prepared commands, and internal state depend on the initial values.
            Options = options ?? new DbContextOptions();
            Options.Validate();
            _strictProviderMobility = Options.ProviderMobilityMode == ProviderMobilityMode.Strict;
            // Only validate TenantColumnName when multi-tenancy is actually configured.
            // Without a TenantProvider, the column name is unused and may safely be null.
            if (Options.TenantProvider != null && string.IsNullOrWhiteSpace(Options.TenantColumnName))
                throw new ArgumentException("TenantColumnName cannot be null or empty when TenantProvider is configured.");
            if (Options.CacheExpiration <= TimeSpan.Zero)
                throw new ArgumentException("CacheExpiration must be positive.");
            // Avoid LINQ .Any() allocation on the constructor hot path; use index-based loop.
            for (int i = 0; i < Options.CommandInterceptors.Count; i++)
            {
                if (Options.CommandInterceptors[i] == null)
                    throw new ArgumentException("CommandInterceptors cannot contain null entries.");
            }
            for (int i = 0; i < Options.SaveChangesInterceptors.Count; i++)
            {
                if (Options.SaveChangesInterceptors[i] == null)
                    throw new ArgumentException("SaveChangesInterceptors cannot contain null entries.");
            }
            ChangeTracker = new ChangeTracker(Options);
            ChangeTracker.BindContext(this);
            _modelBuilder = new ModelBuilder();
            Options.OnModelCreating?.Invoke(_modelBuilder);
            Database = new DatabaseFacade(this);
            _executionStrategy = Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(this, Options.RetryPolicy)
                : new DefaultExecutionStrategy(this);
            _timeoutManager = new AdaptiveTimeoutManager(Options.TimeoutConfiguration,
                Options.Logger ?? NullLogger.Instance);
            // _temporalInitLock initialized via field declarations above;
            // initialization happens lazily inside EnsureConnectionSlowAsync when ct is available.
            _cleanupTimer = new Timer(_ => CleanupDisposables(), null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }
        /// <summary>
        /// Initializes a new <see cref="DbContext"/> by creating and owning a
        /// <see cref="DbConnection"/> using the supplied connection string and
        /// provider. This overload is convenient for scenarios where a connection
        /// has not yet been instantiated.
        /// </summary>
        /// <param name="connectionString">The database connection string.</param>
        /// <param name="p">The provider used to create the connection.</param>
        /// <param name="options">Optional configuration for the context.</param>
        public DbContext(string connectionString, DatabaseProvider p, DbContextOptions? options = null)
            : this(CreateConnectionSafe(connectionString, p), p, options)
        {
        }
    }

}
