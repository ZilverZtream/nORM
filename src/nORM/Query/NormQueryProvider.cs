using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
#nullable enable
namespace nORM.Query
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("NormQueryProvider builds and executes Expression trees via reflection; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("NormQueryProvider reflects over entity and query types; trimming may remove the required members.")]
    internal sealed partial class NormQueryProvider : IQueryProvider, IDisposable
    {
        /// <summary>Default initial capacity for list materialization when no Take hint is available.</summary>
        private const int DefaultListCapacity = 16;
        /// <summary>Maximum string parameter length that gets an explicit Size hint (avoids NVARCHAR(MAX) on SQL Server).</summary>
        // C-4/A-2 fix: reference the single authoritative constant from ParameterOptimizer
        // instead of duplicating the magic value 4000 here.
        private const int MaxInlineStringSize = ParameterOptimizer.MaxInlineStringSize;
        /// <summary>Threshold below which UTF-8 encoding uses stackalloc instead of ArrayPool rental.</summary>
        private const int StackAllocUtf8Threshold = 256;
        /// <summary>Fallback average plan size in bytes when no samples have been collected yet.</summary>
        private const int FallbackAvgPlanSizeBytes = 16 * 1024;
        /// <summary>Maximum bytes the plan cache is allowed to consume.</summary>
        private const long MaxPlanCacheBytes = 64L * 1024 * 1024;
        /// <summary>Percentage of total available memory to allocate for the plan cache (1%).</summary>
        private const int MemoryBudgetDivisor = 100;
        /// <summary>Minimum number of entries the plan cache will hold regardless of memory pressure.</summary>
        private const int MinPlanCacheEntries = 100;
        /// <summary>Maximum number of entries the plan cache will hold regardless of available memory.</summary>
        private const int MaxPlanCacheEntries = 10_000;
        /// <summary>Interval at which the plan cache monitor adjusts cache size based on memory pressure.</summary>
        private static readonly TimeSpan PlanCacheMonitorInterval = TimeSpan.FromMinutes(1);
        /// <summary>How long a plan stays in the LRU cache before expiry.</summary>
        private static readonly TimeSpan PlanCacheTimeToLive = TimeSpan.FromHours(1);
        /// <summary>Interval at which unused cache lock semaphores are cleaned up.</summary>
        private static readonly TimeSpan CacheLockCleanupInterval = TimeSpan.FromHours(1);

        internal readonly DbContext _ctx;
        private static readonly ConcurrentLruCache<ExpressionFingerprint, QueryPlan> _planCache =
            new(maxSize: CalculateInitialPlanCacheSize(), timeToLive: PlanCacheTimeToLive);
        private static readonly Timer _planCacheMonitor = new(AdjustPlanCacheSize, null, PlanCacheMonitorInterval, PlanCacheMonitorInterval);
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _cacheLocks = new();
        private static readonly Timer _cacheLockCleanupTimer = new(CleanupCacheLocks, null, CacheLockCleanupInterval, CacheLockCleanupInterval);
        // Cache GetElementType results to avoid repeated reflection.
        private static readonly ConcurrentDictionary<Type, Type> _elementTypeCache = new();
        // Singleton MaterializerFactory — only wraps static caches, no instance state.
        private static readonly MaterializerFactory _sharedMaterializerFactory = new();
        // Cache constructor existence checks to avoid repeated reflection.
        private static readonly ConcurrentDictionary<Type, bool> _constrainedQueryableCache = new();
        // Cache compiled queryable factory delegates to avoid Activator.CreateInstance on every LINQ chain step
        private static readonly ConcurrentDictionary<Type, Func<IQueryProvider, Expression, IQueryable>> _queryableFactoryCache = new();
        private static long _totalPlanSize;
        private static int _planSizeSamples;
        // Track live provider count so timers can be stopped when all providers are disposed.
        private static int _activeProviderCount;
        private readonly QueryExecutor _executor;
        private readonly IncludeProcessor _includeProcessor;
        private readonly BulkCudBuilder _cudBuilder;
        private readonly ConcurrentDictionary<string, string> _simpleSqlCache = new();
        private readonly ConcurrentDictionary<ExpressionFingerprint, PooledPlanCommand> _pooledPlanCommands = new();
        /// <summary>PERF: Dedicated count SQL cache with structured keys to avoid per-call string composition and predicate-shape collisions.</summary>
        private readonly ConcurrentDictionary<CountSqlCacheKey, (string Sql, bool NeedsParam)> _countSqlCache = new();
        /// <summary>PERF: Pooled prepared commands for parameterless count queries (keyed by SQL), each paired with a lock object to serialize concurrent access.</summary>
        private readonly ConcurrentDictionary<string, (DbCommand Cmd, object Lock)> _pooledCountCommands = new();

        private enum CountPredicateShape : byte
        {
            None,
            BoolTrue,
            BoolFalse,
            EqualityValue,
            EqualityNull,
            Complex
        }

        private readonly record struct CountSqlCacheKey(Type ElementType, string PredicateKey, CountPredicateShape Shape);

        private sealed class PooledPlanCommand
        {
            public PooledPlanCommand(DbCommand command)
            {
                Command = command;
            }

            public DbCommand Command { get; }
            public object Lock { get; } = new();
        }

        public NormQueryProvider(DbContext ctx)
        {
            _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
            _includeProcessor = new IncludeProcessor(ctx);
            _executor = new QueryExecutor(ctx, _includeProcessor);
            _cudBuilder = new BulkCudBuilder(ctx);
            // C1: restart timers if they were stopped when a new provider is created.
            if (Interlocked.Increment(ref _activeProviderCount) == 1)
            {
                _planCacheMonitor.Change(PlanCacheMonitorInterval, PlanCacheMonitorInterval);
                _cacheLockCleanupTimer.Change(CacheLockCleanupInterval, CacheLockCleanupInterval);
            }
        }
        public void Dispose()
        {
            foreach (var entry in _pooledCountCommands.Values)
                try { entry.Cmd.Dispose(); } catch (ObjectDisposedException) { /* already disposed — safe to ignore */ }
            _pooledCountCommands.Clear();
            foreach (var entry in _pooledPlanCommands.Values)
                try { entry.Command.Dispose(); } catch (ObjectDisposedException) { /* already disposed — safe to ignore */ }
            _pooledPlanCommands.Clear();

            // C1: stop background timers when the last provider is disposed so the process
            // can undergo deterministic teardown (e.g. in test runs / hosting teardown).
            if (Interlocked.Decrement(ref _activeProviderCount) <= 0)
            {
                Volatile.Write(ref _activeProviderCount, 0);
                _planCacheMonitor.Change(Timeout.Infinite, Timeout.Infinite);
                _cacheLockCleanupTimer.Change(Timeout.Infinite, Timeout.Infinite);
            }
        }
        public IQueryable CreateQuery(Expression expression)
        {
            var typeArgs = expression.Type.GetGenericArguments();
            if (typeArgs.Length == 0)
                throw new ArgumentException($"Expression type '{expression.Type}' has no generic arguments. Expected IQueryable<T>.", nameof(expression));
            var elementType = typeArgs[0];
            return CreateQueryInternal(elementType, expression);
        }
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            var query = CreateQueryInternal(typeof(TElement), expression);
            if (query is IQueryable<TElement> typedQuery)
            {
                return typedQuery;
            }
            throw new NormQueryException($"Unable to create IQueryable for type '{typeof(TElement)}'.");
        }
        private IQueryable CreateQueryInternal(Type elementType, Expression expression)
        {
            // Use cached compiled factory instead of Activator.CreateInstance on every LINQ chain step.
            // Each .Where/.Take/.OrderBy calls CreateQuery, so this is on a very hot path.
            var factory = _queryableFactoryCache.GetOrAdd(elementType, static t =>
            {
                bool constrained = _constrainedQueryableCache.GetOrAdd(t, static t2 =>
                {
                    if (!t2.IsClass) return false;
                    if (t2.Name.StartsWith("<>")) return false;
                    return t2.GetConstructor(Type.EmptyTypes) != null;
                });

                var queryableType = constrained
                    ? typeof(NormQueryableImpl<>).MakeGenericType(t)
                    : typeof(NormQueryableImplUnconstrained<>).MakeGenericType(t);

                var ctor = queryableType.GetConstructor(new[] { typeof(IQueryProvider), typeof(Expression) })!;
                var providerParam = System.Linq.Expressions.Expression.Parameter(typeof(IQueryProvider), "p");
                var exprParam = System.Linq.Expressions.Expression.Parameter(typeof(Expression), "e");
                var newExpr = System.Linq.Expressions.Expression.New(ctor, providerParam, exprParam);
                var cast = System.Linq.Expressions.Expression.Convert(newExpr, typeof(IQueryable));
                return System.Linq.Expressions.Expression.Lambda<Func<IQueryProvider, Expression, IQueryable>>(cast, providerParam, exprParam).Compile();
            });

            return factory(this, expression);
        }
        /// <summary>
        /// PERFORMANCE OPTIMIZATION: Enhanced cache lock cleanup.
        /// - Limits cleanup to prevent unbounded growth
        /// - Removes unused semaphores to allow GC collection
        /// - Processes locks in batches to reduce iteration overhead
        /// </summary>
        private static void CleanupCacheLocks(object? state)
        {
            const int MaxLocksToKeep = 1000;
            const int CleanupBatchSize = 100;

            if (_cacheLocks.Count <= MaxLocksToKeep)
                return;

            var locksToRemove = new List<(string Key, SemaphoreSlim Semaphore)>(CleanupBatchSize);

            foreach (var kvp in _cacheLocks)
            {
                // Only remove locks that are not currently in use
                if (kvp.Value.CurrentCount == 1)
                {
                    locksToRemove.Add((kvp.Key, kvp.Value));

                    // Process in batches to avoid holding iterator too long
                    if (locksToRemove.Count >= CleanupBatchSize)
                        break;
                }
            }

            // Remove in separate pass to avoid concurrent modification.
            // Use value-matching TryRemove so we don't accidentally remove a
            // concurrently re-inserted semaphore for the same key. Do NOT dispose —
            // threads holding a reference obtained via GetOrAdd before this removal can
            // still call Wait/WaitAsync safely on the semaphore. GC will collect it once
            // all references drop. SemaphoreSlim only allocates its underlying event lazily
            // (on contended WaitAsync); we checked CurrentCount == 1 (no waiters) so the
            // event is not allocated and no unmanaged resources are held.
            foreach (var (key, semaphore) in locksToRemove)
            {
                _cacheLocks.TryRemove(new KeyValuePair<string, SemaphoreSlim>(key, semaphore));
            }
        }
        private static int CalculateInitialPlanCacheSize()
            => CalculatePlanCacheSize(GC.GetGCMemoryInfo());
        private static void AdjustPlanCacheSize(object? state)
        {
            var info = GC.GetGCMemoryInfo();
            if (info.MemoryLoadBytes >= info.HighMemoryLoadThresholdBytes)
            {
                _planCache.Clear();
            }
            _planCache.SetMaxSize(CalculatePlanCacheSize(info));
        }
        private static int CalculatePlanCacheSize(GCMemoryInfo info)
        {
            var samples = Volatile.Read(ref _planSizeSamples);
            var avgPlanSize = samples == 0
                ? FallbackAvgPlanSizeBytes
                : (int)(Volatile.Read(ref _totalPlanSize) / samples);
            var cacheBytes = Math.Min(info.TotalAvailableMemoryBytes / MemoryBudgetDivisor, MaxPlanCacheBytes);
            var size = (int)(cacheBytes / avgPlanSize);
            return Math.Clamp(size, MinPlanCacheEntries, MaxPlanCacheEntries);
        }
        public TResult Execute<TResult>(Expression expression)
            => ExecuteSync<TResult>(expression);

        /// <summary>
        /// Synchronous query execution that uses a true synchronous code path rather than blocking on
        /// async methods. Used by IEnumerable.GetEnumerator() to avoid deadlocks from GetAwaiter().GetResult()
        /// and thread starvation on bounded thread pools.
        /// </summary>
        public TResult ExecuteSync<TResult>(Expression expression)
        {
            // Queryable.Aggregate with a sum-fold accumulator — rewrite to
            // SUM at translation time so we don't materialise the full result
            // set client-side. Supported shapes: 1-arg `(acc, x) => acc + x`
            // and 2-arg `seed, (acc, x) => acc + sub(x)`. Other Aggregate
            // shapes (resultSelector, non-additive fold) remain untranslated
            // and fall through to the unsupported-method error below.
            if (TryRewriteAggregateToSum<TResult>(expression, out var aggResult))
                return aggResult;

            if (TryGetCountQuery(expression, out var countSql, out var countParameters))
            {
                return ExecuteCountSync<TResult>(countSql, countParameters);
            }

            if (TryGetSimpleQuery(expression, out var sql, out var parameters, out var simpleMethodName))
            {
                return ExecuteSimpleSync<TResult>(sql, parameters, simpleMethodName);
            }
            // THREAD STARVATION FIX: Use true synchronous execution path
            return ExecuteInternalSync<TResult>(expression);
        }

        private bool TryRewriteAggregateToSum<TResult>(Expression expression, out TResult result)
        {
            result = default!;
            if (expression is not MethodCallExpression mc
                || mc.Method.DeclaringType != typeof(System.Linq.Queryable)
                || mc.Method.Name != nameof(System.Linq.Queryable.Aggregate))
                return false;

            // Three Aggregate overloads: 2 args (source, func), 3 args (source, seed, func),
            // 4 args (source, seed, func, resultSelector — unsupported).
            LambdaExpression? fold;
            object? seed;
            Type seedType;
            Expression source;
            if (mc.Arguments.Count == 2)
            {
                source = mc.Arguments[0];
                fold = StripQuotesLocal(mc.Arguments[1]) as LambdaExpression;
                seed = null;            // 1-arg form: throw on empty; equivalent to Sum() unless empty
                seedType = mc.Method.GetGenericArguments()[0];
            }
            else if (mc.Arguments.Count == 3)
            {
                source = mc.Arguments[0];
                if (!TryEvaluateConstant(mc.Arguments[1], out seed)) return false;
                fold = StripQuotesLocal(mc.Arguments[2]) as LambdaExpression;
                seedType = mc.Method.GetGenericArguments()[1];
            }
            else
            {
                return false;
            }

            if (fold == null || fold.Parameters.Count != 2)
                return false;

            var accParam = fold.Parameters[0];
            var elemParam = fold.Parameters[1];

            // Min/max fold: handled first so Math.Max/Min calls and Conditional
            // `x > acc ? x : acc` shapes lower to MAX/MIN before the Add check
            // rejects them.
            if (TryRewriteMinMaxAggregate<TResult>(mc, source, fold, accParam, elemParam, seed, mc.Arguments.Count == 2, out result))
                return true;

            if (fold.Body.NodeType != ExpressionType.Add)
                return false;

            // String-concat fold: handled separately so seed-aware-separator
            // (acc + (acc == "" ? "" : sep) + elem) and simple-separator
            // (acc + sep + elem) shapes both lower to GROUP_CONCAT via
            // GroupBy(_=>1) synthesis. Empty source returns the seed for the
            // 3-arg form and throws for the 2-arg form, matching .NET.
            if ((Nullable.GetUnderlyingType(seedType) ?? seedType) == typeof(string)
                && TryRewriteStringConcatAggregate<TResult>(mc, source, fold, accParam, elemParam, seed, mc.Arguments.Count == 2, out result))
            {
                return true;
            }

            var binary = (BinaryExpression)fold.Body;
            Expression sub;
            if (binary.Left == accParam) sub = binary.Right;
            else if (binary.Right == accParam) sub = binary.Left;
            else return false;
            if (ReferencesParameter(sub, accParam)) return false;

            // Build the equivalent Queryable.Sum call. nORM's DirectAggregate
            // translator only emits SUM SQL when given a selector lambda — the
            // no-selector overload Sum(IQueryable<T>) materialises the list
            // and aggregates client-side. Always synthesize the selector form
            // so the aggregation happens on the server.
            //
            // Identity sub over a Select-ed source (the common shape
            // `Select(r => proj).Aggregate(seed, (acc, s) => acc + s)`): peel
            // the Select so the synthesized Sum's selector emits `proj` on
            // the underlying entity rather than a meaningless `s => s` over
            // the projected scalar.
            var tSource = mc.Method.GetGenericArguments()[0];
            Expression sumSource = source;
            Expression sumBody = sub;
            ParameterExpression sumParam = elemParam;
            if (sub == elemParam
                && source is MethodCallExpression selectCall
                && selectCall.Method.DeclaringType == typeof(System.Linq.Queryable)
                && selectCall.Method.Name == nameof(System.Linq.Queryable.Select)
                && selectCall.Arguments.Count == 2
                && StripQuotesLocal(selectCall.Arguments[1]) is LambdaExpression selectLambda)
            {
                sumSource = selectCall.Arguments[0];
                sumParam = selectLambda.Parameters[0];
                sumBody = selectLambda.Body;
            }
            var subType = sumBody.Type;
            var actualTSource = sumSource.Type.IsGenericType
                ? sumSource.Type.GetGenericArguments()[0]
                : tSource;
            var sumLambda = Expression.Lambda(sumBody, sumParam);
            var sumMethod = FindSumOverload(actualTSource, subType);
            if (sumMethod == null) return false;
            var sumCall = Expression.Call(sumMethod, sumSource, Expression.Quote(sumLambda));
            object? sumResult;
            try
            {
                sumResult = Execute(sumCall);
            }
            catch (InvalidOperationException) when (mc.Arguments.Count == 2)
            {
                // Sum on empty returns 0; 1-arg Aggregate on empty must throw. Re-throw to match
                // the .NET semantics.
                throw;
            }

            // Combine sumResult + seed (if any). Use checked arithmetic only for integer types
            // since C# `+` defaults are unchecked at runtime.
            object finalValue = sumResult ?? Activator.CreateInstance(subType)!;
            if (seed != null)
            {
                finalValue = AddSeedToSum(seed, finalValue, seedType);
            }
            result = (TResult)Convert.ChangeType(finalValue, typeof(TResult), System.Globalization.CultureInfo.InvariantCulture)!;
            return true;
        }

        private static Expression StripQuotesLocal(Expression e)
        {
            while (e is UnaryExpression u && u.NodeType == ExpressionType.Quote) e = u.Operand;
            return e;
        }

        private static bool TryEvaluateConstant(Expression e, out object? value)
        {
            if (e is ConstantExpression ce) { value = ce.Value; return true; }
            try { value = Expression.Lambda(e).Compile().DynamicInvoke(); return true; }
            catch { value = null; return false; }
        }

        private static bool ReferencesParameter(Expression e, ParameterExpression p)
        {
            var found = false;
            new ParameterFinderVisitor(p, () => found = true).Visit(e);
            return found;
        }

        private sealed class ParameterFinderVisitor : ExpressionVisitor
        {
            private readonly ParameterExpression _target;
            private readonly Action _onFound;
            public ParameterFinderVisitor(ParameterExpression target, Action onFound) { _target = target; _onFound = onFound; }
            protected override Expression VisitParameter(ParameterExpression node)
            { if (node == _target) _onFound(); return node; }
        }

        // Holder used as the projected anon shape inside the synthesized
        // GroupBy().Select(g => new { V = string.Join(...) }) chain. Real
        // anonymous types would need a runtime emit; a named single-field
        // record is equivalent for the NewExpression-projection translator
        // path and saves the emit cost.
        public sealed class StringConcatAggResult
        {
            public string? V { get; }
            public StringConcatAggResult(string? v) => V = v;
        }

        // Detect string-concat fold shapes and rewrite to
        //   source.GroupBy(_ => 1).Select(g => string.Join(sep, g.Select(<projLambda>))).FirstOrDefault()
        // which the existing IGrouping aggregate translator (QueryTranslator.
        // Aggregates.cs:472) emits as GROUP_CONCAT/STRING_AGG.
        // Detect Aggregate min/max fold shapes:
        //   Conditional: (acc, x) => x [>/<] acc ? x : acc  (or acc[>/<]x ? acc : x)
        //   Method:      (acc, x) => Math.Max(acc, x) / Math.Min(acc, x)
        // Lowers to Queryable.Max / Queryable.Min on the source's element type
        // (peeling the outer Select if the fold body's "element-side" is the
        // elemParam itself, same trick as the sum-fold rewrite). For seed forms
        // we combine the SQL-side max/min with the seed using the same op.
        private bool TryRewriteMinMaxAggregate<TResult>(
            MethodCallExpression mc,
            Expression source,
            LambdaExpression fold,
            ParameterExpression accParam,
            ParameterExpression elemParam,
            object? seed,
            bool throwOnEmpty,
            out TResult result)
        {
            result = default!;
            // Try to detect "isMax" (true = MAX, false = MIN, null = no match).
            bool? isMax = null;
            // Math.Max(acc, x) / Math.Min(acc, x) — order-insensitive args.
            if (fold.Body is MethodCallExpression mathCall
                && mathCall.Method.DeclaringType == typeof(Math)
                && mathCall.Arguments.Count == 2
                && ((mathCall.Arguments[0] == accParam && mathCall.Arguments[1] == elemParam)
                    || (mathCall.Arguments[0] == elemParam && mathCall.Arguments[1] == accParam)))
            {
                if (mathCall.Method.Name == nameof(Math.Max)) isMax = true;
                else if (mathCall.Method.Name == nameof(Math.Min)) isMax = false;
            }
            // Conditional: x [>/<] acc ? x : acc — or acc [>/<] x ? acc : x.
            else if (fold.Body is ConditionalExpression cond
                     && cond.Test is BinaryExpression cmp
                     && (cmp.NodeType == ExpressionType.GreaterThan
                         || cmp.NodeType == ExpressionType.GreaterThanOrEqual
                         || cmp.NodeType == ExpressionType.LessThan
                         || cmp.NodeType == ExpressionType.LessThanOrEqual))
            {
                bool isGreater = cmp.NodeType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual;
                // Test == "x > acc"  : IfTrue=x, IfFalse=acc → MAX
                // Test == "acc > x"  : IfTrue=acc, IfFalse=x → MAX
                // (and mirrored for <)
                if (cmp.Left == elemParam && cmp.Right == accParam
                    && cond.IfTrue == elemParam && cond.IfFalse == accParam) isMax = isGreater;
                else if (cmp.Left == accParam && cmp.Right == elemParam
                         && cond.IfTrue == accParam && cond.IfFalse == elemParam) isMax = isGreater;
            }
            if (isMax == null) return false;

            // Peel `source = Select(r => proj)` so the synthesized Max/Min selector
            // projects an entity column rather than identity-over-scalar (same
            // trick as TryRewriteAggregateToSum's outer-Select unwrap).
            Expression aggSource = source;
            Expression projBody = elemParam;
            ParameterExpression projParam = elemParam;
            if (source is MethodCallExpression preSel
                && preSel.Method.DeclaringType == typeof(System.Linq.Queryable)
                && preSel.Method.Name == nameof(System.Linq.Queryable.Select)
                && preSel.Arguments.Count == 2
                && StripQuotesLocal(preSel.Arguments[1]) is LambdaExpression preSelLambda)
            {
                aggSource = preSel.Arguments[0];
                projParam = preSelLambda.Parameters[0];
                projBody = preSelLambda.Body;
            }
            var sourceGenArg = aggSource.Type.IsGenericType ? aggSource.Type.GetGenericArguments()[0] : projParam.Type;
            var subType = projBody.Type;
            var selectorLambda = Expression.Lambda(projBody, projParam);

            // Find Queryable.Max<TSource, TResult>(IQueryable<TSource>, Expression<Func<TSource,TResult>>).
            var aggMethod = FindAggregateOverload(isMax.Value ? nameof(System.Linq.Queryable.Max) : nameof(System.Linq.Queryable.Min), sourceGenArg, subType);
            if (aggMethod == null) return false;
            var aggCall = Expression.Call(aggMethod, aggSource, Expression.Quote(selectorLambda));

            object? sqlResult;
            try
            {
                sqlResult = Execute(aggCall);
            }
            catch (InvalidOperationException) when (throwOnEmpty)
            {
                // Max/Min on empty already throws "Sequence contains no elements".
                throw;
            }

            // Combine with seed if 3-arg overload. For empty-source: 1-arg form
            // already threw; 3-arg sees sqlResult==null and returns seed alone.
            if (seed != null && sqlResult != null)
            {
                sqlResult = isMax.Value
                    ? CombineMinMax(seed, sqlResult, subType, isMax: true)
                    : CombineMinMax(seed, sqlResult, subType, isMax: false);
            }
            else if (seed != null && sqlResult == null)
            {
                sqlResult = seed;
            }

            result = (TResult)System.Convert.ChangeType(sqlResult!, typeof(TResult), System.Globalization.CultureInfo.InvariantCulture)!;
            return true;
        }

        private static System.Reflection.MethodInfo? FindAggregateOverload(string methodName, Type tSource, Type returnType)
        {
            // Prefer the 2-generic overload Max<TSource,TResult>(...) since it
            // accepts any return type. Queryable also has 1-generic overloads
            // with fixed return types (int/long/double/decimal/etc.), but
            // those would match by return-type comparison only against the
            // already-instantiated Func signature.
            foreach (var m in typeof(System.Linq.Queryable).GetMethods())
            {
                if (m.Name != methodName) continue;
                if (!m.IsGenericMethodDefinition) continue;
                var ga = m.GetGenericArguments();
                if (ga.Length != 2) continue;
                var ps = m.GetParameters();
                if (ps.Length != 2) continue;
                return m.MakeGenericMethod(tSource, returnType);
            }
            return null;
        }

        private static object CombineMinMax(object seed, object sqlValue, Type accType, bool isMax)
        {
            accType = Nullable.GetUnderlyingType(accType) ?? accType;
            if (accType == typeof(int))
            {
                int a = System.Convert.ToInt32(seed), b = System.Convert.ToInt32(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            if (accType == typeof(long))
            {
                long a = System.Convert.ToInt64(seed), b = System.Convert.ToInt64(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            if (accType == typeof(double))
            {
                double a = System.Convert.ToDouble(seed), b = System.Convert.ToDouble(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            if (accType == typeof(decimal))
            {
                decimal a = System.Convert.ToDecimal(seed), b = System.Convert.ToDecimal(sqlValue);
                return isMax ? Math.Max(a, b) : Math.Min(a, b);
            }
            throw new NotSupportedException($"Aggregate min/max fold accumulator type '{accType.Name}' is not supported.");
        }

        private bool TryRewriteStringConcatAggregate<TResult>(
            MethodCallExpression mc,
            Expression source,
            LambdaExpression fold,
            ParameterExpression accParam,
            ParameterExpression elemParam,
            object? seed,
            bool throwOnEmpty,
            out TResult result)
        {
            result = default!;
            if (fold.Body is not BinaryExpression outerAdd || outerAdd.NodeType != ExpressionType.Add)
                return false;

            // Outer Add: (prefix) + elemExpr. elemExpr must not reference acc.
            var elemExpr = outerAdd.Right;
            if (ReferencesParameter(elemExpr, accParam)) throw new InvalidOperationException("DBG-SC3: elemExpr refs acc");

            // prefix is either `acc` (no separator) OR Add(acc, sepExpr).
            string sep;
            if (outerAdd.Left == accParam)
            {
                sep = "";
            }
            else if (outerAdd.Left is BinaryExpression innerAdd
                     && innerAdd.NodeType == ExpressionType.Add
                     && innerAdd.Left == accParam
                     && TryExtractSeparator(innerAdd.Right, out sep))
            {
                // ok
            }
            else
            {
                throw new InvalidOperationException($"DBG-SC4: outerAdd.Left shape unexpected: type={outerAdd.Left.GetType().Name} nodeType={outerAdd.Left.NodeType} body=[{outerAdd.Left}]");
            }

            // Synthesize: groupedSource.GroupBy(_ => 1).Select(g => string.Join(sep, g.Select(<proj>))).FirstOrDefault()
            // where <proj> projects each group element to its string value. For
            // `Query<T>().Select(r => r.Name).Aggregate(...)` peel the outer
            // Select so the inner-Select projects an entity column (which nORM
            // can translate) rather than an identity over a scalar parameter
            // (which can't reference any column).
            Expression groupedSource = source;
            Expression projBody = elemExpr;
            ParameterExpression projParam = elemParam;
            if (elemExpr == elemParam
                && source is MethodCallExpression preSel
                && preSel.Method.DeclaringType == typeof(System.Linq.Queryable)
                && preSel.Method.Name == nameof(System.Linq.Queryable.Select)
                && preSel.Arguments.Count == 2
                && StripQuotesLocal(preSel.Arguments[1]) is LambdaExpression preSelLambda)
            {
                groupedSource = preSel.Arguments[0];
                projParam = preSelLambda.Parameters[0];
                projBody = preSelLambda.Body;
            }
            // Ordering is preserved in groupedSource. HandleGroupBy sets _orderBy when
            // it processes the OrderBy chain. For the constant-key GroupBy (no GROUP BY
            // emitted), TranslateGroupAggregateMethod consumes _orderBy and routes it
            // through GetStringAggregateSql(expr, sep, orderBy) so each provider uses
            // its native ordered-aggregate syntax (WITHIN GROUP / inline ORDER BY / etc.).
            var sourceGenArg = groupedSource.Type.IsGenericType ? groupedSource.Type.GetGenericArguments()[0] : projParam.Type;
            var projLambda = Expression.Lambda(projBody, projParam);

            // GroupBy<TSource, TKey>(IQueryable<TSource>, Expression<Func<TSource, TKey>>)
            var groupKeyParam = Expression.Parameter(sourceGenArg, "_");
            var groupKeyLambda = Expression.Lambda(Expression.Constant(1), groupKeyParam);
            var groupByMethod = typeof(System.Linq.Queryable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Queryable.GroupBy) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2)
                .Where(m => m.GetParameters().Length == 2)
                .FirstOrDefault(m =>
                {
                    var p1 = m.GetParameters()[1].ParameterType;
                    if (!p1.IsGenericType) return false;
                    var fn = p1.GetGenericArguments()[0];
                    return fn.IsGenericType && fn.GetGenericArguments().Length == 2;
                });
            if (groupByMethod == null) throw new InvalidOperationException("DBG-SC6: groupByMethod is null");
            var groupByCall = Expression.Call(groupByMethod.MakeGenericMethod(sourceGenArg, typeof(int)), groupedSource, Expression.Quote(groupKeyLambda));

            // Inside the result selector: g.Select(<projLambda>) — Enumerable.Select
            // (NOT Queryable.Select; the second param is Func<,>, not Expression<Func<,>>).
            var iGroupingType = typeof(System.Linq.IGrouping<,>).MakeGenericType(typeof(int), sourceGenArg);
            var enumerableSelect = typeof(System.Linq.Enumerable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Enumerable.Select) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2 && m.GetParameters().Length == 2)
                .FirstOrDefault(m =>
                {
                    var p1 = m.GetParameters()[1].ParameterType;
                    // Want Func<TSource, TResult>, NOT Func<TSource, int, TResult>.
                    return p1.IsGenericType && p1.GetGenericTypeDefinition() == typeof(Func<,>);
                });
            if (enumerableSelect == null) throw new InvalidOperationException("DBG-SC7: enumerableSelect is null");
            var gParam = Expression.Parameter(iGroupingType, "g");
            // Enumerable.Select takes a Func, but Expression.Lambda<Func<...>>(...) decays
            // implicitly. Pass the LambdaExpression directly.
            var innerSelectCall = Expression.Call(
                enumerableSelect.MakeGenericMethod(sourceGenArg, typeof(string)),
                gParam,
                projLambda);

            // string.Join(string, IEnumerable<string>)
            var stringJoinMethod = typeof(string).GetMethod(nameof(string.Join), new[] { typeof(string), typeof(System.Collections.Generic.IEnumerable<string>) });
            if (stringJoinMethod == null) throw new InvalidOperationException("DBG-SC8: stringJoinMethod null");
            var joinCall = Expression.Call(stringJoinMethod, Expression.Constant(sep), innerSelectCall);

            // Wrap in Select(g => new { V = string.Join(...) }) — the existing
            // IGrouping-projection path emits BOTH groupKey AND value columns
            // for a scalar MethodCall body (Aggregates.cs:184). The single-
            // field NewExpression takes the NewExpression branch which only
            // emits the explicit args — exactly one column (the joined value)
            // so the scalar materialiser reads it correctly.
            var anonType = typeof(StringConcatAggResult);
            var anonCtor = anonType.GetConstructor(new[] { typeof(string) })!;
            var anonNew = Expression.New(anonCtor, new[] { joinCall }, new[] { anonType.GetMember("V")[0] });
            var resultSelector = Expression.Lambda(anonNew, gParam);
            var queryableSelect = typeof(System.Linq.Queryable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Queryable.Select) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2)
                .Where(m => m.GetParameters().Length == 2)
                .FirstOrDefault(m =>
                {
                    var p1 = m.GetParameters()[1].ParameterType;
                    if (!p1.IsGenericType) return false;
                    var fn = p1.GetGenericArguments()[0];
                    return fn.IsGenericType && fn.GetGenericArguments().Length == 2;
                });
            if (queryableSelect == null) throw new InvalidOperationException("DBG-SC9: queryableSelect null");
            var outerSelectCall = Expression.Call(
                queryableSelect.MakeGenericMethod(iGroupingType, anonType),
                groupByCall,
                Expression.Quote(resultSelector));

            var firstOrDefault = typeof(System.Linq.Queryable).GetMethods()
                .Where(m => m.Name == nameof(System.Linq.Queryable.FirstOrDefault) && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 1)
                .First(m => m.GetParameters().Length == 1)
                .MakeGenericMethod(anonType);
            var firstCall = Expression.Call(firstOrDefault, outerSelectCall);

            var row = Execute(firstCall) as StringConcatAggResult;
            var joined = row?.V;

            if (joined == null)
            {
                // Empty source. 1-arg form throws; 2-arg form returns seed.
                if (throwOnEmpty)
                    throw new InvalidOperationException("Sequence contains no elements");
                joined = seed as string ?? string.Empty;
            }

            result = (TResult)(object)joined;
            return true;
        }

        private static bool TryExtractSeparator(Expression sepExpr, out string sep)
        {
            sep = "";
            // Direct string constant: ", "
            if (TryEvaluateConstant(sepExpr, out var v) && v is string s1) { sep = s1; return true; }
            // Conditional: (acc == "" ? "" : sep) — extract the non-empty branch.
            if (sepExpr is ConditionalExpression cond)
            {
                if (TryEvaluateConstant(cond.IfTrue, out var t) && t is string tStr
                    && TryEvaluateConstant(cond.IfFalse, out var f) && f is string fStr)
                {
                    if (string.IsNullOrEmpty(tStr) && !string.IsNullOrEmpty(fStr)) { sep = fStr; return true; }
                    if (string.IsNullOrEmpty(fStr) && !string.IsNullOrEmpty(tStr)) { sep = tStr; return true; }
                }
            }
            return false;
        }

        private static System.Reflection.MethodInfo? FindSumNoSelectorOverload(Type elementType)
        {
            // Sum has a non-generic overload for each numeric type that takes
            // IQueryable<T> directly. Look them up by parameter shape.
            foreach (var m in typeof(System.Linq.Queryable).GetMethods())
            {
                if (m.Name != nameof(System.Linq.Queryable.Sum)) continue;
                if (m.IsGenericMethodDefinition) continue;
                var ps = m.GetParameters();
                if (ps.Length != 1) continue;
                var pt = ps[0].ParameterType;
                if (!pt.IsGenericType || pt.GetGenericTypeDefinition() != typeof(IQueryable<>)) continue;
                if (pt.GetGenericArguments()[0] == elementType) return m;
            }
            return null;
        }

        private static System.Reflection.MethodInfo? FindSumOverload(Type tSource, Type returnType)
        {
            foreach (var m in typeof(System.Linq.Queryable).GetMethods())
            {
                if (m.Name != nameof(System.Linq.Queryable.Sum)) continue;
                if (!m.IsGenericMethodDefinition || m.GetGenericArguments().Length != 1) continue;
                var ps = m.GetParameters();
                if (ps.Length != 2) continue;
                var selectorParam = ps[1].ParameterType;
                if (!selectorParam.IsGenericType) continue;
                var funcType = selectorParam.GetGenericArguments()[0];
                if (!funcType.IsGenericType) continue;
                var funcGen = funcType.GetGenericArguments();
                if (funcGen.Length != 2) continue;
                if (funcGen[1] != returnType) continue;
                return m.MakeGenericMethod(tSource);
            }
            return null;
        }

        private static object AddSeedToSum(object seed, object sumValue, Type accType)
        {
            // Normalize both to the accumulator type via direct casts on the
            // supported numeric types. Convert.ChangeType chokes on bool/null
            // and we don't need it: Aggregate's seed type drives accType.
            accType = Nullable.GetUnderlyingType(accType) ?? accType;
            if (accType == typeof(long))    return (long)System.Convert.ToInt64(seed)    + (long)System.Convert.ToInt64(sumValue);
            if (accType == typeof(int))     return (int)System.Convert.ToInt32(seed)     + (int)System.Convert.ToInt32(sumValue);
            if (accType == typeof(double))  return (double)System.Convert.ToDouble(seed) + (double)System.Convert.ToDouble(sumValue);
            if (accType == typeof(float))   return (float)System.Convert.ToSingle(seed)  + (float)System.Convert.ToSingle(sumValue);
            if (accType == typeof(decimal)) return (decimal)System.Convert.ToDecimal(seed) + (decimal)System.Convert.ToDecimal(sumValue);
            throw new NotSupportedException($"Aggregate sum-fold accumulator type '{accType.Name}' is not supported.");
        }
        public object? Execute(Expression expression) => Execute<object>(expression);
        public Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
        {
            if (TryGetCountQuery(expression, out var countSql, out var countParameters))
            {
                if (_ctx.Options.RetryPolicy != null)
                    return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCountAsync<TResult>(countSql, countParameters, token), ct);
                return ExecuteCountAsync<TResult>(countSql, countParameters, ct);
            }

            // Fast path – bypass translator for recognized simple patterns
            if (TryExecuteFastPath<TResult>(expression, ct, out var fastResult))
                return fastResult;
            // Simple query path (slightly higher overhead than fast path but handles more patterns)
            if (TryGetSimpleQuery(expression, out var sql, out var parameters, out var simpleMethodName))
            {
                if (_ctx.Options.RetryPolicy != null)
                    return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteSimpleAsync<TResult>(sql, parameters, simpleMethodName, token), ct);
                return ExecuteSimpleAsync<TResult>(sql, parameters, simpleMethodName, ct);
            }
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteInternalAsync<TResult>(expression, token), ct);
            return ExecuteInternalAsync<TResult>(expression, ct);
        }
        /// <summary>
        /// Executes a translated <c>DELETE</c> query asynchronously.
        /// </summary>
        /// <param name="expression">Expression representing the delete query.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>A task containing the number of rows affected.</returns>
        public Task<int> ExecuteDeleteAsync(Expression expression, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteDeleteInternalAsync(expression, token), ct);
            return ExecuteDeleteInternalAsync(expression, ct);
        }
        public Task<int> ExecuteUpdateAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteUpdateInternalAsync(expression, set, token), ct);
            return ExecuteUpdateInternalAsync(expression, set, ct);
        }
        /// <summary>
        /// Fast path execution using cached delegates instead of reflection.
        /// Eliminates MakeGenericMethod and Invoke overhead.
        /// </summary>
        private bool TryExecuteFastPath<TResult>(Expression expression, CancellationToken ct, out Task<TResult> result)
        {
            result = default!;
            var resultType = typeof(TResult);
            Type? elementType = null;

            if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(List<>))
            {
                elementType = resultType.GetGenericArguments()[0];
                try
                {
                    if (FastPathQueryExecutor.TryExecuteListNonGeneric(elementType, expression, _ctx, ct, out var typedListTask))
                    {
                        result = (Task<TResult>)typedListTask;
                        return true;
                    }
                }
                catch (NotSupportedException)
                {
                    // ignore and fall back to full translation path
                }
            }
            else if (resultType == typeof(int) || resultType == typeof(long))
            {
                if (expression is MethodCallExpression mc && mc.Arguments.Count > 0)
                {
                    var sourceType = mc.Arguments[0].Type;
                    if (sourceType.IsGenericType)
                        elementType = sourceType.GetGenericArguments()[0];
                }
            }

            if (elementType != null)
            {
                try
                {
                    // Use cached delegate instead of MakeGenericMethod + Invoke.
                    if (FastPathQueryExecutor.TryExecuteNonGeneric(elementType, expression, _ctx, ct, out var taskObject))
                    {
                        // Cast Task<object> to Task<TResult> without additional overhead
                        result = CastTaskResult<TResult>(taskObject);
                        return true;
                    }
                }
                catch (NotSupportedException)
                {
                    // ignore and fall back to full translation path
                }
            }
            return false;
        }

        /// <summary>
        /// Efficiently converts Task&lt;object&gt; to Task&lt;TResult&gt;.
        /// Avoids async state machine allocation when the task is already completed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Task<TResult> CastTaskResult<TResult>(Task<object> task)
        {
            // If the task is already completed (common for cached/fast queries),
            // skip the async state machine entirely.
            if (task.IsCompletedSuccessfully)
            {
                return Task.FromResult(ConvertScalarResult<TResult>(task.Result));
            }
            return CastTaskResultAsync<TResult>(task);
        }

        private static async Task<TResult> CastTaskResultAsync<TResult>(Task<object> task)
        {
            var result = await task.ConfigureAwait(false);
            return ConvertScalarResult<TResult>(result);
        }

        /// <summary>
        /// Converts scalar results without boxing for value types.
        /// Avoids Convert.ChangeType which forces heap allocation for value types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static TResult ConvertScalarResult<TResult>(object result)
        {
            // Guard: null/DBNull should have been handled by callers (ExecuteScalarPlan*),
            // but defend here to avoid NullReferenceException in the Convert paths below.
            if (result is null || result is DBNull)
                return default!;

            // Direct cast for reference types.
            if (typeof(TResult).IsClass || typeof(TResult).IsInterface)
            {
                return (TResult)result;
            }

            // Handle value types without Convert.ChangeType boxing.
            var resultType = typeof(TResult);
            var underlyingType = Nullable.GetUnderlyingType(resultType) ?? resultType;

            // Fast path for common scalar types - avoids boxing. Pass
            // InvariantCulture explicitly so non-English locales (Swedish,
            // German, etc.) don't parse '1.5' under comma-decimal rules and
            // throw FormatException. The database stores numeric scalars
            // using invariant formatting, so the read must use it too.
            var ic = System.Globalization.CultureInfo.InvariantCulture;
            if (underlyingType == typeof(int))
                return (TResult)(object)Convert.ToInt32(result, ic);
            if (underlyingType == typeof(long))
                return (TResult)(object)Convert.ToInt64(result, ic);
            if (underlyingType == typeof(double))
                return (TResult)(object)Convert.ToDouble(result, ic);
            if (underlyingType == typeof(decimal))
                return (TResult)(object)Convert.ToDecimal(result, ic);
            if (underlyingType == typeof(bool))
                return (TResult)(object)Convert.ToBoolean(result, ic);
            if (underlyingType == typeof(short))
                return (TResult)(object)Convert.ToInt16(result, ic);
            if (underlyingType == typeof(byte))
                return (TResult)(object)Convert.ToByte(result, ic);
            if (underlyingType == typeof(float))
                return (TResult)(object)Convert.ToSingle(result, ic);
            if (underlyingType == typeof(DateTime))
                return (TResult)(object)Convert.ToDateTime(result, ic);
            if (underlyingType == typeof(Guid))
                return (TResult)(object)(Guid)result;

            // Fallback for other types (still better than ChangeType for common cases above)
            return (TResult)Convert.ChangeType(result, underlyingType, ic)!;
        }
        private Task<TResult> ExecuteInternalAsync<TResult>(Expression expression, CancellationToken ct)
        {
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            // For cached queries, use the closure-based path (rare).
            // For non-cached queries (common), return task directly — no async state machine needed.
            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                return ExecuteInternalCachedAsync<TResult>(plan, paramValues, sw, ct);
            }
            return ExecuteQueryFromPlanAsync<TResult>(plan, paramValues, sw, ct);
        }

        private async Task<TResult> ExecuteInternalCachedAsync<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            var parameterDictionary = EnsureParameterDictionary(plan, paramValues);
            Func<Task<TResult>> queryExecutorFactory = () => ExecuteQueryFromPlanAsync<TResult>(plan, paramValues, sw, ct);
            var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, parameterDictionary);
            var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
            return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
        }
        /// <summary>
        /// Non-async entry point — does synchronous command setup when connection is ready,
        /// then dispatches to the appropriate async materializer. Avoids one async state machine
        /// allocation on the hot path.
        /// </summary>
        private Task<TResult> ExecuteQueryFromPlanAsync<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            // Check if connection is ready without awaiting
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteQueryFromPlanSlowAsync<TResult>(ensureTask, plan, paramValues, sw, ct);

            if (CanUsePooledPlanCommand(plan, paramValues))
                return ExecutePooledQueryPlanSync<TResult>(plan, sw, ct);

            // Synchronous command setup — no async state machine needed
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);

            // A1: pre-cancelled tokens are silently ignored on the warm sync path unless checked here.
            // EnsureConnectionAsync(ct) above returns immediately for warmed connections without
            // inspecting the token; the sync dispatchers below never check it either.
            ct.ThrowIfCancellationRequested();

            // Dispatch directly to materializer — avoids wrapping in another async method
            if (plan.IsScalar)
            {
                // Sync scalar for providers without true async I/O (SQLite)
                if (_ctx.Provider.PrefersSyncExecution || _ctx.Provider.PrefersSyncQueryPlanExecution)
                    return ExecuteScalarPlanSync<TResult>(plan, cmd, sw);
                return ExecuteScalarPlanAsync<TResult>(plan, cmd, sw, ct);
            }

            // For providers that don't support true async I/O (SQLite), use fully synchronous
            // materialization to eliminate per-row ReadAsync state machine overhead (~50ns × N rows).
            if (_ctx.Provider.PrefersSyncExecution || _ctx.Provider.PrefersSyncQueryPlanExecution)
                return ExecuteListPlanSyncWrapped<TResult>(plan, cmd, sw);

            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (Task<TResult>)(object)ExecuteObjectListPlanAsync(plan, cmd, sw, ct);

            return ExecuteListPlanAsync<TResult>(plan, cmd, sw, ct);
        }

        /// <summary>PERF: Slow path — connection needs initialization.</summary>
        private async Task<TResult> ExecuteQueryFromPlanSlowAsync<TResult>(Task<DbConnection> ensureTask, QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            if (CanUsePooledPlanCommand(plan, paramValues))
                return await ExecutePooledQueryPlanSync<TResult>(plan, sw, ct).ConfigureAwait(false);

            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);

            if (plan.IsScalar)
            {
                if (_ctx.Provider.PrefersSyncExecution || _ctx.Provider.PrefersSyncQueryPlanExecution)
                    return await ExecuteScalarPlanSync<TResult>(plan, cmd, sw).ConfigureAwait(false);
                return await ExecuteScalarPlanAsync<TResult>(plan, cmd, sw, ct).ConfigureAwait(false);
            }

            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (TResult)(object)await ExecuteObjectListPlanAsync(plan, cmd, sw, ct).ConfigureAwait(false);

            if (_ctx.Provider.PrefersSyncExecution || _ctx.Provider.PrefersSyncQueryPlanExecution)
                return await ExecuteListPlanSyncWrapped<TResult>(plan, cmd, sw).ConfigureAwait(false);

            return await ExecuteListPlanAsync<TResult>(plan, cmd, sw, ct).ConfigureAwait(false);
        }

        private bool CanUsePooledPlanCommand(QueryPlan plan, IReadOnlyList<object?>? paramValues)
            => _ctx.Provider.SupportsQueryPlanPreparedCommandCache &&
               _ctx.Options.CommandInterceptors.Count == 0 &&
               paramValues == null &&
               plan.CompiledParameters.Count == 0 &&
               !plan.IsScalar &&
               !plan.SingleResult &&
               plan.GroupJoinInfo == null &&
               plan.Includes.Count == 0 &&
               !plan.SplitQuery &&
               plan.DependentQueries is not { Count: > 0 } &&
               plan.M2MIncludes is not { Count: > 0 } &&
               plan.ClientProjection == null;

        private Task<TResult> ExecutePooledQueryPlanSync<TResult>(QueryPlan plan, Stopwatch? sw, CancellationToken ct)
        {
            var pooled = _pooledPlanCommands.GetOrAdd(plan.Fingerprint, _ => CreatePooledPlanCommand(plan));
            lock (pooled.Lock)
            {
                ct.ThrowIfCancellationRequested();
                pooled.Command.Transaction = _ctx.CurrentTransaction;
                var list = _executor.MaterializePooled(plan, pooled.Command);
                sw?.Stop();
                _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);
                return Task.FromResult((TResult)(object)list);
            }
        }

        private PooledPlanCommand CreatePooledPlanCommand(QueryPlan plan)
        {
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, null);
            try { cmd.Prepare(); } catch (Exception) { }
            return new PooledPlanCommand(cmd);
        }

        /// <summary>PERF: Extracted parameter binding to share between fast and slow paths.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BindPlanParameters(DbCommand cmd, QueryPlan plan, IReadOnlyList<object?>? paramValues)
        {
            var compiledParams = plan.CompiledParameters;
            if (compiledParams.Count == 0)
            {
                foreach (var p in plan.Parameters)
                    cmd.AddOptimizedParam(p.Key, p.Value);
            }
            else
            {
                if (!_compiledParamSets.TryGet(plan, out var compiledSet))
                {
                    compiledSet = new HashSet<string>(compiledParams, StringComparer.Ordinal);
                    _compiledParamSets.Set(plan, compiledSet);
                }
                foreach (var p in plan.Parameters)
                {
                    if (!compiledSet.Contains(p.Key))
                        cmd.AddOptimizedParam(p.Key, p.Value);
                }
                if (paramValues != null)
                {
                    var count = Math.Min(compiledParams.Count, paramValues.Count);
                    for (int i = 0; i < count; i++)
                        cmd.AddOptimizedParam(compiledParams[i], paramValues[i] ?? DBNull.Value);
                }
            }
        }

        /// <summary>PERF: Scalar materialization path.</summary>
        private async Task<TResult> ExecuteScalarPlanAsync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw, CancellationToken ct)
        {
            await using var command = cmd;
            var scalarResult = await command.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
            if (scalarResult == null || scalarResult is DBNull)
            {
                if (plan.MethodName is "Min" or "Max" or "Average" &&
                    typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                    throw new InvalidOperationException("Sequence contains no elements");
                // LINQ-to-Objects Sum returns 0 for empty / all-null source — even for
                // nullable element types (`Enumerable.Sum(IEnumerable<decimal?>)` returns 0,
                // not null). SQL `SUM(col)` returns NULL on the same input, so map NULL to
                // zero-of-target so the materialized result matches LINQ semantics.
                if (plan.MethodName == "Sum")
                    return GetZeroOfTargetType<TResult>();
                return default(TResult)!;
            }
            return ConvertScalarResult<TResult>(scalarResult)!;
        }

        /// <summary>PERF: Fully synchronous scalar path for providers without true async I/O.</summary>
        private Task<TResult> ExecuteScalarPlanSync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw)
        {
            var scalarResult = cmd.ExecuteScalarWithInterceptionSerializedAndDispose(_ctx);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
            if (scalarResult == null || scalarResult is DBNull)
            {
                if (plan.MethodName is "Min" or "Max" or "Average" &&
                    typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                    throw new InvalidOperationException("Sequence contains no elements");
                if (plan.MethodName == "Sum")
                    return Task.FromResult(GetZeroOfTargetType<TResult>());
                return Task.FromResult(default(TResult)!);
            }
            return Task.FromResult(ConvertScalarResult<TResult>(scalarResult)!);
        }

        // Returns the LINQ-Sum "empty source" value for TResult: 0 for value types
        // (including nullable wrappers like decimal? / int?), and default for reference
        // types (uncommon for Sum, but defensive).
        private static TResult GetZeroOfTargetType<TResult>()
        {
            var underlying = Nullable.GetUnderlyingType(typeof(TResult)) ?? typeof(TResult);
            if (!underlying.IsValueType)
                return default(TResult)!;
            // Activator.CreateInstance on a primitive value type yields the numeric zero;
            // boxing then casting to TResult (decimal / decimal? / int / int? / etc.) gives
            // back the correctly-typed zero, including the nullable wrapper case.
            return (TResult)Activator.CreateInstance(underlying)!;
        }

        /// <summary>PERF: List&lt;object&gt; materialization path — avoids covariant copy.</summary>
        private async Task<List<object>> ExecuteObjectListPlanAsync(QueryPlan plan, DbCommand cmd, Stopwatch? sw, CancellationToken ct)
        {
            var objectList = await _executor.MaterializeAsObjectListAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, objectList.Count);
            return objectList;
        }

        /// <summary>
        /// Fully synchronous materialization wrapped in Task.FromResult.
        /// Eliminates async state machine overhead for providers without true async I/O (SQLite).
        /// Saves ~50-100ns per Read() call ? ~1-4µs for typical result sets.
        /// </summary>
        private Task<TResult> ExecuteListPlanSyncWrapped<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw)
        {
            var list = _executor.Materialize(plan, cmd);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);

            object? result;
            if (plan.SingleResult)
            {
                result = plan.MethodName switch
                {
                    "First" or "MinBy" or "MaxBy" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                    "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                    "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                    "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                    "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                    "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "LastOrDefault" => list.Count > 0 ? list[0] : null,
                    _ => list
                };
            }
            else
            {
                if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                {
                    var countList = nonGenericList.Count;
                    var covariantList = new List<object>(countList);
                    for (int i = 0; i < countList; i++)
                        covariantList.Add(nonGenericList[i]!);
                    result = covariantList;
                }
                else
                {
                    result = list;
                }
            }
            return Task.FromResult((TResult)result!);
        }

        /// <summary>PERF: Typed list materialization path.</summary>
        private async Task<TResult> ExecuteListPlanAsync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw, CancellationToken ct)
        {
            var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);

            object? result;
            if (plan.SingleResult)
            {
                result = plan.MethodName switch
                {
                    "First" or "MinBy" or "MaxBy" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                    "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                    "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                    "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                    "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                    "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "LastOrDefault" => list.Count > 0 ? list[0] : null,
                    _ => list
                };
            }
            else
            {
                if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                {
                    var countList = nonGenericList.Count;
                    var covariantList = new List<object>(countList);
                    for (int i = 0; i < countList; i++)
                        covariantList.Add(nonGenericList[i]!);
                    result = covariantList;
                }
                else
                {
                    result = list;
                }
            }
            return (TResult)result!;
        }

        private static string BuildSimpleWhereCacheKey(LambdaExpression lambda)
        {
            if (lambda.Body is MemberExpression { Type: var memberType } member && memberType == typeof(bool))
                return member.Member.Name;

            if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not, Operand: MemberExpression { Type: var negatedType } negatedMember }
                && negatedType == typeof(bool))
                return string.Concat(negatedMember.Member.Name, ":BOOL_FALSE");

            if (lambda.Body is BinaryExpression { NodeType: ExpressionType.Equal } binary
                && binary.Left is MemberExpression comparedMember)
            {
                if (comparedMember.Type == typeof(bool) &&
                    ExpressionValueExtractor.TryGetConstantValue(binary.Right, out var boolValue) &&
                    boolValue is bool expected)
                    return expected
                        ? comparedMember.Member.Name
                        : string.Concat(comparedMember.Member.Name, ":BOOL_FALSE");

                if (IsNullConstant(binary.Right))
                    return string.Concat(comparedMember.Member.Name, ":NULL");

                return string.Concat(comparedMember.Member.Name, ":EQ");
            }

            return lambda.Body.ToString();
        }

        private bool TryGetSimpleQuery(Expression expr, out string sql, out Dictionary<string, object> parameters, out string? resultMethodName)
        {
            sql = string.Empty;
            parameters = _emptyParams;
            resultMethodName = null;
            if (_ctx.Options.GlobalFilters.Count > 0 || _ctx.Options.TenantProvider != null)
                return false;
            // Traverse to find root query and optional Where predicate
            MethodCallExpression? whereCall = null;
            Expression current = expr;
            // Unwrap result operators like First/Single/Take and skip AsNoTracking
            while (current is MethodCallExpression mc)
            {
                if (mc.Method.DeclaringType == typeof(Queryable))
                {
                    if (mc.Method.Name == nameof(Queryable.Where))
                    {
                        if (whereCall != null) return false; // only support single Where
                        whereCall = mc;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault))
                    {
                        resultMethodName = mc.Method.Name;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.Take))
                    {
                        // Accept Take so queries like .Where(...).Take(10) hit simple path;
                        // reject non-constant or negative Take values so validation falls through.
                        if (mc.Arguments[1] is not ConstantExpression takeConst || takeConst.Value is not int tv || tv < 0)
                            return false;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault))
                    {
                        return false;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (mc.Method.Name == "AsNoTracking" && mc.Arguments.Count >= 1)
                {
                    // Skip AsNoTracking (nORM-specific method, not on Queryable)
                    current = mc.Arguments[0];
                }
                else
                {
                    return false;
                }
            }
            if (current is not ConstantExpression constant)
                return false;
            var elementType = GetElementType(constant);
            var map = _ctx.GetMapping(elementType);
            string whereKey;
            if (whereCall == null)
                whereKey = "";
            else
            {
                var wLambda = StripQuotes(whereCall.Arguments[1]) as LambdaExpression;
                if (wLambda == null) return false;
                whereKey = BuildSimpleWhereCacheKey(wLambda);
            }
            var cacheKey = string.Concat("SIMPLE:", elementType.FullName, ":", resultMethodName ?? "", ":", whereKey);
            if (!_simpleSqlCache.TryGetValue(cacheKey, out var cachedSql))
            {
                // Use string interpolation instead of StringBuilder for small queries;
                // StringBuilder overhead is not worth it for simple SELECT statements.
                var columnList = string.Join(", ", map.Columns.Select(c => c.EscCol));
                string whereClause = "";

                if (whereCall != null)
                {
                    var lambda = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                    // Support boolean member: u => u.IsActive
                    if (lambda.Body is MemberExpression boolMember && boolMember.Type == typeof(bool))
                    {
                        if (!map.ColumnsByName.TryGetValue(boolMember.Member.Name, out var boolCol))
                            return false;
                        whereClause = $" WHERE {_ctx.Provider.FormatBooleanPredicate(boolCol.EscCol, expectedValue: true)}";
                    }
                    // Support negated boolean member: u => !u.IsActive
                    else if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr
                             && notExpr.Operand is MemberExpression negBoolMember
                             && negBoolMember.Type == typeof(bool))
                    {
                        if (!map.ColumnsByName.TryGetValue(negBoolMember.Member.Name, out var boolCol))
                            return false;
                        whereClause = $" WHERE {_ctx.Provider.FormatBooleanPredicate(boolCol.EscCol, expectedValue: false)}";
                    }
                    else
                    {
                        if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                            return false;
                        if (be.Left is not MemberExpression me)
                            return false;
                        if (!map.ColumnsByName.TryGetValue(me.Member.Name, out var column))
                            return false;
                        // Use ExpressionValueExtractor instead of Compile().DynamicInvoke();
                        // DynamicInvoke is significantly slower and poses RCE risks.
                        if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                            return false;
                        if (me.Type == typeof(bool) && value is bool boolValue)
                        {
                            whereClause = $" WHERE {_ctx.Provider.FormatBooleanPredicate(column.EscCol, boolValue)}";
                        }
                        // Null value: emit IS NULL (SQL "col = NULL" is always UNKNOWN/false).
                        else if (value == null || value == DBNull.Value)
                        {
                            whereClause = $" WHERE {column.EscCol} IS NULL";
                            // no parameters needed
                        }
                        else
                        {
                            var paramName = _ctx.Provider.ParamPrefix + "p0";
                            whereClause = $" WHERE {column.EscCol} = {paramName}";
                            parameters = new Dictionary<string, object>(1) { [paramName] = value };
                        }
                    }
                }

                sql = $"SELECT {columnList} FROM {map.EscTable}{whereClause}";
                _simpleSqlCache[cacheKey] = sql;
                return true;
            }
            sql = cachedSql!;
            // SQL cached; still need parameter value if where present
            if (whereCall != null)
            {
                var lambda = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                if ((lambda.Body is MemberExpression boolMember2 && boolMember2.Type == typeof(bool))
                    || (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr3
                        && notExpr3.Operand is MemberExpression negBm && negBm.Type == typeof(bool)))
                {
                    // no parameter to bind for boolean literal predicate
                }
                else
                {
                    if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                        return false;
                    if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                        return false;
                    // Boolean literals and NULL predicates are part of the SQL cache key and need no parameter.
                    var isBoolLiteralPredicate = be.Left is MemberExpression { Type: var memberType }
                                                 && memberType == typeof(bool)
                                                 && value is bool;
                    if (!isBoolLiteralPredicate && value != null && value != DBNull.Value)
                    {
                        var paramName = _ctx.Provider.ParamPrefix + "p0";
                        parameters = new Dictionary<string, object>(1) { [paramName] = value };
                    }
                }
            }
            return true;
        }

        private async Task<TResult> ExecuteSimpleAsync<TResult>(string sql, Dictionary<string, object> parameters, string? requestedMethodName, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            Type resultType = typeof(TResult);
            bool returnsList = false;
            Type elementType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(List<>) || genDef == typeof(IEnumerable<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                    returnsList = true;
                }
                else
                {
                    elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
                }
            }
            else
            {
                elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
            }
            var mapping = _ctx.GetMapping(elementType);

            // Use singleton MaterializerFactory (it only wraps static caches)
            var materializer = _sharedMaterializerFactory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(mapping, elementType);

            var plan = new QueryPlan(
                sql,
                parameters,
                Array.Empty<string>(),
                materializer,
                syncMaterializer,
                elementType,
                IsScalar: false,
                SingleResult: !returnsList,
                NoTracking: false,
                MethodName: returnsList ? "ToList" : nameof(Queryable.FirstOrDefault),
                Includes: _emptyIncludes,
                GroupJoinInfo: null,
                Tables: _simpleQueryTableCache.GetOrAdd(mapping.TableName, static t => new List<string>(1) { t }),
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw?.Elapsed ?? default, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // Preserve First vs FirstOrDefault semantics.
            if (list.Count > 0) return (TResult)list[0]!;
            if (requestedMethodName == nameof(Queryable.First))
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }
        private TResult ExecuteSimpleSync<TResult>(string sql, Dictionary<string, object> parameters, string? requestedMethodName = null)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            Type resultType = typeof(TResult);
            bool returnsList = false;
            Type elementType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(List<>) || genDef == typeof(IEnumerable<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                    returnsList = true;
                }
                else
                {
                    elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
                }
            }
            else
            {
                elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
            }
            var mapping = _ctx.GetMapping(elementType);

            // Use singleton MaterializerFactory (it only wraps static caches)
            var materializer = _sharedMaterializerFactory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(mapping, elementType);

            var plan = new QueryPlan(
                sql,
                parameters,
                Array.Empty<string>(),
                materializer,
                syncMaterializer,
                elementType,
                IsScalar: false,
                SingleResult: !returnsList,
                NoTracking: false,
                MethodName: returnsList ? "ToList" : nameof(Queryable.FirstOrDefault),
                Includes: _emptyIncludes,
                GroupJoinInfo: null,
                Tables: _simpleQueryTableCache.GetOrAdd(mapping.TableName, static t => new List<string>(1) { t }),
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = _executor.Materialize(plan, cmd);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw?.Elapsed ?? default, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // Preserve First vs FirstOrDefault semantics.
            if (list.Count > 0) return (TResult)list[0]!;
            if (requestedMethodName == nameof(Queryable.First))
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }

        /// <summary>
        /// THREAD STARVATION FIX: True synchronous execution path for complex queries.
        /// Uses synchronous ADO.NET methods instead of blocking on async code.
        /// </summary>
        private TResult ExecuteInternalSync<TResult>(Expression expression)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            IReadOnlyDictionary<string, object>? parameterDictionary = null;
            IReadOnlyDictionary<string, object> GetParameterDictionary()
            {
                parameterDictionary ??= EnsureParameterDictionary(plan, paramValues);
                return parameterDictionary;
            }

            Func<TResult> queryExecutorFactory = () =>
            {
                _ctx.EnsureConnection();
                using var cmd = _ctx.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                BindPlanParameters(cmd, plan, paramValues);

                object? result;
                if (plan.IsScalar)
                {
                    var scalarResult = cmd.ExecuteScalarWithInterceptionSerializedAndDispose(_ctx);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull)
                    {
                        if (plan.MethodName is "Min" or "Max" or "Average" &&
                            typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                            throw new InvalidOperationException("Sequence contains no elements");
                        return default(TResult)!;
                    }
                    result = ConvertScalarResult<TResult>(scalarResult)!;
                }
                else
                {
                    var list = _executor.Materialize(plan, cmd);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw?.Elapsed ?? default, list.Count);
                    if (plan.SingleResult)
                    {
                        result = plan.MethodName switch
                        {
                            "First" or "MinBy" or "MaxBy" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                            "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                            "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                            "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                            "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                            "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                            "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                            "LastOrDefault" => list.Count > 0 ? list[0] : null,
                            _ => list
                        };
                    }
                    else
                    {
                        if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                        {
                            var countList = nonGenericList.Count;
                            var covariantList = new List<object>(countList);
                            for (int i = 0; i < countList; i++)
                            {
                                covariantList.Add(nonGenericList[i]!);
                            }
                            result = covariantList;
                        }
                        else
                        {
                            result = list;
                        }
                    }
                }
                return (TResult)result!;
            };

            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, GetParameterDictionary());
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return ExecuteWithCacheSync(cacheKey, plan.Tables, expiration, queryExecutorFactory);
            }
            else
            {
                return queryExecutorFactory();
            }
        }

        /// <summary>
        /// Executes a DELETE statement represented by the provided LINQ expression. The method
        /// validates the generated plan, constructs the final SQL and executes it, returning the
        /// number of affected rows.
        /// </summary>
        /// <param name="expression">The LINQ expression describing the entities to delete.</param>
        /// <param name="ct">A token used to cancel the asynchronous operation.</param>
        /// <returns>The count of rows removed from the database.</returns>
        private async Task<int> ExecuteDeleteInternalAsync(Expression expression, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            string finalSql;
            if (plan.Tables.Count != 1)
            {
                ValidateJoinedCudShape(plan.BulkCudShape);
                finalSql = BuildJoinedCudWhereInSql("DELETE FROM " + mapping.EscTable, null, plan.Sql, mapping);
            }
            else
            {
                _cudBuilder.ValidateCudPlan(plan.BulkCudShape);
                var whereClause = _cudBuilder.GetWhereClauseWithOuterQualifier(plan.BulkCudShape, mapping.EscTable);
                finalSql = $"DELETE FROM {mapping.EscTable}{whereClause}";
            }
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = finalSql;
            BindPlanParameters(cmd, plan, paramValues);
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(finalSql, EnsureParameterDictionary(plan, paramValues), sw?.Elapsed ?? default, affected);
            return affected;
        }

        private static void ValidateJoinedCudShape(BulkCudQueryShape? shape)
        {
            if (shape == null)
                throw new NormUnsupportedFeatureException("ExecuteUpdate/Delete requires query-shape metadata.");
            if (shape.HasGroupBy || shape.HasOrderBy || shape.HasHaving || shape.HasDistinct || shape.HasPaging)
                throw new NormUnsupportedFeatureException(
                    "ExecuteUpdate/Delete with a join does not support grouped, ordered, distinct, or paged queries.");
        }

        private string BuildJoinedCudWhereInSql(string prefix, string? setSql, string planSql, TableMapping mapping)
        {
            if (mapping.KeyColumns.Length != 1)
                throw new NormUnsupportedFeatureException(
                    $"ExecuteDelete/UpdateAsync with a join requires a single-column primary key on {mapping.EscTable}. " +
                    "For composite-key entities use a correlated WHERE: " +
                    "`ctx.Query<T>().Where(t => ctx.Query<Other>().Any(o => o.Fk == t.Pk)).ExecuteDeleteAsync()`.");
            var fromIdx = planSql.IndexOf(" FROM ", StringComparison.Ordinal);
            if (fromIdx < 0)
                throw new InvalidOperationException("Cannot locate FROM clause in join SQL.");
            var pk = mapping.KeyColumns[0].EscCol;
            var subquery = "SELECT T0." + pk + planSql[fromIdx..];
            string whereIn;
            if (_ctx.Provider.CudWhereInSubqueryNeedsDoubleWrap)
                whereIn = pk + " IN (SELECT " + pk + " FROM (" + subquery + ") AS __nm_cud)";
            else
                whereIn = pk + " IN (" + subquery + ")";
            return setSql == null
                ? prefix + " WHERE " + whereIn
                : prefix + " SET " + setSql + " WHERE " + whereIn;
        }
        private async Task<int> ExecuteUpdateInternalAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            string finalSql;
            Dictionary<string, object> setParams;
            if (plan.Tables.Count != 1)
            {
                ValidateJoinedCudShape(plan.BulkCudShape);
                var (setClauseJ, setParamsJ) = _cudBuilder.BuildSetClause(mapping, set);
                setParams = setParamsJ;
                finalSql = BuildJoinedCudWhereInSql("UPDATE " + mapping.EscTable, setClauseJ, plan.Sql, mapping);
            }
            else
            {
                _cudBuilder.ValidateCudPlan(plan.BulkCudShape);
                var whereClause = _cudBuilder.GetWhereClauseWithOuterQualifier(plan.BulkCudShape, mapping.EscTable);
                var (setClause, setParamsSingle) = _cudBuilder.BuildSetClause(mapping, set);
                setParams = setParamsSingle;
                finalSql = $"UPDATE {mapping.EscTable} SET {setClause}{whereClause}";
            }
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = finalSql;
            BindPlanParameters(cmd, plan, paramValues);
            foreach (var p in setParams)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var baseDict = EnsureParameterDictionary(plan, paramValues);
            var allParams = baseDict is Dictionary<string, object> mutableDict && !ReferenceEquals(baseDict, plan.Parameters)
                ? mutableDict
                : new Dictionary<string, object>(baseDict);
            foreach (var p in setParams)
                allParams[p.Key] = p.Value;
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(finalSql, allParams, sw?.Elapsed ?? default, affected);
            return affected;
        }
        public async IAsyncEnumerable<T> AsAsyncEnumerable<T>(Expression expression, [EnumeratorCancellation] CancellationToken ct = default)
        {
            // Execute in true streaming mode so only one row is materialized at a time.
            var plan = GetPlan(expression, out _, out var paramValues);
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);
            if (plan.Includes.Count > 0 || plan.GroupJoinInfo != null)
                throw new NormUnsupportedFeatureException(
                    "AsAsyncEnumerable does not support Include or GroupJoin. Eager-load paths " +
                    "issue a dependent fetch after the principal materializer completes — incompatible " +
                    "with row-by-row streaming. Use `await query.ToListAsync()` to materialize the " +
                    "fully-loaded set in one round-trip, or remove the Include and reissue the " +
                    "child query manually per principal if streaming is required.");
            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null &&
                             _ctx.IsMapped(plan.ElementType);   // only mapped entity roots
            if (trackable)
                _ctx.GetMapping(plan.ElementType);
            var count = 0;
            await using var reader = await cmd
                .ExecuteReaderWithInterceptionAsync(
                    _ctx,
                    CommandBehavior.SingleResult,
                    ct)
                .ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = (T)await plan.Materializer(reader, ct).ConfigureAwait(false);
                if (trackable)
                {
                    var actualMap = _ctx.GetMapping(entity!.GetType());
                    var entry = _ctx.ChangeTracker.Track(entity!, EntityState.Unchanged, actualMap);
                    entity = (T)entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading((object)entity!, _ctx);
                }
                count++;
                yield return entity;
            }
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, EnsureParameterDictionary(plan, paramValues), sw?.Elapsed ?? default, count);
        }
        /// <summary>
        /// Returns the cached plan and binds parameters separately to avoid cloning the
        /// parameters dictionary on every cache hit.
        /// </summary>
        internal QueryPlan GetPlan(Expression expression, out Expression filtered, out IReadOnlyList<object?>? parameterValues)
        {
            filtered = ApplyGlobalFilters(expression);
            var elementType = GetElementType(UnwrapQueryExpression(filtered));
            var tenantHash = _ctx.Options.TenantProvider?.GetCurrentTenantId()?.GetHashCode() ?? 0;
            // Use cached mapping hash instead of recomputing on every query
            int mappingHash = _ctx.GetMappingHash();

            // Batch all 5 extends into a single hash operation (saves 4 XxHash128 calls)
            var fingerprint = ExpressionFingerprint
                .ComputeForPlanCache(filtered)
                .Extend(tenantHash, elementType.GetHashCode(), filtered.Type.GetHashCode(),
                        _ctx.Provider.GetType().GetHashCode(), mappingHash)
                .Extend((int)_ctx.Options.ClientEvaluationPolicy);

            // ExceptBy / IntersectBy / UnionBy capture an in-memory IEnumerable from
            // the user's closure into a post-materialize transform. The plan cache
            // keys by expression fingerprint, which doesn't differentiate captured
            // collection identity / contents; reusing the cached plan would replay
            // the prior call's collection. Bypass the cache for these methods so each
            // invocation translates fresh against the live closure values. (Future
            // work: model these like CompiledParameters so the captured collection
            // becomes a per-call lookup instead of a plan-baked closure.)
            bool bypassPlanCache = ExpressionContainsKeyedSetByOp(filtered);

            if (!bypassPlanCache && _planCache.TryGet(fingerprint, out var cached))
            {
                parameterValues = ExtractParameterValues(filtered, cached);
                return cached;
            }

            var localFiltered = filtered;
            QueryPlan plan;
            if (bypassPlanCache)
            {
                using var freshTranslator = new QueryTranslator(_ctx);
                var p = freshTranslator.Translate(localFiltered);
                plan = p with
                {
                    Fingerprint = fingerprint,
                    Parameters = new Dictionary<string, object>(p.Parameters),
                    CompiledParameters = new List<string>(p.CompiledParameters)
                };
                parameterValues = ExtractParameterValues(filtered, plan);
                return plan;
            }
            plan = _planCache.GetOrAdd(fingerprint, _ =>
            {
                using var translator = new QueryTranslator(_ctx);
                var before = GC.GetAllocatedBytesForCurrentThread();
                var p = translator.Translate(localFiltered);
                var after = GC.GetAllocatedBytesForCurrentThread();
                var size = after - before;
                Interlocked.Add(ref _totalPlanSize, size);
                Interlocked.Increment(ref _planSizeSamples);
                var clonedParams = new Dictionary<string, object>(p.Parameters);
                var clonedCompiledParams = new List<string>(p.CompiledParameters);
                return p with
                {
                    Fingerprint = fingerprint,
                    Parameters = clonedParams,
                    CompiledParameters = clonedCompiledParams
                };
            });

            parameterValues = ExtractParameterValues(filtered, plan);
            return plan;
        }

        /// <summary>
        /// Walks the expression tree looking for ExceptBy / IntersectBy / UnionBy
        /// method calls -- their second argument is a closure-captured in-memory
        /// collection that needs fresh evaluation per invocation. Plans containing
        /// these calls bypass the fingerprint-keyed plan cache to avoid replaying
        /// a prior call's captured collection.
        /// </summary>
        private static bool ExpressionContainsKeyedSetByOp(Expression expression)
        {
            return KeyedSetByOpDetector.Has(expression);
        }

        private sealed class KeyedSetByOpDetector : ExpressionVisitor
        {
            private bool _found;
            public static bool Has(Expression e)
            {
                var d = new KeyedSetByOpDetector();
                d.Visit(e);
                return d._found;
            }
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (!_found
                    && (node.Method.DeclaringType == typeof(System.Linq.Queryable)
                        || node.Method.DeclaringType == typeof(System.Linq.Enumerable))
                    && (node.Method.Name == "ExceptBy"
                        || node.Method.Name == "IntersectBy"
                        || node.Method.Name == "UnionBy"))
                {
                    _found = true;
                    return node;
                }
                return base.VisitMethodCall(node);
            }
        }

        /// <summary>
        /// Returns ONLY the extracted values, no Dictionary allocation.
        /// Reuses a thread-local extractor to avoid allocating a new visitor + List per query.
        /// </summary>
        [ThreadStatic] private static ParameterValueExtractor? t_extractor;
        private IReadOnlyList<object?>? ExtractParameterValues(Expression expression, QueryPlan plan)
        {
            if (plan.CompiledParameters.Count == 0)
                return null;

            var extractor = t_extractor ??= new ParameterValueExtractor();
            extractor.Reset();
            extractor.Visit(expression);

            // Copy values to a new list since the extractor will be reused
            return extractor.GetValuesCopy();
        }

        private IReadOnlyDictionary<string, object> EnsureParameterDictionary(QueryPlan plan, IReadOnlyList<object?>? parameterValues)
        {
            if (plan.CompiledParameters.Count == 0)
                return plan.Parameters;

            var parameters = new Dictionary<string, object>(plan.Parameters);
            if (parameterValues != null)
            {
                // The audit pattern (407e03d / eeff6e7 / cf39b61 / 04a0003 /
                // 7d6d7ac / c6c4710) repeatedly surfaced a silent-wrongness bug
                // where ETSV inline-folds consumed a closure MemberExpression's
                // value without reserving a compiled-param slot. ParameterValue
                // Extractor walks every closure unconditionally, so the value
                // array can drift longer than the compiled-param list -- and
                // Math.Min silently truncated, producing the wrong row set.
                // Assert equal counts in Debug so any future inline-fold that
                // forgets to reserve a placeholder surfaces immediately during
                // tests instead of as a hard-to-trace silent wrong-rows return.
                // Release stays with the Math.Min fallback so production users
                // get best-effort behavior rather than an assertion crash.
                System.Diagnostics.Debug.Assert(
                    parameterValues.Count == plan.CompiledParameters.Count,
                    $"ParameterValueExtractor produced {parameterValues.Count} values but plan has " +
                    $"{plan.CompiledParameters.Count} compiled-param slots. An ETSV inline-fold consumed a " +
                    $"closure MemberExpression without calling ReserveCompiledParamSlotIfClosure -- see " +
                    $"the audit thread (407e03d, eeff6e7, cf39b61, 04a0003, 7d6d7ac, c6c4710) for the fix " +
                    $"shape.");
                var count = Math.Min(plan.CompiledParameters.Count, parameterValues.Count);
                for (int i = 0; i < count; i++)
                {
                    parameters[plan.CompiledParameters[i]] = parameterValues[i] ?? DBNull.Value;
                }
            }
            else
            {
                for (int i = 0; i < plan.CompiledParameters.Count; i++)
                {
                    parameters[plan.CompiledParameters[i]] = DBNull.Value;
                }
            }

            return parameters;
        }
        private static Expression UnwrapQueryExpression(Expression expression)
        {
            return expression is MethodCallExpression mc &&
                   !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                   mc.Arguments.Count > 0
                ? mc.Arguments[0]
                : expression;
        }
        private Expression ApplyGlobalFilters(Expression expression)
        {
            // Skip the entire recursive walk when no global filters or tenant provider exist.
            // The recursion allocates new expression nodes (ToArray + Update) on every node even
            // when there are no filters to apply.
            if (_ctx.Options.GlobalFilters.Count == 0 && _ctx.Options.TenantProvider == null)
                return expression;

            if (expression is MethodCallExpression mc &&
                !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                mc.Arguments.Count > 0)
            {
                var filteredSource = ApplyGlobalFilters(mc.Arguments[0]);
                var args = mc.Arguments.ToArray();
                args[0] = filteredSource;
                return mc.Update(mc.Object, args);
            }
            var entityType = GetElementType(expression);
            if (_ctx.Options.GlobalFilters.Count > 0)
            {
                foreach (var kvp in _ctx.Options.GlobalFilters)
                {
                    if (!kvp.Key.IsAssignableFrom(entityType)) continue;
                    foreach (var filter in kvp.Value)
                    {
                        LambdaExpression lambda;
                        if (filter.Parameters.Count == 2)
                        {
                            var replacer = new ParameterReplacer(filter.Parameters[0], Expression.Constant(_ctx));
                            var body = replacer.Visit(filter.Body)!;
                            lambda = Expression.Lambda(body, filter.Parameters[1]);
                        }
                        else
                        {
                            lambda = filter;
                        }
                        expression = Expression.Call(
                            typeof(Queryable),
                            nameof(Queryable.Where),
                            new[] { entityType },
                            expression,
                            Expression.Quote(lambda));
                    }
                }
            }
            if (_ctx.Options.TenantProvider != null)
            {
                var map = _ctx.GetMapping(entityType);
                var tenantCol = map.TenantColumn;
                if (tenantCol != null)
                {
                    var param = Expression.Parameter(entityType, "t");
                    var prop = Expression.Property(param, tenantCol.Prop.Name);
                    var tenantId = _ctx.Options.TenantProvider.GetCurrentTenantId();
                    // Coerce the tenant ID to the mapped property type before building the expression
                    // constant. If TenantProvider.GetCurrentTenantId() returns a boxed long but the
                    // entity property is int (or any other cross-type mismatch), Expression.Constant
                    // would throw ArgumentException before translation. Convert.ChangeType handles
                    // common numeric widening/narrowing and string representations. If conversion is
                    // genuinely impossible (e.g., string "abc" ? int), throw a deterministic
                    // NormConfigurationException with an actionable message.
                    var propType = tenantCol.Prop.PropertyType;
                    object coercedTenantId;
                    if (tenantId == null)
                    {
                        var isNullable = !propType.IsValueType || Nullable.GetUnderlyingType(propType) != null;
                        if (!isNullable)
                            throw new nORM.Core.NormConfigurationException(
                                $"TenantProvider.GetCurrentTenantId() returned null but the tenant column " +
                                $"'{tenantCol.PropName}' on entity '{entityType.Name}' has non-nullable type " +
                                $"'{propType.FullName}'. Return a valid tenant ID or use a nullable column type.");
                        coercedTenantId = null!;  // safe: propType is nullable reference or Nullable<T>
                    }
                    else if (propType.IsInstanceOfType(tenantId))
                    {
                        coercedTenantId = tenantId;
                    }
                    else
                    {
                        var underlyingType = Nullable.GetUnderlyingType(propType) ?? propType;
                        try
                        {
                            coercedTenantId = Convert.ChangeType(tenantId, underlyingType,
                                System.Globalization.CultureInfo.InvariantCulture);
                        }
                        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
                        {
                            throw new nORM.Core.NormConfigurationException(
                                $"TenantProvider.GetCurrentTenantId() returned a value of type " +
                                $"'{tenantId.GetType().FullName}' which cannot be converted to the " +
                                $"tenant column property type '{propType.FullName}' " +
                                $"(column: '{tenantCol.PropName}', entity: '{entityType.Name}'). " +
                                $"Ensure TenantProvider returns a compatible type or update the entity mapping.", ex);
                        }
                    }
                    var constant = Expression.Constant(coercedTenantId, propType);
                    var body = Expression.Equal(prop, constant);
                    var lambda = Expression.Lambda(body, param);
                    expression = Expression.Call(
                        typeof(Queryable),
                        nameof(Queryable.Where),
                        new[] { entityType },
                        expression,
                        Expression.Quote(lambda));
                }
            }
            return expression;
        }
        /// <summary>
        /// Cached version of GetElementType to avoid repeated reflection.
        /// GetInterfaces() is expensive and this is called frequently in hot paths.
        /// </summary>
        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;

            // Fast path for generic types with arguments
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }

            // Cached reflection path
            return _elementTypeCache.GetOrAdd(type, static t =>
            {
                var iface = t.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
                if (iface != null) return iface.GetGenericArguments()[0];
                throw new ArgumentException($"Cannot determine element type from expression of type {t}");
            });
        }
        private sealed class ParameterValueExtractor : ExpressionVisitor
        {
            private readonly List<object?> _values = new();

            public void Reset() => _values.Clear();

            /// <summary>
            /// Returns a copy of extracted values (caller owns the array).
            /// Uses array for less overhead than List when count is small.
            /// </summary>
            public object?[] GetValuesCopy() => _values.ToArray();

            protected override Expression VisitConstant(ConstantExpression node)
            {
                // Skip all constants - they are either IQueryable roots or values already baked
                // into the plan's Parameters dictionary by AppendConstant during translation.
                return node;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                // Closure-captured variables (member access whose root resolves to a constant)
                // are now emitted as compiled parameters by ExpressionToSqlVisitor.VisitMember.
                // Extract the live value here so it can be bound at execution time.
                if (QueryTranslator.TryGetConstantValue(node, out var value))
                {
                    _values.Add(value ?? DBNull.Value);
                    return node; // early return: do NOT visit children (avoids double-counting)
                }
                return base.VisitMember(node);
            }
        }
    }
}
