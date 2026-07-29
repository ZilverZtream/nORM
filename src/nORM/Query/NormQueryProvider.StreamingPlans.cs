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
    internal sealed partial class NormQueryProvider
    {        public async IAsyncEnumerable<T> AsAsyncEnumerable<T>(Expression expression, [EnumeratorCancellation] CancellationToken ct = default)
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
            // A query that eager-loads a nested collection cannot stream row-by-row: the collection is filled
            // by a dependent fetch that runs only AFTER the full root set is materialized (batched by root
            // key). That covers an Include, a collection projection / split query (plan.DependentQueries — a
            // Select whose body materializes a child collection) and a many-to-many navigation
            // (plan.M2MIncludes). Previously only plan.Includes was guarded, so the other two silently fell
            // through to the row-by-row loop below and yielded roots with EMPTY collections (silent data
            // loss). Fail loud instead — the same reason EF Core cannot stream split queries — so the
            // streaming memory contract is never silently broken.
            if (plan.Includes.Count > 0 || plan.DependentQueries is { Count: > 0 } || plan.M2MIncludes is { Count: > 0 })
                throw new NormUnsupportedFeatureException(
                    "AsAsyncEnumerable cannot stream a query that eager-loads a nested collection - an " +
                    "Include, a collection projection (a Select whose body builds a child collection, e.g. " +
                    "`.Select(a => new { a.Books })`), or a many-to-many navigation. Those collections are " +
                    "populated by a dependent fetch that runs after the root set is fully materialized, which " +
                    "is incompatible with row-by-row streaming. Use `await query.ToListAsync()` to materialize " +
                    "the fully-loaded set in one pass, or drop the eager-loaded collection and reissue the " +
                    "child query per root if streaming is required.",
                    NormUnsupportedReason.AsAsyncEnumerableIncludeUnsupported);
            // OwnsMany owned collections are part of the entity AGGREGATE (not an opt-in Include) and are loaded
            // by an independent SELECT over the full owner set AFTER materialization; the row-by-row loop never
            // runs it, so a streamed owner would silently carry EMPTY owned collections. Unlike an Include —
            // which is explicit and can fan out unboundedly — owned data is implicit and bounded by the result
            // set, so buffer through MaterializeAsync (which loads owned, applies ClientProjection and tracking)
            // to yield the correct fully-populated owners, matching ToListAsync. (OwnsOne inlines as owner
            // columns and streams fine.)
            var hasOwnedCollections = plan.ElementType.IsClass
                && !plan.ElementType.Name.StartsWith("<>", StringComparison.Ordinal)
                && _ctx.IsMapped(plan.ElementType)
                && _ctx.GetMapping(plan.ElementType).OwnedCollections.Count > 0;
            if (plan.PostMaterializeTransform != null || plan.PostReverse || hasOwnedCollections)
            {
                // Client-tail reshapes and tail paging operate on the COMPLETE materialized
                // list — Append attaches after the final row, Reverse and TakeLast need every
                // row — so row-by-row streaming would silently yield the pre-reshape rows.
                // Materialize the reshaped list once (same memory profile as ToListAsync,
                // and MaterializeAsync applies tracking, PostReverse and the transforms)
                // and yield it, preserving correct sequence contents.
                var buffered = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                var bufferedCount = 0;
                foreach (var item in buffered)
                {
                    ct.ThrowIfCancellationRequested();
                    bufferedCount++;
                    yield return (T)item!;
                }
                sw?.Stop();
                _ctx.Options.Logger?.LogQuery(plan.Sql, EnsureParameterDictionary(plan, paramValues), sw?.Elapsed ?? default, bufferedCount);
                yield break;
            }
            if (plan.GroupJoinInfo != null)
            {
                await foreach (var item in _executor.StreamGroupJoinAsync<T>(plan, cmd, ct).ConfigureAwait(false))
                    yield return item;
                yield break;
            }
            // Honour the context tracking DEFAULT, not just an explicit .AsNoTracking(): the buffered/fast
            // read paths gate tracking on `!plan.NoTracking && !(DefaultTrackingBehavior==NoTracking &&
            // !ForceTracking)`, but the streaming path checked only plan.NoTracking. So under a NoTracking
            // context default, AsAsyncEnumerable silently TRACKED every streamed row (unlike ToListAsync) —
            // an edit then persisted on the next SaveChanges, and the ChangeTracker grew O(rows), defeating
            // the bounded-memory purpose of streaming. Fold the same read-only gate in here.
            var isReadOnly = _ctx.Options.DefaultTrackingBehavior == QueryTrackingBehavior.NoTracking && !plan.ForceTracking;
            var trackable = !plan.NoTracking && !isReadOnly &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>", StringComparison.Ordinal) &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null &&
                             _ctx.IsMapped(plan.ElementType) &&
                             !_ctx.GetMapping(plan.ElementType).IsKeyless;   // keyless = query-only, never tracked
            if (trackable)
                _ctx.GetMapping(plan.ElementType);
            var (idMap, idMapping) = QueryExecutor.CreateIdentityResolutionMap(_ctx, plan);
            var count = 0;
            await using var reader = await cmd
                .ExecuteReaderWithInterceptionAsync(
                    _ctx,
                    CommandBehavior.SingleResult,
                    ct)
                .ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var materialized = await plan.Materializer(reader, ct).ConfigureAwait(false);
                // A split (partially client-evaluated) projection materializes the ENTITY server-side, then a
                // client transform produces the result type — every buffered path applies plan.ClientProjection
                // for exactly this. Apply it per row here too; casting the entity straight to T otherwise throws
                // InvalidCastException (or, when T is a supertype like object, silently yields raw entities).
                // The projection result is not a tracked entity, so it bypasses the tracking/identity blocks.
                if (plan.ClientProjection != null)
                {
                    count++;
                    yield return (T)plan.ClientProjection(materialized);
                    continue;
                }
                var entity = (T)materialized;
                if (trackable)
                {
                    var actualMap = _ctx.GetMapping(entity!.GetType());
                    var entry = _ctx.ChangeTracker.Track(entity!, EntityState.Unchanged, actualMap);
                    entity = (T)entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading((object)entity!, _ctx);
                }
                else if (idMap != null)
                {
                    // AsNoTrackingWithIdentityResolution: collapse a repeated root key to one shared instance.
                    entity = (T)QueryExecutor.ResolveRootIdentity(entity!, idMap, idMapping);
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
            // IgnoreQueryFilters() bypasses the user's AddGlobalFilter predicates (soft-delete etc.)
            // for the whole query, but NEVER the tenant boundary — that is a security invariant, not
            // an opt-out filter. A single tree scan decides it for this plan.
            var ignoreUserFilters = ExpressionContainsIgnoreQueryFilters(expression);
            filtered = ApplyGlobalFilters(expression, ignoreUserFilters);
            var elementType = GetElementType(UnwrapQueryExpression(filtered));
            var tenantHash = _ctx.Options.TenantProvider != null
                ? _ctx.GetRequiredTenantId("query plan cache key").GetHashCode()
                : 0;
            // Use cached mapping hash instead of recomputing on every query
            int mappingHash = _ctx.GetMappingHash();

            // Batch all 5 extends into a single hash operation (saves 4 XxHash128 calls)
            // Single tree walk: fingerprint AND collect the closure parameter values at once. The collected
            // values are identical (count + order) to a separate ParameterValueExtractor walk (asserted in
            // Debug at each use), so the cached/hot path avoids the second full traversal.
            var fingerprint = ExpressionFingerprint
                .ComputeForPlanCacheWithValues(filtered, out var collectedParamValues)
                .Extend(tenantHash, elementType.GetHashCode(), filtered.Type.GetHashCode(),
                        _ctx.RawProvider.GetType().GetHashCode(), mappingHash)
                .Extend((int)_ctx.Options.ClientEvaluationPolicy)
                // Injected SUBQUERY filters never appear in the top-level filtered
                // expression: without the filter-set hash, contexts with different
                // global filters share a fingerprint and replay each other's plans.
                .Extend(_ctx.GetGlobalFiltersHash());

            // IgnoreQueryFilters() over a filter defined on a CHILD/related type leaves the ROOT tree byte-for-byte
            // identical to the non-ignore query (the filter only ever reached the secondary loads and correlated
            // subqueries, never the root), so `filtered` — hence the structural fingerprint — is the same for both.
            // Fold the flag in so the ignore plan (subqueries + eager loads run unfiltered) never reuses, or is
            // reused as, the normal plan. Only extend when set so the far-more-common non-ignore path is unchanged.
            if (ignoreUserFilters)
                fingerprint = fingerprint.Extend(unchecked((int)0x1_9F_1C_5D));

            // FromSqlRaw supplies the FROM source as a raw SQL string that the expression-structure
            // fingerprint can't see, so distinct raw SQL (and raw-vs-mapped queries) would otherwise share a
            // plan. Fold its hash into the key so the lookup can never return the wrong plan.
            var rawSource = QueryTranslator.FindRootRawSource(filtered);
            if (rawSource != null)
                fingerprint = fingerprint.Extend(StringComparer.Ordinal.GetHashCode(rawSource.RawSql));

            if (_planCache.TryGet(fingerprint, out var cached)
                && !HasClosureFoldedIntoSql(cached))
            {
                parameterValues = ResolveParameterValues(collectedParamValues, cached, filtered);
                return RebindGroupJoinClosures(cached, filtered);
            }

            // ExceptBy / IntersectBy / UnionBy and local-sequence SequenceEqual capture
            // an in-memory IEnumerable from the user's closure. The plan cache
            // keys by expression fingerprint, which doesn't differentiate captured
            // collection identity / contents; reusing the cached plan would replay
            // the prior call's collection. Bypass the cache for these methods so each
            // invocation translates fresh against the live closure values. Run this
            // detector only after a cache miss so normal hot cached queries do not
            // pay a full tree walk on every execution.
            // Raw-SQL queries bake their POSITIONAL parameter values into the plan at translation, so caching
            // would replay stale values on a same-SQL-different-parameters call — bypass those. But a raw
            // query with NO positional parameters has nothing to replay: its SQL is already folded into the
            // fingerprint (above) so distinct SQL never collides, and any composed closures re-bind through
            // the normal compiled-parameter path. Caching those avoids re-translating on every execution
            // (a parameterless FromSqlRaw was allocating ~2x a plain query for exactly this reason).
            bool bypassPlanCache = ExpressionContainsLocalSequenceOp(filtered)
                || (rawSource != null && rawSource.RawParameters.Length > 0);

            var localFiltered = filtered;
            QueryPlan plan;
            if (bypassPlanCache)
            {
                using var freshTranslator = new QueryTranslator(_ctx, ignoreUserFilters);
                var p = freshTranslator.Translate(localFiltered);
                plan = p with
                {
                    Fingerprint = fingerprint,
                    Parameters = new Dictionary<string, object>(p.Parameters),
                    CompiledParameters = new List<string>(p.CompiledParameters),
                    IgnoreUserFilters = ignoreUserFilters
                };
                parameterValues = ResolveParameterValues(collectedParamValues, plan, filtered);
                return plan;
            }
            {
                using var translator = new QueryTranslator(_ctx, ignoreUserFilters);
                var before = GC.GetAllocatedBytesForCurrentThread();
                var p = translator.Translate(localFiltered);
                var after = GC.GetAllocatedBytesForCurrentThread();
                var size = after - before;
                Interlocked.Add(ref _totalPlanSize, size);
                Interlocked.Increment(ref _planSizeSamples);
                var clonedParams = new Dictionary<string, object>(p.Parameters);
                var clonedCompiledParams = new List<string>(p.CompiledParameters);
                plan = p with
                {
                    Fingerprint = fingerprint,
                    Parameters = clonedParams,
                    CompiledParameters = clonedCompiledParams,
                    IgnoreUserFilters = ignoreUserFilters
                };
            }

            // Translators that FOLD a closure-captured value into the SQL itself
            // (StringComparison args, TimeOnly.Add deltas, ToString format strings, ...)
            // mark the fold with a reserved *_unused compiled-param placeholder. Such
            // SQL is execution-specific: caching it would replay the first execution's
            // folded values on every later call with different captures, so these
            // plans translate fresh per execution and are never stored.
            if (HasClosureFoldedIntoSql(plan))
            {
                parameterValues = ResolveParameterValues(collectedParamValues, plan, filtered);
                return plan;
            }

            plan = _planCache.GetOrAdd(fingerprint, _ => plan);

            parameterValues = ResolveParameterValues(collectedParamValues, plan, filtered);
            return RebindGroupJoinClosures(plan, filtered);
        }

        /// <summary>
        /// True when the plan's SQL embeds a closure-captured value (marked by the
        /// reserved *_unused compiled-param placeholders every folding translator
        /// creates for extractor alignment), making the SQL execution-specific.
        /// </summary>
        private static bool HasClosureFoldedIntoSql(QueryPlan plan)
        {
            if (plan.ClosureFoldedIntoSql)
                return true;
            for (var i = 0; i < plan.CompiledParameters.Count; i++)
            {
                // `_ctx_unused` placeholders are pure extractor alignment for
                // structurally-consumed query roots — nothing execution-specific
                // is folded, so they must not disable caching.
                var name = plan.CompiledParameters[i];
                if (IsUnusedCompiledParameter(name)
                    && !name.EndsWith("_ctx_unused", StringComparison.Ordinal))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// A GroupJoin result selector executes client-side as a compiled delegate
        /// cached with the plan; closures captured inside it (e.g. the bound of a
        /// filtered per-group aggregate) would replay the FIRST execution's values
        /// on every cache hit. When the selector was closure-lifted at translation,
        /// re-collect the current expression's closure values and bind them —
        /// returning a per-execution plan copy so the cached plan stays untouched.
        /// </summary>
        private static QueryPlan RebindGroupJoinClosures(QueryPlan plan, Expression current)
        {
            var info = plan.GroupJoinInfo;
            if (info == null
                || (info.ClosureLiftedResultSelector == null && info.ClosureLiftedOuterKeySelector == null))
                return plan;

            var finder = new GroupJoinFinder();
            finder.Visit(current);
            if (finder.Found is not { Arguments.Count: 5 } node)
                return plan;

            var updated = info;

            if (info.ClosureLiftedResultSelector != null && info.ClosureSlotCount > 0
                && ComposeGroupJoinConsumer(node, node.Arguments[4]) is { } selector)
            {
                var values = new List<object?>(info.ClosureSlotCount);
                GroupJoinClosureValues.Collect(selector.Body, values);
                if (values.Count == info.ClosureSlotCount) // structural mismatch keeps the translation-time binding
                {
                    var lifted = info.ClosureLiftedResultSelector;
                    var bound = values.ToArray();
                    updated = updated with { ResultSelector = (o, cs) => lifted(o, cs, bound) };
                }
            }

            // The outer KEY selector drives Distinct de-dup and key-based segmentation;
            // its captures replay from the cache just like the result selector's.
            if (info.ClosureLiftedOuterKeySelector != null && info.OuterKeyClosureSlotCount > 0
                && ComposeGroupJoinConsumer(node, node.Arguments[2]) is { } keySelector)
            {
                var values = new List<object?>(info.OuterKeyClosureSlotCount);
                GroupJoinClosureValues.Collect(keySelector.Body, values);
                if (values.Count == info.OuterKeyClosureSlotCount)
                {
                    var liftedKey = info.ClosureLiftedOuterKeySelector;
                    var bound = values.ToArray();
                    updated = updated with { OuterKeySelector = o => liftedKey(o, bound) };
                }
            }

            return ReferenceEquals(updated, info) ? plan : plan with { GroupJoinInfo = updated };
        }

        /// <summary>
        /// Unquotes a GroupJoin lambda argument and mirrors translation's
        /// Select-projected outer handling: the projection is composed INTO the
        /// consumer (key or result selector) there, moving the projection's closure
        /// captures into the lifted body. Closure values must be collected from the
        /// SAME composed shape or the slot ordering (and count) diverges and cached
        /// plans replay the first execution's projection closures.
        /// </summary>
        private static LambdaExpression? ComposeGroupJoinConsumer(MethodCallExpression node, Expression arg)
        {
            while (arg is UnaryExpression { NodeType: ExpressionType.Quote } quote)
                arg = quote.Operand;
            if (arg is not LambdaExpression consumer)
                return null;

            var outer = node.Arguments[0];
            while (outer is MethodCallExpression { Arguments.Count: >= 1 } outerCall
                   && outerCall.Method.Name != nameof(Queryable.Select))
                outer = outerCall.Arguments[0];
            if (outer is MethodCallExpression { Arguments.Count: 2 } selectCall
                && selectCall.Method.Name == nameof(Queryable.Select))
            {
                var projArg = selectCall.Arguments[1];
                while (projArg is UnaryExpression { NodeType: ExpressionType.Quote } projQuote)
                    projArg = projQuote.Operand;
                if (projArg is LambdaExpression { Parameters.Count: 1 } projection
                    && projection.Body.Type == consumer.Parameters[0].Type
                    && projection.Body.Type != projection.Parameters[0].Type)
                    return QueryTranslator.ComposeThroughOuterProjection(projection, consumer);
            }
            return consumer;
        }

        private sealed class GroupJoinFinder : ExpressionVisitor
        {
            public MethodCallExpression? Found { get; private set; }

            public override Expression? Visit(Expression? node)
            {
                if (Found != null)
                    return node;
                if (node is MethodCallExpression { Arguments.Count: 5 } call
                    && call.Method.Name == nameof(Queryable.GroupJoin))
                {
                    Found = call;
                    return node;
                }
                return base.Visit(node);
            }
        }

        /// <summary>
        /// Walks the expression tree looking for methods whose captured local sequence
        /// contents are embedded into generated SQL. Plans containing these calls bypass
        /// the fingerprint-keyed plan cache to avoid replaying a prior call's collection.
        /// </summary>
        private static bool ExpressionContainsLocalSequenceOp(Expression expression)
        {
            return LocalSequenceOpDetector.Has(expression);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class LocalSequenceOpDetector : ExpressionVisitor
        {
            private bool _found;
            public static bool Has(Expression e)
            {
                var d = new LocalSequenceOpDetector();
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
                        || node.Method.Name == "UnionBy"
                        || node.Method.Name == "SequenceEqual"
                        // These bake captured runtime values (an element, a local second
                        // sequence, a default value, or a captured chunk size) into the
                        // plan's post-materialization transform, which the fingerprint
                        // cannot differentiate.
                        || node.Method.Name == "Append"
                        || node.Method.Name == "Prepend"
                        || node.Method.Name == "Zip"
                        || node.Method.Name == "Chunk"
                        // Only the value-carrying overload bakes a captured default;
                        // the parameterless form is the hot left-join building block
                        // and stays cacheable.
                        || (node.Method.Name == "DefaultIfEmpty" && node.Arguments.Count == 2)))
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

        /// <summary>
        /// Returns the closure parameter values collected during the single fingerprint walk
        /// (<see cref="ExpressionFingerprint.ComputeForPlanCacheWithValues"/>) instead of re-walking the tree.
        /// Semantically identical to <see cref="ExtractParameterValues"/>: null when the plan has no compiled
        /// params, otherwise the full document-ordered value list (alignment to compiled-param slots happens
        /// later in <see cref="EnsureParameterDictionary"/>). Opt-in verification: when the environment variable
        /// <c>NORM_VERIFY_PARAM_COLLECTION=1</c> is set in a Debug build it re-runs the old extractor and asserts
        /// the two agree, so a regression surfaces in tests. It is OFF by default because re-running the extractor
        /// on every query doubles the tree-walk work — the very cost this consolidation removes — which is
        /// pointless in the steady state and destabilizes heavy fuzz sweeps.
        /// </summary>
        private IReadOnlyList<object?>? ResolveParameterValues(List<object?>? collected, QueryPlan plan, Expression filtered)
        {
            if (plan.CompiledParameters.Count == 0)
                return null;
            IReadOnlyList<object?> values = collected ?? (IReadOnlyList<object?>)Array.Empty<object?>();
#if DEBUG
            if (_verifyParamCollection)
            {
                var oracle = ExtractParameterValues(filtered, plan);
                Debug.Assert(ParameterValuesMatch(values, oracle),
                    "Single-pass fingerprint value collection diverged from ParameterValueExtractor: the plan-cache " +
                    "would bind wrong parameter values. Fix ExpressionFingerprint's VisitMember collection to mirror " +
                    "ParameterValueExtractor exactly (same TryGetConstantValue, same early-return/suppress semantics).");
            }
#endif
            return values;
        }

#if DEBUG
        // Opt-in one-way switch (read once) that turns the ResolveParameterValues oracle back on for a Debug run.
        private static readonly bool _verifyParamCollection =
            string.Equals(Environment.GetEnvironmentVariable("NORM_VERIFY_PARAM_COLLECTION"), "1", StringComparison.Ordinal);
#endif

#if DEBUG
        // Verifies the single-pass collection matches the extractor on the alignment-critical properties:
        // COUNT (the documented param-count-drift bug class) + per-position runtime TYPE (catches collecting a
        // different/mis-ordered node). Exact value equality is intentionally NOT required: a non-deterministic
        // closure such as DateTime.UtcNow / Random evaluates to a slightly different value on the two walks, yet
        // both are a single correct query-time snapshot. Actual bound-value correctness is covered by the
        // LINQ-parity fuzzer (results vs a LINQ-to-Objects oracle).
        private static bool ParameterValuesMatch(IReadOnlyList<object?> a, IReadOnlyList<object?>? b)
        {
            var bCount = b?.Count ?? 0;
            if (a.Count != bCount)
                return false;
            for (int i = 0; i < a.Count; i++)
                if (a[i]?.GetType() != b![i]?.GetType())
                    return false;
            return true;
        }
#endif

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
                var converters = plan.ParameterConverters;
                var ordinals = plan.CompiledParameterOrdinals;
                // Slots with a recorded document ordinal bind values[ordinal] directly;
                // the rest consume the unclaimed ordinals in ascending order — the
                // legacy positional stream. Projection-rendered slots register at
                // Build time (after every clause slot), so registration order alone
                // can diverge from the extractor's document order.
                HashSet<int>? claimed = null;
                if (ordinals != null && ordinals.Count > 0)
                    claimed = new HashSet<int>(ordinals.Values);
                var stream = 0;
                for (int i = 0; i < plan.CompiledParameters.Count; i++)
                {
                    var name = plan.CompiledParameters[i];
                    int valueIndex;
                    if (ordinals != null && ordinals.TryGetValue(name, out var ordinal))
                    {
                        valueIndex = ordinal;
                    }
                    else
                    {
                        while (claimed != null && stream < parameterValues.Count && claimed.Contains(stream))
                            stream++;
                        valueIndex = stream++;
                    }
                    if (valueIndex >= parameterValues.Count)
                        continue;
                    var value = parameterValues[valueIndex];
                    // A closure value compared against a value-converter column binds its provider
                    // representation, matching the inline-constant and fast-path behavior.
                    if (value != null && converters != null && converters.TryGetValue(name, out var converter))
                        value = converter.ConvertToProvider(value);
                    parameters[name] = value ?? DBNull.Value;
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
        /// <summary>
        /// True when the outer query operator chain contains an <c>IgnoreQueryFilters()</c> marker,
        /// so the query bypasses user global filters (EF semantics). Walks only the source spine —
        /// the instance-method receiver or the first argument, unwrapping the interface Convert the
        /// instance operators emit — rather than a full expression visit, which can recurse into the
        /// self-referential Constant(queryable) roots nORM builds and overflow the stack. Matched by
        /// declaring type and name so an unrelated method named IgnoreQueryFilters cannot trigger it.
        /// </summary>
        private static bool ExpressionContainsIgnoreQueryFilters(Expression? expression)
        {
            var current = expression;
            while (current != null)
            {
                if (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                {
                    current = convert.Operand;
                    continue;
                }
                if (current is not MethodCallExpression call)
                    return false;
                if (call.Method.DeclaringType == typeof(nORM.Core.NormIncludableQueryableExtensions)
                    && call.Method.Name == nameof(nORM.Core.NormIncludableQueryableExtensions.IgnoreQueryFilters))
                    return true;
                // Descend the source spine: instance receiver (Object) else the first argument.
                current = call.Object ?? (call.Arguments.Count > 0 ? call.Arguments[0] : null);
            }
            return false;
        }

        private Expression ApplyGlobalFilters(Expression expression, bool ignoreUserFilters)
        {
            // Skip the entire recursive walk when no global filters or tenant provider exist.
            // The recursion allocates new expression nodes (ToArray + Update) on every node even
            // when there are no filters to apply. (The IgnoreQueryFilters marker is left in the
            // tree here and stripped by its pass-through translator.)
            if (_ctx.Options.GlobalFilters.Count == 0 && _ctx.Options.TenantProvider == null)
                return expression;

            if (expression is MethodCallExpression mc &&
                !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                mc.Arguments.Count > 0)
            {
                var filteredSource = ApplyGlobalFilters(mc.Arguments[0], ignoreUserFilters);
                var args = mc.Arguments.ToArray();
                args[0] = filteredSource;
                return mc.Update(mc.Object, args);
            }

            if (expression is MethodCallExpression queryCall &&
                queryCall.Method.DeclaringType == typeof(Queryable) &&
                queryCall.Arguments.Count > 0 &&
                typeof(IQueryable).IsAssignableFrom(queryCall.Arguments[0].Type))
            {
                var resultElementType = GetElementType(expression);
                var sourceElementType = GetElementType(queryCall.Arguments[0]);

                // Keyed set operators (DistinctBy / UnionBy / ExceptBy / IntersectBy) collapse the source to one
                // row per key. A global/tenant filter must apply to the SOURCE, before the dedup: the fall-through
                // base case wraps the whole call in an outer Where, which over the pre-built ROW_NUMBER dedup
                // subquery is untranslatable (a raw "near WHERE" syntax error) AND semantically wrong — a key
                // whose surviving row is filtered out would vanish even though a visible sibling remains. Filter
                // every entity-sequence argument (a scalar key sequence has no mapped filter, so it is a no-op)
                // and return, so the predicate lands inside the dedup rather than after it.
                if (queryCall.Method.Name is "DistinctBy" or "UnionBy" or "ExceptBy" or "IntersectBy")
                {
                    var keyedArgs = queryCall.Arguments.ToArray();
                    var keyedChanged = false;
                    for (int i = 0; i < keyedArgs.Length; i++)
                    {
                        if (!typeof(IQueryable).IsAssignableFrom(keyedArgs[i].Type)) continue;
                        var filteredArg = ApplyGlobalFilters(keyedArgs[i], ignoreUserFilters);
                        if (!ReferenceEquals(filteredArg, keyedArgs[i]))
                        {
                            keyedArgs[i] = filteredArg;
                            keyedChanged = true;
                        }
                    }
                    return keyedChanged ? queryCall.Update(queryCall.Object, keyedArgs) : expression;
                }

                // Recurse into EVERY IQueryable-typed argument, not just the source.
                // Join/GroupJoin carry the inner (joined-to) sequence at Arguments[1];
                // without this, global filters (soft-delete, tenant) on the inner entity
                // are silently dropped and the inner table leaks unfiltered rows into the
                // join result. The source (index 0) is only recursed when the call is a
                // projection/transformation (preserving the original element-type guard);
                // an entity-root source falls through to the base case below so its own
                // filter is applied there exactly once.
                Expression[]? rewritten = null;
                for (int i = 0; i < queryCall.Arguments.Count; i++)
                {
                    var arg = queryCall.Arguments[i];
                    bool recurse = i == 0
                        ? (resultElementType != sourceElementType || !_ctx.IsMapped(resultElementType))
                        : typeof(IQueryable).IsAssignableFrom(arg.Type);
                    if (!recurse)
                        continue;
                    var filtered = ApplyGlobalFilters(arg, ignoreUserFilters);
                    if (!ReferenceEquals(filtered, arg))
                    {
                        rewritten ??= queryCall.Arguments.ToArray();
                        rewritten[i] = filtered;
                    }
                }
                if (rewritten != null)
                {
                    var updated = queryCall.Update(queryCall.Object, rewritten);
                    // For an unmapped result (a join/groupjoin/selectmany projection into an anonymous
                    // type) the base case cannot express a filter over that shape, so return as-is.
                    // For a MAPPED result element type — set operations (Union/Intersect/Except) whose
                    // result IS the entity type, and OfType<Derived>() — fall through so the base case
                    // also wraps the combined expression. Without this, the left/source arm (which is
                    // deliberately not recursed for same-type operators) keeps NO global filter and
                    // leaks unfiltered rows: soft-deleted rows, or — with a tenant provider — another
                    // tenant's rows.
                    if (!_ctx.IsMapped(resultElementType))
                        return updated;
                    expression = updated;
                }
            }

            // Window functions (WithRowNumber/WithRank/WithDenseRank/WithLag/WithLead) project the ranked
            // rows into an unmapped result type, so neither branch above descends into them and the base case
            // cannot express a filter over the anonymous shape — the window source table would be ranked with
            // NO global/tenant predicate, leaking soft-deleted (or another tenant's) rows INTO the window and
            // corrupting every rank. Recurse into the window SOURCE (argument 0 is the mapped entity query) so
            // the filter lands before the window, then return: the result shape is unmapped, like a projection.
            if (expression is MethodCallExpression windowCall &&
                windowCall.Method.DeclaringType == typeof(WindowFunctionsExtensions) &&
                windowCall.Arguments.Count > 0 &&
                typeof(IQueryable).IsAssignableFrom(windowCall.Arguments[0].Type))
            {
                var filteredSource = ApplyGlobalFilters(windowCall.Arguments[0], ignoreUserFilters);
                if (ReferenceEquals(filteredSource, windowCall.Arguments[0]))
                    return expression;
                var windowArgs = windowCall.Arguments.ToArray();
                windowArgs[0] = filteredSource;
                return windowCall.Update(windowCall.Object, windowArgs);
            }

            // Take/Skip/TakeLast/SkipLast do NOT commute with a post-filter: an entity-typed row-limit reaches
            // the base case (result type == source type, mapped, so neither branch above descended into it),
            // which appends an OUTER Where AFTER the LIMIT/OFFSET. The window then runs over the UNFILTERED rows
            // and the filter trims the window — silently dropping the caller's OWN rows (Take) or shifting the
            // page (Skip); no foreign-tenant rows leak, but the caller loses its own data in pagination.
            // Filtering DOES commute with OrderBy, so push the filter into the SOURCE, before the window —
            // turning Take(OrderBy(root)) into Take(Where(OrderBy(root), filter)): order, filter, then limit.
            if (expression is MethodCallExpression pagingCall &&
                pagingCall.Method.DeclaringType == typeof(Queryable) &&
                pagingCall.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip)
                    or nameof(Queryable.TakeLast) or nameof(Queryable.SkipLast) &&
                pagingCall.Arguments.Count > 0 &&
                typeof(IQueryable).IsAssignableFrom(pagingCall.Arguments[0].Type))
            {
                var filteredSource = ApplyGlobalFilters(pagingCall.Arguments[0], ignoreUserFilters);
                if (ReferenceEquals(filteredSource, pagingCall.Arguments[0]))
                    return expression;
                var pagingArgs = pagingCall.Arguments.ToArray();
                pagingArgs[0] = filteredSource;
                return pagingCall.Update(pagingCall.Object, pagingArgs);
            }

            // A filter-COMMUTING operator (Where / OrderBy / ThenBy / Distinct) whose result IS the mapped
            // entity type reaches the base case, which wraps the filter as an OUTER Where. That is correct only
            // when nothing between here and the root is a NON-commuting window/dedup (Take/Skip/TakeLast/
            // SkipLast/DistinctBy/UnionBy/ExceptBy/IntersectBy). When one is buried below a trailing Where/OrderBy
            // — e.g. `Take(2).Where(...)` or `DistinctBy(k).OrderBy(...)` — the arg[0] recursion above did NOT
            // descend (same mapped element type), so the base case would apply the filter AFTER the window,
            // silently dropping the caller's OWN rows (soft-delete filter → empty page; tenant filter → own-tenant
            // rows lost) or shifting the page. Push the filter into the SOURCE instead (the recursion then reaches
            // the paging / keyed branch, which lands the predicate before the window) and return without wrapping.
            if (expression is MethodCallExpression commutingCall &&
                commutingCall.Method.DeclaringType == typeof(Queryable) &&
                commutingCall.Method.Name is nameof(Queryable.Where) or nameof(Queryable.OrderBy)
                    or nameof(Queryable.OrderByDescending) or nameof(Queryable.ThenBy)
                    or nameof(Queryable.ThenByDescending) or nameof(Queryable.Distinct) &&
                commutingCall.Arguments.Count > 0 &&
                typeof(IQueryable).IsAssignableFrom(commutingCall.Arguments[0].Type) &&
                GetElementType(commutingCall) == GetElementType(commutingCall.Arguments[0]) &&
                _ctx.IsMapped(GetElementType(commutingCall)) &&
                SourceSpineHasNonCommutingWindow(commutingCall.Arguments[0]))
            {
                var filteredSource = ApplyGlobalFilters(commutingCall.Arguments[0], ignoreUserFilters);
                if (ReferenceEquals(filteredSource, commutingCall.Arguments[0]))
                    return expression;
                var commutingArgs = commutingCall.Arguments.ToArray();
                commutingArgs[0] = filteredSource;
                return commutingCall.Update(commutingCall.Object, commutingArgs);
            }

            var entityType = GetElementType(expression);
            // User global filters (soft-delete etc.) are skipped when IgnoreQueryFilters() is in the
            // query. The tenant predicate below is NEVER skipped.
            if (!ignoreUserFilters && _ctx.Options.GlobalFilters.Count > 0)
            {
                var combined = CombineGlobalFilterPredicates(entityType);
                if (combined != null)
                {
                    expression = Expression.Call(
                        typeof(Queryable),
                        nameof(Queryable.Where),
                        new[] { entityType },
                        expression,
                        Expression.Quote(combined));
                }
            }
            if (_ctx.Options.TenantProvider != null)
            {
                var map = _ctx.GetMapping(entityType);
                var tenantCol = _ctx.RequireTenantColumn(map, "query");
                var param = Expression.Parameter(entityType, "t");
                var prop = Expression.Property(param, tenantCol.Prop.Name);
                var tenantId = _ctx.GetRequiredTenantId(map, "query");
                var constant = Expression.Constant(tenantId, tenantCol.Prop.PropertyType);
                var body = Expression.Equal(prop, constant);
                var lambda = Expression.Lambda(body, param);
                expression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.Where),
                    new[] { entityType },
                    expression,
                    Expression.Quote(lambda));
            }
            return expression;
        }

        /// <summary>
        /// Walks the single-source spine below <paramref name="source"/> through filter-COMMUTING operators
        /// (Where / OrderBy / ThenBy / Distinct) and returns true if it reaches a NON-commuting window/dedup
        /// operator (Take / Skip / TakeLast / SkipLast / DistinctBy / UnionBy / ExceptBy / IntersectBy) before the
        /// root — meaning a global/tenant filter wrapped at the outer level would apply AFTER that window and lose
        /// rows. Stops (returns false) at any shape-changing or multi-source operator (Select / Join / Union / …)
        /// so the filter is never pushed through a boundary it does not commute across.
        /// </summary>
        private static bool SourceSpineHasNonCommutingWindow(Expression source)
        {
            var e = source;
            while (e is MethodCallExpression mc
                && mc.Method.DeclaringType == typeof(Queryable)
                && mc.Arguments.Count > 0
                && typeof(IQueryable).IsAssignableFrom(mc.Arguments[0].Type))
            {
                switch (mc.Method.Name)
                {
                    case nameof(Queryable.Take):
                    case nameof(Queryable.Skip):
                    case nameof(Queryable.TakeLast):
                    case nameof(Queryable.SkipLast):
                    case "DistinctBy":
                    case "UnionBy":
                    case "ExceptBy":
                    case "IntersectBy":
                        return true;
                    case nameof(Queryable.Where):
                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                    case nameof(Queryable.ThenBy):
                    case nameof(Queryable.ThenByDescending):
                    case nameof(Queryable.Distinct):
                        e = mc.Arguments[0];
                        continue;
                    default:
                        return false;
                }
            }
            return false;
        }

        /// <summary>
        /// Rebinds every global filter applicable to <paramref name="entityType"/> onto one
        /// shared parameter and ANDs the bodies as a balanced tree inside a single predicate.
        /// One nested Where call per filter would make translation recursion depth linear in
        /// the number of registered filters, which overflows the thread stack once dynamic
        /// registration reaches the hundreds; a balanced tree keeps the depth logarithmic.
        /// </summary>
        private LambdaExpression? CombineGlobalFilterPredicates(Type entityType)
        {
            List<Expression>? bodies = null;
            ParameterExpression? param = null;
            foreach (var kvp in _ctx.Options.GlobalFilters)
            {
                if (!kvp.Key.IsAssignableFrom(entityType)) continue;
                foreach (var filter in kvp.Value)
                {
                    param ??= Expression.Parameter(entityType, "gf");
                    Expression body;
                    if (filter.Parameters.Count == 2)
                    {
                        body = new ParameterReplacer(filter.Parameters[0], Expression.Constant(_ctx)).Visit(filter.Body)!;
                        body = new ParameterReplacer(filter.Parameters[1], param).Visit(body)!;
                    }
                    else
                    {
                        body = new ParameterReplacer(filter.Parameters[0], param).Visit(filter.Body)!;
                    }
                    (bodies ??= new List<Expression>()).Add(body);
                }
            }
            if (bodies == null)
                return null;
            return Expression.Lambda(BuildBalancedAnd(bodies, 0, bodies.Count), param!);
        }

        private static Expression BuildBalancedAnd(List<Expression> bodies, int start, int count)
        {
            if (count == 1)
                return bodies[start];
            var half = count / 2;
            return Expression.AndAlso(
                BuildBalancedAnd(bodies, start, half),
                BuildBalancedAnd(bodies, start + half, count - half));
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
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
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
