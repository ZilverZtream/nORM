using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Configuration;
using nORM.Core;

#nullable enable

namespace nORM.Query
{
    /// <summary>
    /// Holds query complexity metrics computed during expression tree translation.
    /// These are gathered once during the LINQ-to-SQL translation pass and stored on
    /// the <see cref="QueryPlan"/> so that timeout heuristics never need to re-scan the
    /// generated SQL string.
    /// </summary>
    internal struct QueryComplexityMetrics
    {
        /// <summary>Number of JOIN / SelectMany / GroupJoin operations.</summary>
        public int JoinCount;
        /// <summary>True when a GROUP BY clause was produced.</summary>
        public bool HasGroupBy;
        /// <summary>True when an ORDER BY clause was produced.</summary>
        public bool HasOrderBy;
        /// <summary>Depth of nested subqueries (incremented per TranslateSubExpression call).</summary>
        public int SubqueryDepth;
        /// <summary>Number of WHERE predicates (each Where call = +1).</summary>
        public int PredicateCount;
        /// <summary>True when a DISTINCT was applied.</summary>
        public bool HasDistinct;
        /// <summary>True when an aggregate (Sum/Count/Min/Max/Average) was used.</summary>
        public bool HasAggregates;

        private const int BaseScore = 1;
        private const int JoinWeight = 2;
        private const int GroupByWeight = 2;
        private const int OrderByWeight = 2;
        private const int SubqueryWeight = 2;
        private const int DistinctWeight = 1;
        private const int AggregateWeight = 1;
        private const int PredicateDivisor = 5;
        private const int MaxComplexityScore = 50;

        /// <summary>
        /// Derives a scalar complexity score for use with
        /// <see cref="nORM.Execution.AdaptiveTimeoutManager"/>.
        /// The formula mirrors the SQL-string heuristic in DbContext.GetAdaptiveTimeout
        /// but is computed from reliable tree-walk data instead of substring scanning.
        /// </summary>
        public int ToComplexityScore()
        {
            int score = BaseScore;
            score += JoinCount * JoinWeight;
            if (HasGroupBy) score += GroupByWeight;
            if (HasOrderBy) score += OrderByWeight;
            score += SubqueryDepth * SubqueryWeight;
            if (HasDistinct) score += DistinctWeight;
            if (HasAggregates) score += AggregateWeight;
            // Each predicate contributes a tiny amount
            score += PredicateCount / PredicateDivisor;
            return Math.Min(score, MaxComplexityScore);
        }
    }

    internal sealed record QueryPlan(
        string Sql,
        IReadOnlyDictionary<string, object> Parameters,
        IReadOnlyList<string> CompiledParameters,
        Func<DbDataReader, CancellationToken, Task<object>> Materializer,
        Func<DbDataReader, object> SyncMaterializer,
        Type ElementType,
        bool IsScalar,
        bool SingleResult,
        bool NoTracking,
        string MethodName,
        List<IncludePlan> Includes,
        GroupJoinInfo? GroupJoinInfo,
        IReadOnlyCollection<string> Tables,
        bool SplitQuery,
        TimeSpan CommandTimeout,
        bool IsCacheable,
        TimeSpan? CacheExpiration,
        ExpressionFingerprint Fingerprint = default,
        int? Take = null,
        List<DependentQueryDefinition>? DependentQueries = null,
        Func<object, object>? ClientProjection = null,
        QueryComplexityMetrics Complexity = default,
        List<M2MIncludePlan>? M2MIncludes = null,
        BulkCudQueryShape? BulkCudShape = null,
        // TakeLast/SkipLast translators flip the ORDER BY direction and apply Take/Skip
        // to the reversed sequence; setting this flag tells the materializer to reverse
        // the result list once read so the caller sees rows in the original ORDER BY
        // direction. The DB still scans only N rows (TakeLast) or n - N (SkipLast) so
        // there's no full-table-scan penalty.
        bool PostReverse = false,
        // Post-materialization transforms are reserved for operators whose v1 contract
        // is explicitly in-memory after a bounded/generated SQL source, such as
        // standalone DefaultIfEmpty. DistinctBy and the keyed set operators are
        // server-translated with ROW_NUMBER() and do not use this hook.
        System.Func<DbContext, System.Collections.IList, System.Collections.IList>? PostMaterializeTransform = null,
        // Client-side scalar aggregates (e.g. Count over a client-tail reshaped
        // sequence) materialize rows, reduce them via PostMaterializeTransform to a
        // single boxed value, and the executor unwraps that value as the result.
        bool ClientScalar = false,
        // Value converters keyed by compiled-parameter name, for closure values compared against a
        // value-converter column. Applied to the extractor-supplied value before binding so the
        // predicate compares against the provider representation. Null/empty for almost all queries.
        IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? ParameterConverters = null,
        // True when a translator baked closure-captured VALUES into the SQL text without
        // registering compiled parameters (e.g. correlated-subquery fragments re-translated
        // against a sub-alias, where a second registration would shift positional bindings).
        // Such SQL is execution-specific: the plan translates fresh per execution and skips
        // both the plan cache and the pooled prepared-command cache.
        bool ClosureFoldedIntoSql = false,
        // Compiled-parameter name → document ordinal of the closure occurrence it binds.
        // The value extractor produces one value per closure occurrence in document order;
        // slots recorded here bind values[ordinal] directly, and the remaining slots consume
        // the unclaimed ordinals in ascending order (the legacy positional stream). Needed
        // because projection-rendered slots register at Build time — after clause slots —
        // so registration order alone can diverge from document order.
        IReadOnlyDictionary<string, int>? CompiledParameterOrdinals = null,
        // Every table the query READS, for result-cache tagging so a write to any of them
        // invalidates the entry — this is a SUPERSET of <see cref="Tables"/>, which stays the
        // semantic table set (root + joins) that ExecuteUpdate/ExecuteDelete use to choose their
        // UPDATE/DELETE form. Correlated-subquery and navigation-aggregate tables belong here but
        // NOT in Tables. Null when it would equal Tables (no subquery-only tables).
        IReadOnlyCollection<string>? CacheTables = null,
        // The temporal AsOf timestamp the root query reconstructs at, when present. Eager
        // loads must read related rows through the SAME history window or the result mixes
        // eras (historical roots joined to live relations). Safe to bake per plan: AsOf()
        // embeds the timestamp as an expression CONSTANT, so the plan-cache fingerprint
        // hashes the full value and every distinct timestamp gets its own plan.
        DateTime? AsOfTimestamp = null,
        // AsTracking() sets this to force change-tracking ON for THIS query even when the context's
        // DefaultTrackingBehavior is NoTracking — the per-query override of a no-tracking default.
        // It overrides only the context-default (IsReadOnlyQuery) gate; AsNoTracking/AsOf still win
        // because they set NoTracking=true, which makes the entity untrackable before this is consulted.
        bool ForceTracking = false,
        // AsNoTrackingWithIdentityResolution() sets this: the query is untracked (NoTracking=true) but the
        // complex materialize loop still resolves identity — a root key seen more than once in one result set
        // (Concat/UNION ALL, or a self-join/SelectMany/GroupJoin flatten projecting the root) materializes to a
        // single shared instance, matching what the change tracker would do for a tracked query. Inert on the
        // fast/simple paths, which cannot produce a duplicate root key.
        bool IdentityResolution = false,
        // The value converter for a scalar Min/Max over a value-converter column. Min/Max return an actual
        // stored column value, so ConvertFromProvider must run on the raw scalar to yield the MODEL value
        // (Max(o => o.Score) over a +1000 converter returns 9, not 1009). Null for every other query:
        // Sum/Average aggregate across rows and do not correspond to a single stored value.
        nORM.Mapping.IValueConverter? ScalarResultConverter = null
    );

    internal sealed record BulkCudQueryShape(
        string WhereClause,
        bool HasGroupBy,
        bool HasOrderBy,
        bool HasHaving,
        bool HasJoins,
        bool HasDistinct,
        bool HasPaging
    );

    /// <summary>A filtered-Include predicate (from <c>Include(o =&gt; o.Lines.Where(pred))</c>), rendered to
    /// SQL against the level's alias at plan-build time, with the compiled-parameter names it references
    /// (closure captures) for per-execution rebinding.</summary>
    internal sealed record IncludeFilter(string Sql, IReadOnlyList<string> Parameters);

    internal sealed record IncludePlan(List<TableMapping.Relation> Path)
    {
        /// <summary>Per-level filter predicates, parallel to <see cref="Path"/> (null where a level has no
        /// filter). Grows alongside Path as ThenInclude extends the chain.</summary>
        public List<IncludeFilter?> Filters { get; } = new();
    }

    /// <summary>Eager-load plan for a many-to-many navigation property.</summary>
    internal sealed record M2MIncludePlan(JoinTableMapping JoinTable);

    internal sealed record GroupJoinInfo(
        Type OuterType,
        Type InnerType,
        Type ResultType,
        Func<object, object?> OuterKeySelector,
        Column InnerKeyColumn,
        Func<object, IEnumerable<object>, object> ResultSelector,
        bool OuterIsEntity = true,
        int OuterColumnCount = -1,
        // Per-OUTER-ROW segmentation identity (the outer's primary key). GroupJoin yields
        // one result per outer ELEMENT, so segmenting the flattened LEFT JOIN stream by the
        // join KEY would fuse distinct outers that share a key value (and lets a
        // navigation-member key, unreadable from the materialized entity, avoid client
        // evaluation entirely). Null falls back to OuterKeySelector — correct for scalar
        // outers, which are DISTINCT keys by construction.
        Func<object, object?>? OuterIdentitySelector = null,
        // The result selector executes CLIENT-side as a compiled delegate cached with
        // the plan; a closure captured inside it (e.g. the bound of a filtered
        // per-group aggregate) would replay the FIRST execution's value forever.
        // When the selector captures closures, this delegate takes the closure
        // values as a slot array and the provider rebinds it per execution from the
        // current expression (see NormQueryProvider.RebindGroupJoinClosures).
        Func<object, IEnumerable<object>, object?[], object>? ClosureLiftedResultSelector = null,
        int ClosureSlotCount = 0,
        // Select(computed).Distinct().GroupJoin(...): the outer translates as the full
        // entity (rows ordered by the computed key, then PK), and Distinct semantics are
        // restored by emitting only the FIRST segment per distinct outer key — equal-key
        // segments are adjacent by construction. Each segment still carries its own
        // correct inner group; duplicates are simply not emitted.
        bool DistinctOuterKeys = false,
        // Closure captures inside the outer KEY selector (e.g. a modulus captured in a
        // Distinct projection) would bake the first execution's values into the compiled
        // key delegate used for de-dup and key-based segmentation. When present, this
        // takes the current closure values as a slot array and the provider rebinds
        // OuterKeySelector per execution (see NormQueryProvider.RebindGroupJoinClosures).
        Func<object, object?[], object?>? ClosureLiftedOuterKeySelector = null,
        int OuterKeyClosureSlotCount = 0
    );

    /// <summary>
    /// Collects closure-captured values (member accesses rooted in a constant) in
    /// visitor pre-order — the SAME traversal the GroupJoin closure lift uses to
    /// assign slots, so collected values bind positionally to the lifted delegate.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure value collection evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure value collection reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal static class GroupJoinClosureValues
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure value collection evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure value collection reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal static void Collect(System.Linq.Expressions.Expression body, List<object?> into)
            => new Collector(into).Visit(body);

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure value collection evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure value collection reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class Collector : System.Linq.Expressions.ExpressionVisitor
        {
            private readonly List<object?> _into;
            public Collector(List<object?> into) => _into = into;

            protected override System.Linq.Expressions.Expression VisitConstant(System.Linq.Expressions.ConstantExpression node) => node;

            protected override System.Linq.Expressions.Expression VisitMember(System.Linq.Expressions.MemberExpression node)
            {
                if (QueryTranslator.TryGetConstantValue(node, out var value))
                {
                    _into.Add(value);
                    return node;
                }
                return base.VisitMember(node);
            }
        }
    }

    /// <summary>
    /// Structural equality wrapper for a composite outer identity in GroupJoin
    /// segmentation: two flattened rows belong to the same outer element only when
    /// every key component matches.
    /// </summary>
    internal sealed class GroupJoinOuterIdentity : IEquatable<GroupJoinOuterIdentity>
    {
        private readonly object?[] _values;

        public GroupJoinOuterIdentity(object?[] values) => _values = values;

        public bool Equals(GroupJoinOuterIdentity? other)
        {
            if (other is null || other._values.Length != _values.Length)
                return false;
            for (var i = 0; i < _values.Length; i++)
            {
                if (!Equals(_values[i], other._values[i]))
                    return false;
            }
            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as GroupJoinOuterIdentity);

        public override int GetHashCode()
        {
            var hash = new HashCode();
            foreach (var value in _values)
                hash.Add(value);
            return hash.ToHashCode();
        }
    }

    /// <summary>
    /// Defines a secondary query for fetching dependent collection data in split query scenarios.
    /// Used to mitigate Cartesian explosion when projecting nested collections.
    /// </summary>
    /// <param name="TargetMapping">The table to fetch children from.</param>
    /// <param name="ForeignKeyColumns">The ordered foreign key columns on the child table linking to the parent.</param>
    /// <param name="ParentKeyProperties">The ordered key properties on the parent object to extract IDs from.</param>
    /// <param name="TargetCollectionProperty">The collection property on the parent object to populate with children.</param>
    /// <param name="CollectionElementType">The type of elements in the collection.</param>
    /// <param name="FilterSql">Optional per-element predicate SQL from a shaped projection binding
    /// (Select(o =&gt; new Dto { Lines = o.Lines.Where(pred).ToList() })), rendered at plan-build time
    /// against the child table and ANDed onto the split-query child fetch; null for a bare include.</param>
    /// <param name="FilterParameters">Names of the compiled parameters the shaped filter references,
    /// bound onto the child command from the main command's live values so closure captures re-bind
    /// correctly across plan-cache hits. Empty/null when the filter is constant-only or absent.</param>
    /// <param name="ElementProjection">Optional element projection from a shaped projection binding
    /// (Select(o =&gt; new Dto { Lines = o.Lines.Select(l =&gt; new LineDto{...}).ToList() })); when present
    /// CollectionElementType is the projected type and the child materializer applies this lambda
    /// client-side. Null for a bare/filtered (non-projected) collection.</param>
    /// <param name="Owned">Set when the shaped collection is an OwnsMany owned collection; the fetch groups
    /// children by the FK column's ordinal (owned rows carry no FK property) and materializes through the
    /// owned mapping. Null for relations and many-to-many.</param>
    /// <param name="M2M">Set when the shaped collection is a many-to-many; the fetch runs the two-phase
    /// bridge+related query and correlates by the projected left-key member. Null for relations and owned.</param>
    /// <param name="OrderingSql">Rendered ORDER BY keys (no keyword) for an ordered / top-N projection; when
    /// non-null the relation child fetch wraps the filtered set with a ROW_NUMBER window partitioned by the
    /// FK. Null for an unordered collection.</param>
    /// <param name="RowCap">Take(n) value — keep rows ranked <c>__rn &lt;= RowSkip + RowCap</c>. Null when there is no Take.</param>
    /// <param name="RowSkip">Skip(n) value — keep rows ranked <c>__rn &gt; RowSkip</c>. Null when there is no Skip.</param>
    internal sealed record DependentQueryDefinition(
        TableMapping TargetMapping,
        IReadOnlyList<Column> ForeignKeyColumns,
        IReadOnlyList<PropertyInfo> ParentKeyProperties,
        PropertyInfo TargetCollectionProperty,
        Type CollectionElementType,
        // Optional per-element filter from a shaped projection binding
        // (Select(o => new Dto { Lines = o.Lines.Where(pred).ToList() })). Rendered to SQL at plan-build
        // time so its closures flow through the shared compiled-parameter channel (RecordClosureSlot),
        // keeping values fresh across cached-plan reuse. The child fetch ANDs FilterSql onto its WHERE
        // and binds FilterParameters from the main command. Null for a bare collection include.
        string? FilterSql = null,
        IReadOnlyList<string>? FilterParameters = null,
        // Optional element projection from a shaped projection binding
        // (Select(o => new Dto { Lines = o.Lines.Select(l => new LineDto{...}).ToList() })). When present,
        // CollectionElementType is the PROJECTED type and the child materializer applies this lambda
        // client-side to shape each fetched child entity into it. Only closure-free, element-only
        // projections reach here (captured/outer references fall through to fail-loud at translation).
        System.Linq.Expressions.LambdaExpression? ElementProjection = null,
        // A shaped projection over an OWNED (OwnsMany) or MANY-TO-MANY collection rather than a relation.
        // The fetch/stitch branches on these: owned rows carry no FK PROPERTY, so they are grouped by the FK
        // column's ordinal and materialized through the owned mapping; m2m runs the two-phase bridge+related
        // fetch. Both keep the shared parent-keying/assignment helpers, which resolve the parent key and the
        // target member by NAME against the projected DTO. At most one is non-null; both null = a relation.
        OwnedCollectionMapping? Owned = null,
        JoinTableMapping? M2M = null,
        // Optional ordering + row cap/skip for an ordered / top-N shaped projection
        // (Select(o => new { Recent = o.Lines.OrderByDescending(l => l.Date).Take(3).ToList() })). OrderingSql
        // is the rendered ORDER BY key list (no keyword) against the child table; when non-null the relation
        // child fetch wraps the (FK + tenant + global + element-filtered) inner query with
        // ROW_NUMBER() OVER (PARTITION BY fk ORDER BY OrderingSql) and keeps only rows with __rn <= RowCap
        // (and > RowSkip). Relation collections only (owned/m2m fail loud at plan build).
        string? OrderingSql = null,
        int? RowCap = null,
        int? RowSkip = null
    )
    {
        internal Column ForeignKeyColumn => ForeignKeyColumns[0];
        internal PropertyInfo ParentKeyProperty => ParentKeyProperties[0];
        internal bool IsComposite => ForeignKeyColumns.Count > 1;
    }

}
