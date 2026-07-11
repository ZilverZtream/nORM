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
        bool ClientScalar = false
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

    internal sealed record IncludePlan(List<TableMapping.Relation> Path);

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
        int OuterColumnCount = -1
    );

    /// <summary>
    /// Defines a secondary query for fetching dependent collection data in split query scenarios.
    /// Used to mitigate Cartesian explosion when projecting nested collections.
    /// </summary>
    /// <param name="TargetMapping">The table to fetch children from.</param>
    /// <param name="ForeignKeyColumns">The ordered foreign key columns on the child table linking to the parent.</param>
    /// <param name="ParentKeyProperties">The ordered key properties on the parent object to extract IDs from.</param>
    /// <param name="TargetCollectionProperty">The collection property on the parent object to populate with children.</param>
    /// <param name="CollectionElementType">The type of elements in the collection.</param>
    internal sealed record DependentQueryDefinition(
        TableMapping TargetMapping,
        IReadOnlyList<Column> ForeignKeyColumns,
        IReadOnlyList<PropertyInfo> ParentKeyProperties,
        PropertyInfo TargetCollectionProperty,
        Type CollectionElementType
    )
    {
        internal Column ForeignKeyColumn => ForeignKeyColumns[0];
        internal PropertyInfo ParentKeyProperty => ParentKeyProperties[0];
        internal bool IsComposite => ForeignKeyColumns.Count > 1;
    }

}
