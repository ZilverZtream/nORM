using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;

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

        /// <summary>
        /// Derives a scalar complexity score for use with
        /// <see cref="nORM.Execution.AdaptiveTimeoutManager"/>.
        /// The formula mirrors the SQL-string heuristic in DbContext.GetAdaptiveTimeout
        /// but is computed from reliable tree-walk data instead of substring scanning.
        /// </summary>
        public int ToComplexityScore()
        {
            int score = 1;
            score += JoinCount * 2;
            if (HasGroupBy) score += 2;
            if (HasOrderBy) score += 2;
            score += SubqueryDepth * 2;
            if (HasDistinct) score += 1;
            if (HasAggregates) score += 1;
            // Each predicate contributes a tiny amount
            score += PredicateCount / 5;
            return Math.Min(score, 50); // cap matches DbContext.GetAdaptiveTimeout
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
        QueryComplexityMetrics Complexity = default
    );

    internal sealed record IncludePlan(List<TableMapping.Relation> Path);

    internal sealed record GroupJoinInfo(
        Type OuterType,
        Type InnerType,
        Type ResultType,
        Func<object, object?> OuterKeySelector,
        Column InnerKeyColumn,
        Func<object, IEnumerable<object>, object> ResultSelector
    );

    /// <summary>
    /// Defines a secondary query for fetching dependent collection data in split query scenarios.
    /// Used to mitigate Cartesian explosion when projecting nested collections.
    /// </summary>
    /// <param name="TargetMapping">The table to fetch children from.</param>
    /// <param name="ForeignKeyColumn">The foreign key column on the child table linking to the parent.</param>
    /// <param name="ParentKeyProperty">The primary key property on the parent object to extract IDs from.</param>
    /// <param name="TargetCollectionProperty">The collection property on the parent object to populate with children.</param>
    /// <param name="CollectionElementType">The type of elements in the collection.</param>
    internal sealed record DependentQueryDefinition(
        TableMapping TargetMapping,
        Column ForeignKeyColumn,
        PropertyInfo ParentKeyProperty,
        PropertyInfo TargetCollectionProperty,
        Type CollectionElementType
    );

}