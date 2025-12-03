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
        List<DependentQueryDefinition>? DependentQueries = null
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
    internal sealed record DependentQueryDefinition(
        /// <summary>The table to fetch children from.</summary>
        TableMapping TargetMapping,
        /// <summary>The foreign key column on the child table linking to the parent.</summary>
        Column ForeignKeyColumn,
        /// <summary>The primary key property on the parent object to extract IDs from.</summary>
        PropertyInfo ParentKeyProperty,
        /// <summary>The collection property on the parent object to populate with children.</summary>
        PropertyInfo TargetCollectionProperty,
        /// <summary>The type of elements in the collection.</summary>
        Type CollectionElementType
    );

}