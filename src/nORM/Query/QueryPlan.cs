using System;
using System.Collections.Generic;
using System.Data.Common;
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
        TimeSpan? CacheExpiration
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

}