using System;
using System.Collections.Generic;
using System.Data.Common;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed record QueryPlan(
        string Sql, 
        IReadOnlyDictionary<string, object> Parameters, 
        Func<DbDataReader, object> Materializer, 
        Type ElementType, 
        bool IsScalar, 
        bool SingleResult, 
        string MethodName, 
        List<IncludePlan> Includes, 
        GroupJoinInfo? GroupJoinInfo
    );
    
    internal sealed record IncludePlan(TableMapping.Relation Relation);
    
    internal sealed record GroupJoinInfo(
        Type OuterType, 
        Type InnerType, 
        Type ResultType, 
        Func<object, object> OuterKeySelector, 
        Func<object, object> InnerKeySelector, 
        Column InnerKeyColumn, 
        string CollectionName
    );

    internal sealed class QueryPlanCacheKey : IEquatable<QueryPlanCacheKey>
    {
        private readonly string _key;
        
        public QueryPlanCacheKey(System.Linq.Expressions.Expression expression, object? tenantId) 
            => _key = $"{tenantId}_{expression}";
            
        public override int GetHashCode() => _key.GetHashCode();
        public override bool Equals(object? obj) => Equals(obj as QueryPlanCacheKey);
        public bool Equals(QueryPlanCacheKey? other) => other != null && _key == other._key;
    }
}