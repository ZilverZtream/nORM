using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
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
        bool NoTracking,
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
        private readonly object? _tenantId;
        private readonly int _fingerprint;
        private readonly int _hashCode;
        private readonly Type _elementType;
        public QueryPlanCacheKey(Expression expression, object? tenantId, Type elementType)
        {
            _tenantId = tenantId;
            _elementType = elementType;
            _fingerprint = ExpressionFingerprint.Compute(expression);
            _hashCode = HashCode.Combine(_tenantId?.GetHashCode() ?? 0, _fingerprint, _elementType);
        }

        public override int GetHashCode() => _hashCode;
        public override bool Equals(object? obj) => Equals(obj as QueryPlanCacheKey);
        public bool Equals(QueryPlanCacheKey? other)
            => other != null && _fingerprint == other._fingerprint && Equals(_tenantId, other._tenantId) && _elementType == other._elementType;
    }
}